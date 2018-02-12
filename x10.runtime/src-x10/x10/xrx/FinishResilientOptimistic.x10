/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Sara Salem Hamouda 2017-2018.
 */
package x10.xrx;

import x10.compiler.AsyncClosure;
import x10.compiler.Immediate;
import x10.compiler.Uncounted;
import x10.compiler.Inline;

import x10.io.CustomSerialization;
import x10.io.Deserializer;
import x10.io.Serializer;
import x10.util.concurrent.AtomicInteger;
import x10.util.concurrent.SimpleLatch;
import x10.util.GrowableRail;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.concurrent.Lock;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.util.resilient.concurrent.ResilientLowLevelFinish;
import x10.util.concurrent.Condition;

//TODO: clean remote finishes -> design a piggybacking protocol for GC
//TODO:  bulk globalInit for a chain of finishes.
//TODO:  createBackup: repeat if backup place is dead. block until another place is found!!
//TODO:  test notifyActivityCreationFailed()
//TODO: handle RemoteCreationDenied
//TODO: getNewMasterBlocking() does not need to be blocking
//TODO: revise the adoption logic of nested local finishes
//TODO: delete backup in sync(...) if quiescent reached
//TODO: DenyId, can it use the src place only without the parent Id??? consider cases when a recovered master is creating new children backups!!!!
/**
 * Distributed Resilient Finish (records transit tasks only)
 * Implementation notes: remote objects are shared and are persisted in a static hashmap (special GC needed)
 */
class FinishResilientOptimistic extends FinishResilient implements CustomSerialization {
    private static val DUMMY_INT = -1n;
    
    protected transient var me:FinishResilient; // local finish object
    val id:Id;

    public def toString():String { 
        return me.toString();
    }
    
    def notifySubActivitySpawn(place:Place):void { me.notifySubActivitySpawn(place); }  /*Blocking replication*/
    def notifyShiftedActivitySpawn(place:Place):void { me.notifyShiftedActivitySpawn(place); }  /*Blocking replication*/
    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean { return me.notifyActivityCreation(srcPlace, activity); }
    def notifyShiftedActivityCreation(srcPlace:Place):Boolean { return me.notifyShiftedActivityCreation(srcPlace); }
    def notifyRemoteContinuationCreated():void { me.notifyRemoteContinuationCreated(); }
    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { me.notifyActivityCreationFailed(srcPlace, t); }
    def notifyActivityCreatedAndTerminated(srcPlace:Place):void { me.notifyActivityCreatedAndTerminated(srcPlace); }
    def pushException(t:CheckedThrowable):void { me.pushException(t); } /*Blocking replication*/
    def notifyActivityTermination(srcPlace:Place):void { me.notifyActivityTermination(srcPlace); }
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def waitForFinish():void { me.waitForFinish(); }
    def spawnRemoteActivity(place:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { me.spawnRemoteActivity(place, body, prof); }
    
    //create root finish
    public def this (parent:FinishState, src:Place, kind:Int) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        me = new OptimisticMasterState(id, parent, src, kind);
        //NOLOG if (verbose>=1) debug("<<<< RootFinish(id="+id+", src="+src+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = OptimisticRemoteState.getOrCreateRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState, src:Place, kind:Int) {
        return new FinishResilientOptimistic(parent, src, kind);
    }
    
    def getSource():Place {
        if (me instanceof OptimisticMasterState)
            return here;
        else
            return Runtime.activity().srcPlace;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        //NOLOG if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        if (me instanceof OptimisticMasterState) {
            (me as OptimisticMasterState).globalInit(false); 
        }
        ser.writeAny(id);
        //NOLOG if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    /* Because we record only the transit tasks with src-dst, we must record the
     * number of received tasks from each place, to be able to identify which transit tasks terminated.
     * 
     * For adoption, we rely on getting information about remote tasks from remote finish objects.
     * Therefore, we must store the remote objects until root finish is released.
     * 
     * To save memory, we don't create a remote object for each src place for each finish,
     * rather we create one remote object per finish with hash maps recording received and reported tasks per place.
     */
    //REMOTE
    public static final class OptimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
        val id:Id; //parent root finish
        val ilock = new Lock(); //instance level lock
        val received = new HashMap[Task,Int](); //increasing counts
        val reported = new HashMap[Task,Int](); //increasing counts
        var taskDeny:HashSet[Int] = null; // lazily allocated
        val lc = new AtomicInteger(0n);//each time lc reaches zero, we report reported-received to root finish and set reported=received

        private static val remoteLock = new Lock();
        private static val remotes = new HashMap[Id, OptimisticRemoteState]() ; //a cache for remote finish objects
        private static val remoteDeny = new HashSet[Id](); //remote deny list
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Remote dump:\n");
            s.add("           here:" + here.id); s.add('\n');
            s.add("             id:" + id); s.add('\n');            
            s.add("     localCount:"); s.add(lc); s.add('\n');
            if (received.size() > 0) {
                s.add("          received:\n");
                for (e in received.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (reported.size() > 0) {
                s.add("          reported:\n");
                for (e in reported.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:OptimisticRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
                    if (remoteDeny.contains(id)) {
                        throw new RemoteCreationDenied();
                    }
                    remoteState = new OptimisticRemoteState(id);
                    remotes.put(id, remoteState);
                }
                return remoteState;
            } finally {
                remoteLock.unlock();
            }
        }

        public static def countReceived(id:Id, src:Int, kind:Int) {
            //NOLOG if (verbose>=1) debug(">>>> countReceived(id="+id+", src="+src+", kind="+kind+") called");
            var count:Int = 0n;
            try {
                remoteLock.lock();
                val remote = remotes.getOrElse(id, null);
                if (remote != null) {
                    count = remote.receivedFrom(src, kind);
                } else {
                    //deny future remote finishes from that id 
                    remoteDeny.add(id);
                }
            } finally {
                remoteLock.unlock();
            }
            //NOLOG if (verbose>=1) debug("<<<< countReceived(id="+id+", src="+src+", kind="+kind+") returning, count="+count);
            return count;
        }
        
        private def receivedFrom(src:Int, kind:Int) {
            var count:Int = 0n;
            try {
                ilock.lock();
                count = received.getOrElse(Task(src, kind),0n);
                //no more tasks from that src place should be accepted
                if (taskDeny == null)
                    taskDeny = new HashSet[Int]();
                taskDeny.add(src);
            } finally {
                ilock.unlock();
            }
            return count;
        }
        
        //Calculates the delta between received and reported
        private def getReportMap() {
            try {
                ilock.lock();
                //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMap called");
                var map:HashMap[Task,Int] = null;
                val iter = received.keySet().iterator();
                while (iter.hasNext()) {
                    val t = iter.next();
                    if (t.place == here.id as Int) {
                        //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] ignored");
                        continue;
                    }
                    val rep = reported.getOrElse(t, 0n);
                    val rec = received.getOrThrow(t);
                    
                    //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] rep="+rep + " rec = " + rec);
                    if ( rep < rec) {
                        if (map == null)
                            map = new HashMap[Task,Int]();
                        map.put(t, rec - rep);
                        reported.put (t, rec);
                        //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] reported.put("+t+","+(rec-rep)+")");
                    }
                }
                /*map can be null here: because notifyTerminationAndGetMap doesn't decrement lc and get the map as a one atomic action,  
                  it is possible that two activities reach zero lc and then one of them reports all of the activities, while the other finds no activity to report */
                //NOLOG if (verbose>=1) printMap(map);
                //NOLOG if (verbose>=3) dump();
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMap returning");
                return map;
            } finally {
                ilock.unlock();
            }
        }
        
        public def notifyTerminationAndGetMap(t:Task) {
            var map:HashMap[Task,Int] = null;
            val count = lc.decrementAndGet();
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called, taskFrom["+t.place+"] decremented localCount to "+count);
            if (count == 0n) {
                map = getReportMap();
            }
            else assert count > 0: here + " FATAL ERROR: notifyTerminationAndGetMap(id="+id+") reached a negative local count";
            return map;
        }
        
        public def notifyReceived(t:Task) {
            try {
                ilock.lock();
                if (taskDeny != null && taskDeny.contains(t.place) )
                    throw new RemoteCreationDenied();
                increment(received, t);
            } finally {
                ilock.unlock();
            }
            return lc.incrementAndGet();
        }
        
        def printMap(map:HashMap[Task,Int]) {
            if (map == null)
                debug("<<<< Remote(id="+id+").notifyTerminationAndGetMap returning null, localCount = " + lc);
            else {
                val s = new x10.util.StringBuilder();
                for (e in map.entries()) {
                    s.add(e.getKey()+" = "+e.getValue()+", ");
                }
                debug("<<<< Remote(id="+id+").notifyTerminationAndGetMap returning, MAP = {"+s.toString()+"} ");
            }
        }
        
        def notifySubActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, ASYNC);
        }
        
        def notifyShiftedActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, AT);
        }
        
        def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (dstId == here.id as Int) {
                //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called locally, no action required");
            } else {
                val parentId = UNASSIGNED;
                //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, DUMMY_INT, DUMMY_INT);
                FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            val parentId = UNASSIGNED;
            
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+",kind="+kind+",bytes="+bytes.size+") called");
            val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, DUMMY_INT, DUMMY_INT);
            val num = req.num;
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
            };
            val count = notifyReceived(Task(srcId, ASYNC)); // synthetic activity to keep finish locally live during async replication
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+",kind="+kind+") incremented localCount to " + count);
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            try {
                val count = notifyReceived(Task(srcId, ASYNC));
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
                return true;
            } catch (e:RemoteCreationDenied) {
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, task denied");
                return false;
            }
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") called");
            try {
                val count = notifyReceived(Task(srcId, AT));
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, localCount = " + count);
                return true;
            } catch (e:RemoteCreationDenied) {
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, task denied");
                return false;
            }
        }
        
        def notifyRemoteContinuationCreated():void { /*noop for remote finish*/ }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            val parentId = UNASSIGNED;
            val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, DUMMY_INT,  DUMMY_INT);
            req.ex = t;
            FinishReplicator.asyncExec(req, null);
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
        }
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val count = notifyReceived(Task(srcId, ASYNC));
            //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null) {
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }
            

            //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
            val req = FinishRequest.makeTermMulRequest(id, parentId, dstId, map, DUMMY_INT,  DUMMY_INT);
            FinishReplicator.asyncExec(req, null);
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            assert (Runtime.activity() != null) : here + " >>>> Remote(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            val parentId = UNASSIGNED;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, t, DUMMY_INT,  DUMMY_INT);
            FinishReplicator.exec(req, null);
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called ");
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null)
                return;
            val parentId = UNASSIGNED;
            //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
            val req = FinishRequest.makeTermMulRequest(id, parentId, dstId, map, DUMMY_INT,  DUMMY_INT);
            FinishReplicator.asyncExec(req, null);
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
    }
    
    //ROOT
    public static final class OptimisticMasterState extends FinishMasterState implements x10.io.Unserializable {
        val id:Id;
        val parentId:Id; //the direct parent of this finish (could be a remote finish) 
        val parent:FinishState; //the direct parent finish object (used in globalInit for recursive initializing)
        val latch:SimpleLatch; //latch for blocking the finish activity, and a also used as the instance lock

        //resilient finish counter set
        var numActive:Long;
        val sent:HashMap[Edge,Int]; //increasing counts    
        val transit:HashMap[Edge,Int];
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        var backupPlaceId:Int = FinishReplicator.getBackupPlace(here.id as Int);  //may be updated in notifyPlaceDeath
        var backupChanged:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        val finSrc:Place; //the source place that initiated the task that created this finish
        val finKind:Int; //the kind of the task that created the finish
        val lc:AtomicInteger;
        var isAdopted:Boolean = false; //set to true when backup recreates a master
        var migrating:Boolean = false; //used to stop updates to the object during recovery
    
        //initialize from backup values
        def this(_id:Id, _parentId:Id, _numActive:Long, _sent:HashMap[Edge,Int],
                _transit:HashMap[Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _finSrc:Place, _finKind:Int,
                _backupPlaceId:Int) {
            this.id = _id;
            this.parentId = _parentId;
            this.numActive = _numActive;
            this.sent = _sent;
            this.transit = _transit;
            this.backupPlaceId = _backupPlaceId;
            this.excs = _excs;
            this.finSrc = _finSrc;
            this.finKind = _finKind;
            this.strictFinish = false;
            this.parent = null;
            this.latch = new SimpleLatch();
            this.isGlobal = true;
            this.backupChanged = true;
            this.lc = new AtomicInteger(0n);
            this.isAdopted = true; // will have to call notifyParent
        }
        
        def this(id:Id, parent:FinishState, src:Place, kind:Int) {
            this.latch = new SimpleLatch();
            this.id = id;
            this.numActive = 1;
            this.parent = parent;
            this.finSrc = src;
            this.finKind = kind;
            this.lc = new AtomicInteger(1n);
            sent = new HashMap[Edge,Int]();
            sent.put(Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            transit = new HashMap[Edge,Int]();
            transit.put(Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            if (parent instanceof FinishResilientOptimistic) {
                parentId = (parent as FinishResilientOptimistic).id;
            }
            else {
                parentId = UNASSIGNED;
            }
        }
        
        public def getId() = id;
        
        //makeBackup is true only when a parent finish if forced to be global by its child
        //otherwise, backup is created with the first transit request
        def globalInit(makeBackup:Boolean) {
            latch.lock();
            strictFinish = true;
            if (!isGlobal) {
                //NOLOG if (verbose>=1) debug(">>>> globalInit(id="+id+") called");
                if (parent instanceof FinishResilientOptimistic) {
                    val frParent = parent as FinishResilientOptimistic;
                    if (frParent.me instanceof OptimisticMasterState) (frParent.me as OptimisticMasterState).globalInit(true);
                }
                if (makeBackup)
                    createBackup(backupPlaceId);
                isGlobal = true;
                FinishReplicator.addMaster(id, this);
                //NOLOG if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            latch.unlock();
        }
        
        private def createBackup(backupPlaceId:Int) {
            //TODO: redo if backup dies
            //NOLOG if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
            val backup = Place(backupPlaceId);
            if (backup.isDead()) {
                 //NOLOG if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
                 return false;
            }
            val myId = id; //don't copy this
            val home = here;
            val myParentId = parentId;
            val mySrc = finSrc;
            val myKind = finKind;
            val rCond = ResilientCondition.make(backup);
            val closure = (gr:GlobalRef[Condition]) => {
                at (backup) @Immediate("backup_create") async {
                    val bFin = FinishReplicator.findBackupOrCreate(myId, myParentId, mySrc, myKind);
                    at (gr) @Immediate("backup_create_response") async {
                        gr().release();
                    }
                }; 
            };
            rCond.run(closure);
            //TODO: redo if backup is dead
            if (rCond.failed()) {
                val excp = new DeadPlaceException(backup);
            }
            rCond.forget();
            return true;
        }
        
        public def lock() {
            latch.lock();
        }
        
        public def unlock() {
            latch.unlock();
        }
        
        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Root dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("     localCount:"); s.add(lc.get()); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (sent.size() > 0) {
                s.add("   sent-transit:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+" - "+transit.getOrElse(e.getKey(), 0n)+"\n");
                }
            }
            debug(s.toString());
        }
                
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
            //NOLOG if (verbose>=1) debug("<<<< addExceptionUnsafe(id="+id+") t="+t.getMessage() + " exceptions size = " + excs.size());
        }
        
        def addDeadPlaceException(placeId:Long, resp:MasterResponse) {
            try {
                latch.lock();
                val dpe = new DeadPlaceException(Place(placeId));
                dpe.fillInStackTrace();
                addExceptionUnsafe(dpe);
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def addException(t:CheckedThrowable, resp:MasterResponse) {
            try {
                latch.lock();
                addExceptionUnsafe(t);
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        
        def updateBackup(newBackup:Int) {
            try {
                latch.lock();
                backupPlaceId = newBackup;
            } finally {
                latch.unlock();
            }
        }
        
       def localFinishExceptionPushed(t:CheckedThrowable) {
            try { 
                latch.lock();
                if (!isGlobal) {
                    //NOLOG if (verbose>=1) debug(">>>> localFinishExceptionPushed(id="+id+") true");
                    addExceptionUnsafe(t);
                    return true;
                } 
                //NOLOG if (verbose>=1) debug("<<<< localFinishExceptionPushed(id="+id+") false: global finish");
                return false;
            } finally {
                latch.unlock();
            }
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, resp:MasterResponse) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " called");
            try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                increment(sent, e);
                increment(transit, e);
                numActive++;
                //NOLOG if (verbose>=3) debug("==== Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
                //NOLOG if (verbose>=3) dump();
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                 latch.unlock();
            }
        }
        
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, tag:String, resp:MasterResponse) {
            try {
                latch.lock();
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
                val e = Edge(srcId, dstId, kind);
                decrement(transit, e);
                //don't decrement 'sent' 
                
                numActive--;
                assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
                if (t != null) addExceptionUnsafe(t);
                if (quiescent()) {
                    releaseLatch();
                    notifyParent();
                    FinishReplicator.removeMaster(id);
                }
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, tag:String, resp:MasterResponse) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            deduct(transit, e, cnt);
            //don't decrement 'sent' 
            
            numActive-=cnt;
            assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            }
            resp.backupPlaceId = backupPlaceId;
            resp.backupChanged = backupChanged;
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " set backup="+resp.backupPlaceId+ " set backupChanged="+resp.backupChanged);
        }
        
        def quiescent():Boolean {
            //NOLOG if (verbose>=2) debug(">>>> Master(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!");
                dump();
                assert false : here + " COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!";
                return true; 
            }
        
            val quiet = numActive == 0;
            //NOLOG if (verbose>=3) dump();
            //NOLOG if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Master(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        def releaseLatch() {
            if (isAdopted) {
                //NOLOG if (verbose>=1) debug("releaseLatch(id="+id+") called on adopted finish; not releasing latch");
            } else {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                //NOLOG if (verbose>=2) debug("Master(id="+id+") releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));
                latch.release();
            }
            //NOLOG if (verbose>=2) debug("Master(id="+id+").releaseLatch returning");
        }
        
        def notifyParent() {
            if (!isAdopted || parentId == FinishResilient.UNASSIGNED) {
                //NOLOG if (verbose>=1) debug("<<< Master(id="+id+").notifyParent returning, not adopted or parent["+parentId+"] does not exist");
                return;
            }
            val srcId = finSrc.id as Int;
            val dstId = id.home;
            val dpe:CheckedThrowable;
            if (finKind == FinishResilient.ASYNC) {
                dpe = new DeadPlaceException(Place(dstId));
                dpe.fillInStackTrace();
            } else {
                dpe = null;
            }
            
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") called");
            val req = FinishRequest.makeTermRequest(parentId, FinishResilient.UNASSIGNED, FinishResilient.UNASSIGNED, srcId, dstId, finKind, DUMMY_INT,  DUMMY_INT);
            req.ex = dpe;
            FinishReplicator.asyncExec(req, null);
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") returning");
        }
        
        public def exec(req:FinishRequest) {
            val id = req.id;
            val resp = new MasterResponse();
            try {
                lock();
                if (migrating) {
                    resp.excp = new MasterMigrating();
                    return resp;
                }
            } finally {
                unlock();
            }
            if (req.reqType == FinishRequest.TRANSIT) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + " ] called");
                try{
                    if (Place(srcId).isDead()) {
                        //NOLOG if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                    } else if (Place(dstId).isDead()) {
                        if (kind == FinishResilient.ASYNC) {
                            //NOLOG if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                            addDeadPlaceException(dstId, resp);
                            resp.transitSubmitDPE = true;
                        } else {
                            //NOLOG if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                        }
                    } else {
                        inTransit(srcId, dstId, kind, "notifySubActivitySpawn", resp);
                        resp.submit = true;
                    }
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit="+resp.submit+" ] returning");
            } else if (req.reqType == FinishRequest.TERM) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                val ex = req.ex;
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] called");
                try{
                    //Unlike place0 pessimistic finish, we don't suppress termination notifications whose dst is dead.
                    //We actually wait for these termination messages to come from (kind of) adopted children
                    transitToCompleted(srcId, dstId, kind, ex, "notifyActivityTermination", resp);
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    addException(ex, resp);
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TERM_MUL) {
                val map = req.map;
                val dstId = req.dstId;
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] called");
                try{
                  //Unlike place0 finish, we don't suppress termination notifications whose dst is dead.
                  //Because we expect termination messages from these tasks to be notified if the tasks were recieved by a dead dst
                    try {
                        latch.lock();
                        resp.backupPlaceId = -1n;
                        for (e in map.entries()) {
                            val srcId = e.getKey().place;
                            val kind = e.getKey().kind;
                            val cnt = e.getValue();
                            transitToCompletedUnsafe(srcId, dstId, kind, cnt, "notifyActivityTermination", resp);
                        }
                    } finally {
                        latch.unlock();
                    }
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning, backup="+resp.backupPlaceId+", exception="+resp.excp);
            }
            return resp;
        }
        
        def notifySubActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, ASYNC);
        }
        
        def notifyShiftedActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, AT);
        }
        
        def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (srcId == dstId) {
                lc.incrementAndGet();
                //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
                //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, finSrc.id as Int, finKind);
                FinishReplicator.exec(req, this);
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            globalInit(false);//globalize parent if not global - cannot postpone this till sendPendingAct because it is called in an immediate thread and globalInit is blocking
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").spawnRemoteActivity(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, finSrc.id as Int, finKind);
            val num = req.num;
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
            };
            val count = lc.incrementAndGet(); // synthetic activity to keep finish locally live during async replication
            FinishReplicator.asyncExec(req, this, preSendAction, postSendAction);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").spawnRemoteActivity(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, incremented localCount to " + count);
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        def notifyRemoteContinuationCreated():void { 
            strictFinish = true;
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyRemoteContinuationCreated() isGlobal = "+isGlobal);
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, finSrc.id as Int, finKind);
            req.ex = t;
            FinishReplicator.asyncExec(req, this);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") called");
            //no need to call notifyActivityCreation() since it does NOOP in Root finish
            notifyActivityTermination(srcPlace, ASYNC);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            assert (Runtime.activity() != null) : here + " >>>> Root(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, t, finSrc.id as Int, finKind);
            FinishReplicator.exec(req, this);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val count = lc.decrementAndGet();
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") called, decremented localCount to "+count);
            if (count > 0) {
                return;
            }
            assert count == 0n: here + " FATAL ERROR: Root(id="+id+").notifyActivityTermination reached a negative local count";
            if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                latch.release();
                return;
            }
            //NOLOG if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, finSrc.id as Int, finKind);
            FinishReplicator.asyncExec(req, this);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def waitForFinish():void {
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called, lc = " + lc );

            // terminate myself
            notifyActivityTermination(here, ASYNC);

            // If we haven't gone remote with this finish yet, see if this worker
            // can execute other asyncs that are governed by the finish before waiting on the latch.
            if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS || !strictFinish)) {
                //NOLOG if (verbose>=2) debug("calling worker.join for id="+id);
                Runtime.worker().join(this.latch);
            }

            // wait for the latch release
            //NOLOG if (verbose>=2) debug("calling latch.await for id="+id);
            latch.await(); // wait for the termination (latch may already be released)
            //NOLOG if (verbose>=2) debug("returned from latch.await for id="+id);

            // no more messages will come back to this finish state 
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").waitForFinish returning with exceptions, size=" + excs.size() );
                throw new MultipleExceptions(excs);
            }
            
            if (id == TOP_FINISH) {
                //blocks until replication work is done at all places
                FinishReplicator.finalizeReplication();
            }
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        
        public def isImpactedByDeadPlaces(newDead:HashSet[Int]) {
            if (newDead.contains(backupPlaceId)) /*needs re-replication*/
                return true;
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.src) || newDead.contains(edge.dst))
                    return true;
            }
            return false;
        }
        
        public def convertToDead(newDead:HashSet[Int], countChildrenBackups:HashMap[FinishResilient.Id, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").convertToDead called");
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) && edge.dst != edge.src ) {
                    val t1 = e.getValue();
                    val t2 = countChildrenBackups.get(id);
                    //NOLOG if (verbose>=1) debug("==== Master(id="+id+").convertToDead("+id+") t1=" + t1 + " t2=" + t2);
                    assert t1 > 0 : here + " Master(id="+id+").convertToDead FATAL error, t1 must be positive";
                    if (t1 >= t2) {
                        val count = t1-t2;
                        val oldActive = numActive;
                        numActive -= count;
                        
                        //NOLOG if (verbose>=1) debug("==== Master(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
                        if (t2 == 0n)
                            toRemove.add(edge);
                        else
                            transit.put(edge, t2);
                        
                        assert numActive >= 0 : here + " Master(id="+id+").convertToDead FATAL error, numActive must not be negative";
                        if (edge.kind == ASYNC) {
                            for (1..count) {
                                //NOLOG if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                                val dpe = new DeadPlaceException(Place(edge.dst));
                                dpe.fillInStackTrace();
                                addExceptionUnsafe(dpe);
                            }
                        }
                    }
                    else assert false: here + "["+Runtime.activity()+"] Master(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met, t1="+t1+" and t2=" + t2;
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").convertFromDead called");
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                var trns:Int = e.getValue();
                if (newDead.contains(edge.src) && edge.src != edge.dst ) {
                    val sent = sent.get(edge);
                    val rcvId = FinishResilientOptimistic.ReceivedQueryId(id, edge.src, edge.dst, edge.kind);
                    val received = countReceived.get(rcvId);
                    assert sent > 0 && sent >= received : here + " Master(id="+id+").convertFromDead FATAL error, transit["+trns+"] sent["+sent+"] received["+received+"]";

                    val dropedMsgs = sent - received;
                    val oldActive = numActive;
                    numActive -= dropedMsgs;
                    trns -= dropedMsgs;
                    //NOLOG if (verbose>=1) debug("==== Master(id="+id+").convertFromDead src="+edge.src+" dst="+edge.dst+" transit["+trns+"] sent["+sent+"] received["+received+"] numActive["+oldActive+"] dropedMsgs["+dropedMsgs+"] changing numActive to " + numActive);
                    
                    if (trns == 0n)
                        toRemove.add(edge);
                    else
                        transit.put(edge, trns);
                    
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").convertFromDead returning, numActive="+numActive);
        }
    }

    //BACKUP
    public static final class OptimisticBackupState extends FinishBackupState implements x10.io.Unserializable {
        private static val verbose = FinishResilient.verbose;
        static def debug(msg:String) {
            val nsec = System.nanoTime();
            val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
            Console.OUT.println(output); Console.OUT.flush();
        }

        val id:Id; //finish id 
        val ilock = new Lock(); //instance lock
        val parentId:Id; //the root parent id (potential adopter)
        
        //resilient finish counter set
        var numActive:Long;
        val sent = new HashMap[FinishResilient.Edge,Int](); //always increasing   
        val transit = new HashMap[FinishResilient.Edge,Int]();
        var excs:GrowableRail[CheckedThrowable]; 
        var isAdopted:Boolean = false;
        var migrating:Boolean = false;
        var placeOfMaster:Int = -1n; //will be updated during adoption
        val finSrc:Place;
        val finKind:Int;
        var isReleased:Boolean = false;
        
        def this(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, kind:Int) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            sent.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            transit.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            this.placeOfMaster = id.home;
            this.finSrc = src;
            this.finKind = kind;
        }        
        
        def this(id:FinishResilient.Id, _parentId:FinishResilient.Id, src:Place, kind:Int, _numActive:Long, 
                _sent:HashMap[FinishResilient.Edge,Int],
                _transit:HashMap[FinishResilient.Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int) {
            this.id = id;
            this.numActive = _numActive;
            this.parentId = _parentId;
            for (e in _sent.entries()) {
                this.sent.put(e.getKey(), e.getValue());
            }
            for (e in _transit.entries()) {
                this.transit.put(e.getKey(), e.getValue());
            }
            this.placeOfMaster = _placeOfMaster;
            this.excs = _excs;
            this.finSrc = src;
            this.finKind = kind;
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Backup dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (sent.size() > 0) {
                s.add("   sent-transit:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+" - "+transit.getOrElse(e.getKey(), 0n)+"\n");
                }
            }
            debug(s.toString());
        }
        
        def sync(_numActive:Long, _sent:HashMap[FinishResilient.Edge,Int],
                _transit:HashMap[FinishResilient.Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int) {
            try {
                lock();
                this.numActive = _numActive;
                this.sent.clear();
                for (e in _sent.entries()) {
                    this.sent.put(e.getKey(), e.getValue());
                }
                this.transit.clear();
                for (e in _transit.entries()) {
                    this.transit.put(e.getKey(), e.getValue());
                }
                this.placeOfMaster = _placeOfMaster;
                this.excs = _excs;
                //NOLOG if (verbose>=3) dump();
            } finally {
                unlock();
            }
        }
        
        public def getId() = id;
        
        public def lock() {
            ilock.lock();
        }
        
        public def unlock() {
            ilock.unlock();
        }
        
        public def getParentId() = parentId;
        
        public def getPlaceOfMaster() {
            try {
                lock();
                return placeOfMaster;
            } finally {
                unlock();
            }
        }
        
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def addDeadPlaceException(placeId:Long) {
            try {
                ilock.lock();
                val dpe = new DeadPlaceException(Place(placeId));
                dpe.fillInStackTrace();
                addExceptionUnsafe(dpe);
            } finally {
                ilock.unlock();
            }
        }
        
        def addException(t:CheckedThrowable) {
            try {
                ilock.lock();
                addExceptionUnsafe(t);
            } finally {
                ilock.unlock();
            }
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
            try {
                ilock.lock();
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").inTransit called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                FinishResilient.increment(sent, e);
                FinishResilient.increment(transit, e);
                numActive++;
                
                //NOLOG if (verbose>=3) debug("==== Backup(id="+id+").inTransit (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                //NOLOG if (verbose>=3) dump();
                
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").inTransit returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, ex:CheckedThrowable, tag:String) {
            try {
                ilock.lock();
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                decrement(transit, e);
                //don't decrement 'sent'
                numActive--;
                //NOLOG if (verbose>=3) debug("==== Backup(id="+id+").transitToCompleted (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                //NOLOG if (verbose>=3) dump();
                if (ex != null) addExceptionUnsafe(ex);
                if (quiescent()) {
                    isReleased = true;
                    FinishReplicator.removeBackup(id);
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, tag:String) {
            try {
                ilock.lock();
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompletedMul called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                FinishResilient.deduct(transit, e, cnt);
                //don't decrement 'sent'
                numActive-=cnt;
                //NOLOG if (verbose>=3) debug("==== Backup(id="+id+").transitToCompletedMul (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                //NOLOG if (verbose>=3) dump();
                if (quiescent()) {
                    isReleased = true;
                    FinishReplicator.removeBackup(id);
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompletedMul returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def quiescent():Boolean {
            //NOLOG if (verbose>=2) debug(">>>> Backup(id="+id+").quiescent called");

            if (numActive < 0) {
                debug("COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!");
                assert false : "COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            //NOLOG if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Backup(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        def notifyParent() {
            val srcId = finSrc.id as Int;
            val dstId = id.home;
            val dpe:CheckedThrowable;
            if (finKind == FinishResilient.ASYNC) {
                dpe = new DeadPlaceException(Place(dstId));
                dpe.fillInStackTrace();
            } else 
                dpe = null;
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") called");
                val req = FinishRequest.makeTermRequest(parentId, FinishResilient.UNASSIGNED, FinishResilient.UNASSIGNED, srcId, dstId, finKind, DUMMY_INT, DUMMY_INT);
                req.ex = dpe;
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") returning");
            });
        }

        public def exec(req:FinishRequest, transitSubmitDPE:Boolean):Exception {
            val reqMaster = req.masterPlaceId;
            try {
                lock();
                if (migrating) {
                    return new MasterDied();
                }
                if (reqMaster != placeOfMaster) {
                    return new MasterChanged(id, placeOfMaster);
                }
            } finally {
                unlock();
            }
            var bexcp:Exception = null;
            if (req.reqType == FinishRequest.TRANSIT) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] called");
                try{
                    inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                } catch (t:Exception) {
                    bexcp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] returning");
            } else if (req.reqType == FinishRequest.TERM) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                val ex = req.ex;
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + ", ex=" + ex + " ] called");
                try{
                    transitToCompleted(srcId, dstId, kind, ex, "notifyActivityTermination");
                } catch (t:Exception) {
                    bexcp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + ", ex=" + ex + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    addException(ex);
                } catch (t:Exception) {
                    bexcp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TERM_MUL) {
                val map = req.map;
                val dstId = req.dstId;
                if (map == null)
                    throw new Exception(here + " FATAL ERROR map is null");
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] called");
                try{
                    try {
                        ilock.lock();
                        for (e in map.entries()) {
                            val srcId = e.getKey().place;
                            val kind = e.getKey().kind;
                            val cnt = e.getValue();
                            transitToCompletedUnsafe(srcId, dstId, kind, cnt, "notifyActivityTermination");
                        }
                    }finally {
                        ilock.unlock();
                    }
                } catch (t:Exception) {
                    bexcp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
            }
            return bexcp;
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        //waits until backup is adopted
        public def getNewMasterBlocking() {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").getNewMasterBlocking called");
            Runtime.increaseParallelism();
            ilock.lock();
            while (!isAdopted || migrating) {
                ilock.unlock();
                System.threadSleep(0); // release the CPU to more productive pursuits
                ilock.lock();
            }
            ilock.unlock();
            Runtime.decreaseParallelism(1n);
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").getNewMasterBlocking returning, newMaster=" + id);
            return id;
        }
        
        public def convertToDead(newDead:HashSet[Int], countChildrenBackups:HashMap[FinishResilient.Id, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").convertToDead called");
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) && edge.src != edge.dst ) {
                    val t1 = e.getValue();
                    val t2 = countChildrenBackups.get(id);
                    //NOLOG if (verbose>=1) debug("==== Backup(id="+id+").convertToDead("+id+") t1=" + t1 + " t2=" + t2);
                    assert t1 > 0 : here + " Backup(id="+id+").convertToDead FATAL error, t1 must be positive";
                    if (t1 >= t2) {
                        val count = t1-t2;
                        val oldActive = numActive;
                        numActive -= count;
                        //NOLOG if (verbose>=1) debug("==== Backup(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
                        if (t2 == 0n)
                            toRemove.add(edge);
                        else
                            transit.put(edge, t2);
                        
                        assert numActive >= 0 : here + " Backup(id="+id+").convertToDead FATAL error, numActive must not be negative";
                        if (edge.kind == ASYNC) {
                            for (1..count) {
                                //NOLOG if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                                val dpe = new DeadPlaceException(Place(edge.dst));
                                dpe.fillInStackTrace();
                                addExceptionUnsafe(dpe);
                            }
                        }
                    }
                    else assert false: here + "["+Runtime.activity()+"] Backup(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met, t1="+t1+" t2=" + t2;
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                isReleased = true;
                notifyParent();
                FinishReplicator.removeBackup(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").convertFromDead called");
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                var trns:Int = e.getValue();
                if (newDead.contains(edge.src) && edge.src != edge.dst) {
                    val sent = sent.get(edge);
                    val rcvId = FinishResilientOptimistic.ReceivedQueryId(id, edge.src, edge.dst, edge.kind);
                    val received = countReceived.get(rcvId);
                    assert sent > 0 && sent >= received : here + " Backup(id="+id+").convertFromDead FATAL error, transit["+trns+"] sent["+sent+"] received["+received+"]";

                    val dropedMsgs = sent - received;
                    val oldActive = numActive;
                    numActive -= dropedMsgs;
                    trns -= dropedMsgs;
                    //NOLOG if (verbose>=1) debug("==== Backup(id="+id+").convertFromDead src="+edge.src+" dst="+edge.dst+" transit["+trns+"] sent["+sent+"] received["+received+"] numActive["+oldActive+"] dropedMsgs["+dropedMsgs+"] changing numActive to " + numActive);
                    
                    if (trns == 0n)
                        toRemove.add(edge);
                    else
                        transit.put(edge, trns);
                    
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                isReleased = true;
                notifyParent();
                FinishReplicator.removeBackup(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").convertFromDead returning, numActive="+numActive);
        }
        
        public def setLocalTaskCount(localChildrenCount:Int) {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").setLocalTaskCount called, localChildrenCount = " + localChildrenCount);
            val edge = FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC);
            val old = transit.getOrElse(edge, 0n);
            
            if (localChildrenCount == 0n)
                transit.remove(edge);
            else
                transit.put(edge, localChildrenCount);
            //NOLOG if (verbose>=3) dump();
            var count:Long = 0;
            if (old != localChildrenCount)
                count = old - localChildrenCount;
            val oldActive = numActive;
            numActive -= count;
            if (quiescent()) {
                isReleased = true;
                notifyParent();
                FinishReplicator.removeBackup(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").setLocalTaskCount returning, old["+old+"] new["+localChildrenCount+"] numActive="+oldActive +" changing numActive to " + numActive);
        }
        
    }
    
    /*********************************************************************/
    /*******************   Failure Recovery Methods   ********************/
    /*********************************************************************/
    static def aggregateCountingRequests(newDead:HashSet[Int],
            masters:HashSet[FinishMasterState], backups:HashSet[FinishBackupState]) {
        val countingReqs = new HashMap[Int,OptResolveRequest]();
        if (!masters.isEmpty()) {
            for (dead in newDead) {
                var backup:Int = FinishReplicator.getBackupPlace(dead);
                if (Place(backup).isDead())
                    backup = FinishReplicator.searchBackup(dead, backup);
                for (mx in masters) {
                    val m = mx as OptimisticMasterState; 
                    m.lock();
                    for (entry in m.transit.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countChildren .put(ChildrenQueryId(m.id /*parent id*/, dead, edge.src ), -1n);
                        } else if (dead == edge.src) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(edge.dst, rreq);
                            }
                            rreq.countReceived.put(ReceivedQueryId(m.id, dead, edge.dst, edge.kind), -1n);
                        }
                    }
                    m.unlock();
                }
            }
        }
        if (!backups.isEmpty()) {
            for (dead in newDead) {
                val backup = here.id as Int;
                for (bx in backups) {
                    val b = bx as  OptimisticBackupState;
                    b.lock();
                    for (entry in b.transit.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countChildren .put(ChildrenQueryId(b.id, dead, edge.src), -1n);
                        } else if (dead == edge.src) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(edge.dst, rreq);
                            }
                            rreq.countReceived.put(ReceivedQueryId(b.id, dead, edge.dst, edge.kind), -1n);
                        }
                    }
                    b.unlock();
                }
            }
        }
        //NOLOG if (verbose >=1) printResolveReqs(countingReqs); 
        return countingReqs;
    }
    
    static def processCountingRequests(countingReqs:HashMap[Int,OptResolveRequest]) {
        //NOLOG if (verbose>=1) debug(">>>> processCountingRequests(size="+countingReqs.size()+") called");
        if (countingReqs.size() == 0) {
            //NOLOG if (verbose>=1) debug("<<<< processCountingRequests(size="+countingReqs.size()+") returning, zero size");
            return;
        }
        val places = new Rail[Int](countingReqs.size());
        val iter = countingReqs.keySet().iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val pl = iter.next();
            places(i++) = pl;
        }
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val outputGr = GlobalRef[HashMap[Int,OptResolveRequest]](countingReqs);
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                val requests = countingReqs.getOrThrow(p);
                //NOLOG if (verbose>=1) debug("==== processCountingRequests  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("counting_request") async {
                        //NOLOG if (verbose>=1) debug("==== processCountingRequests  reached from " + gr.home + " to " + here);
                        val countChildrenBackups = requests.countChildren ;
                        val countReceived = requests.countReceived;
                        if (countChildrenBackups.size() > 0) {
                            for (b in countChildrenBackups.entries()) {
                                val rec = b.getKey();
                                val count = FinishReplicator.countChildrenBackups(rec.parentId, rec.dead, rec.src);
                                countChildrenBackups.put(b.getKey(), count);   
                            }
                        }
                        
                        if (countReceived.size() > 0) {
                            for (r in countReceived.entries()) {
                                val key = r.getKey();
                                val count = OptimisticRemoteState.countReceived(key.id, key.src, key.kind);
                                countReceived.put(key, count);
                            }
                        }
                        
                        val me = here.id as Int;
                        //NOLOG if (verbose>=1) debug("==== processCountingRequests  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("counting_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,OptResolveRequest]]{self.home == here})().getOrThrow(me);
                            
                            for (ve in countChildrenBackups.entries()) {
                                output.countChildren .put(ve.getKey(), ve.getValue());
                            }
                            for (vr in countReceived.entries()) {
                                output.countReceived.put(vr.getKey(), vr.getValue());
                            }
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception(here + " FATAL ERROR: another place failed during recovery ...");
    }
    
    static def countLocalChildren(id:Id, backups:HashSet[FinishBackupState]) {
        //NOLOG if (verbose>=1) debug(">>>> countLocalChildren(id="+id+") called");
        var count:Int = 0n;
        for (b in backups) {
            if (b.getParentId() == id) 
                count++;
        }
        //NOLOG if (verbose>=1) debug("<<<< countLocalChildren(id="+id+") returning, count = " + count);
        return count;
    }
    
    //FIXME: nominate another master if the nominated one is dead
    static def createMasters(backups:HashSet[FinishBackupState]) {
        //NOLOG if (verbose>=1) debug(">>>> createMasters(size="+backups.size()+") called");
        if (backups.size() == 0) {
            //NOLOG if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+" returning, zero size");
            return;
        }
        
        val places = new Rail[Int](backups.size());
        var i:Long = 0;
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            places(i++) = FinishReplicator.nominateMasterPlaceIfDead(b.placeOfMaster);
        }
        
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val _backupPlaceId = here.id as Int;
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            var i:Long = 0;
            for (bx in backups) {
                val b = bx as OptimisticBackupState;
                val _id = b.id;
                val _parentId = b.parentId;
                val _numActive = b.numActive;
                val _sent = b.sent;
                val _transit = b.transit;
                val _excs = b.excs;
                val _finSrc = b.finSrc;
                val _finKind = b.finKind;
                
                val master = Place(places(i));
                if (master.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (master) @Immediate("create_masters") async {
                        val newM = new OptimisticMasterState(_id, _parentId, _numActive, 
                                _sent, _transit, _excs, _finSrc, _finKind, _backupPlaceId);
                        FinishReplicator.addMaster(_id, newM);
                        val me = here.id as Int;
                        at (gr) @Immediate("create_masters_response") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception("FATAL ERROR: another place failed during recovery ...");

        i = 0;
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            b.lock();
            b.placeOfMaster = places(i);
            b.isAdopted = true;
            b.migrating = false;
            b.unlock();
        }
        
        //NOLOG if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+") returning");
    }
    
    //FIXME: nominate another backup if the nominated one is dead
    static def createOrSyncBackups(newDead:HashSet[Int], masters:HashSet[FinishMasterState]) {
        //NOLOG if (verbose>=1) debug(">>>> createOrSyncBackups(size="+masters.size()+") called");
        
        val places = new Rail[Int](masters.size());
        val iter = masters.iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val m = iter.next() as OptimisticMasterState;
            places(i) = FinishReplicator.nominateBackupPlaceIfDead(m.id.home);
            m.backupPlaceId = places(i);
            m.backupChanged = true;
            i++;
        }
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val placeOfMaster = here.id as Int;
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (mx in masters) {
                val m = mx as OptimisticMasterState;
                val backup = Place(m.backupPlaceId);
                val id = m.id;
                val parentId = m.parentId;
                val finSrc = m.finSrc;
                val finKind = m.finKind;
                val numActive = m.numActive;
                val sent = m.sent;
                val transit = m.transit;
                val excs = m.excs;
                if (backup.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (backup) @Immediate("create_or_sync_backup") async {
                        FinishReplicator.createOptimisticBackupOrSync(id, parentId, finSrc, finKind, numActive, 
                                sent, transit, excs, placeOfMaster);
                        val me = here.id as Int;
                        at (gr) @Immediate("create_or_sync_backup_response") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception("FATAL ERROR: another place failed during recovery ...");
        
        for (mx in masters) {
            val m = mx as OptimisticMasterState;
            m.lock();
            m.migrating = false;
            m.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning");
    }
    
    static def notifyPlaceDeath():void {
        //NOLOG if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (Runtime.activity() == null) {
            //NOLOG if (verbose>=1) debug(">>>> notifyPlaceDeath returning, IGNORED REQUEST FROM IMMEDIATE THREAD");
            return; 
        }
        val newDead = FinishReplicator.getNewDeadPlaces();
        if (newDead == null || newDead.size() == 0) //occurs at program termination
            return;
        
        val masters = FinishReplicator.getImpactedMasters(newDead); //any master who contacted the dead place or whose backup was lost
        val backups = FinishReplicator.getImpactedBackups(newDead); //any backup who lost its master.
        
        //prevent updates on backups since they are based on decisions made by a dead master
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            b.lock();
            b.migrating = true;
            b.unlock();
        }
        
        val countingReqs = aggregateCountingRequests(newDead, masters, backups); // combine all requests targetted to a specific place
        processCountingRequests(countingReqs); //obtain the counts
        
        //merge all results
        val countChildrenBackups = new HashMap[FinishResilient.Id, Int]();
        val countReceived = new HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]();
        for (e in countingReqs.entries()) {
            val v = e.getValue();
            for (ve in v.countChildren .entries()) {
                countChildrenBackups.put(ve.getKey().parentId, ve.getValue());
            }
            for (vr in v.countReceived.entries()) {
                countReceived.put(vr.getKey(), vr.getValue());
            }
        }
        
        //update counts and check if quiecent reached
        for (m in masters) {
            val master = m as OptimisticMasterState;
            
            master.lock();
            //convert to dead
            master.convertToDead(newDead, countChildrenBackups);
            if (master.numActive > 0) {
                //convert from dead
                master.convertFromDead(newDead, countReceived);
            }
            master.migrating = true; //prevent updates to masters as we are copying the data, 
                                     //and we want to ensure that backup is created before processing new requests
            master.unlock();
        }
        
        if (masters.size() > 0)
            createOrSyncBackups(newDead, masters);
        else {
            val hereId = here.id as Int;
            val newBackup = Place(FinishReplicator.nominateBackupPlaceIfDead(hereId));
            val rCond = ResilientCondition.make(newBackup);
            val closure = (gr:GlobalRef[Condition]) => {
                at (newBackup) @Immediate("dummy_backup") async {
                    FinishReplicator.createDummyBackup(hereId);
                    at (gr) @Immediate("dummy_backup_response") async {
                        gr().release();
                    }
                }
            };
            rCond.run(closure);
        }

        val newMasters = new HashSet[FinishMasterState]();
        val activeBackups = new HashSet[FinishBackupState]();
        //sorting the backups is not needed. notifyParent calls are done using uncounted activities that will retry until the parent is available
        for (b in backups) {
            val backup = b as OptimisticBackupState;
            
            backup.lock();
            
            val localChildren = countLocalChildren(backup.id, backups);
            backup.setLocalTaskCount(localChildren);
            if (!backup.isReleased) {
                //convert to dead
                backup.convertToDead(newDead, countChildrenBackups);
                if (!backup.isReleased) {
                    //convert from dead
                    backup.convertFromDead(newDead, countReceived);
                    if (!backup.isReleased){
                        activeBackups.add(backup);
                    }
                }
            }
            
            backup.unlock(); //as long as we keep migrating = true, no changes will be permitted on backup
        }
        
        if (backups.size() > 0) {
            createMasters(activeBackups);
        } else {
            //NOLOG if (verbose>=1) debug("==== createMasters bypassed ====");
        }
        
        //NOLOG if (verbose>=1) debug("==== handling non-blocking pending requests ====");
        FinishReplicator.submitDeadBackupPendingRequests(newDead);
        FinishReplicator.submitDeadMasterPendingRequests(newDead);
        
        //NOLOG if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
    
}