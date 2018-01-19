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
//TODO: delete backup in sync(...) if quiescent
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
    
    protected static struct BackupQueryId(parentId:Id, src:Int /*src is only used to be added in the deny list*/) {
        public def toString() = "<BackupQueryId parentId=" + parentId + " src=" + src +">";
    }
    
    def notifySubActivitySpawn(place:Place):void { me.notifySubActivitySpawn(place); }
    def notifyShiftedActivitySpawn(place:Place):void { me.notifyShiftedActivitySpawn(place); }
    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean { return me.notifyActivityCreation(srcPlace, activity); }
    def notifyShiftedActivityCreation(srcPlace:Place):Boolean { return me.notifyShiftedActivityCreation(srcPlace); }
    def notifyRemoteContinuationCreated():void { me.notifyRemoteContinuationCreated(); }
    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { me.notifyActivityCreationFailed(srcPlace, t); }
    def notifyActivityCreatedAndTerminated(srcPlace:Place):void { me.notifyActivityCreatedAndTerminated(srcPlace); }
    def pushException(t:CheckedThrowable):void { me.pushException(t); }
    def notifyActivityTermination(srcPlace:Place):void { me.notifyActivityTermination(srcPlace); }
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def waitForFinish():void { me.waitForFinish(); }
    
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
        val fs = new FinishResilientOptimistic(parent, src, kind);
        FinishReplicator.addMaster(fs.id, fs.me as OptimisticMasterState);
        return fs;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        //NOLOG if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
                           // false means don't create a backup
        ser.writeAny(id);
        //NOLOG if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    //makeBackup is true only when a parent finish if forced to be global by its child
    //otherwise, backup is created with the first transit request
    private def globalInit(makeBackup:Boolean) {
        if (me instanceof OptimisticMasterState) {
            val rootState = me as OptimisticMasterState;
            rootState.latch.lock();
            rootState.strictFinish = true;
            if (!rootState.isGlobal) {
                //NOLOG if (verbose>=1) debug(">>>> globalInit(id="+id+") called");
                val parent = rootState.parent;
                if (parent instanceof FinishResilientOptimistic) {
                    val frParent = parent as FinishResilientOptimistic;
                    if (frParent.me instanceof OptimisticMasterState) (frParent as FinishResilientOptimistic).globalInit(true);
                }
                if (makeBackup)
                    createBackup(rootState.backupPlaceId);
                rootState.isGlobal = true;
                //NOLOG if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            rootState.latch.unlock();
        }
    }
    
    private def createBackup(backupPlaceId:Int) {
        val rootState = me as OptimisticMasterState;
        //TODO: redo if backup dies
        //NOLOG if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
         val backup = Place(backupPlaceId);
         if (backup.isDead()) {
             //NOLOG if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
             return false;
         }
         val myId = id; //don't copy this
         val home = here;
         val parentId = rootState.parentId;
         val src = rootState.optSrc;
         val optKind = rootState.optKind;
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_create") async {
                val bFin = FinishReplicator.findBackupOrCreate(myId, parentId, src, optKind);
                at (condGR) @Immediate("backup_create_response") async {
                    condGR().release();
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
        var lc:Int = 0n; //whenever lc reaches zero, report reported-received to root finish and set reported=received

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
        
        public def lc_incrementAndGet() {
            try {
                ilock.lock();
                return ++lc;
            } finally {
                ilock.unlock();
            }
        }
        
        public def lc_decrementAndGet() {
            try {
                ilock.lock();
                return --lc;
            } finally {
                ilock.unlock();
            }
        }
        
        //Calculated the delta between received and reported
        private def getReportMapUnsafe() {
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMapUnsafe called");
            
            val map = new HashMap[Task,Int]();
            val iter = received.keySet().iterator();
            while (iter.hasNext()) {
                val t = iter.next();
                if (t.place == here.id as Int) {
                    //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMapUnsafe Task["+t+"] ignored");
                    continue;
                }
                val rep = reported.getOrElse(t, 0n);
                val rec = received.getOrThrow(t);
                
                //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMapUnsafe Task["+t+"] rep="+rep + " rec = " + rec);
                if ( rep < rec) {
                    map.put(t, rec - rep);
                    reported.put (t, rec);
                    //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").getReportMapUnsafe Task["+t+"] reported.put("+t+","+(rec-rep)+")");
                }
            }
            //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMapUnsafe returning");
            //NOLOG if (verbose>=3) dump();
            return map;
        }
        
        public def notifyReceived(t:Task) {
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyReceived called");
            try {
                ilock.lock();
                if (taskDeny != null && taskDeny.contains(t.place) )
                    throw new RemoteCreationDenied();
                increment(received, t);
                ++lc;
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyReceived returning, lc="+lc);
                //NOLOG if (verbose >= 3) dump();
                return lc;
            } finally {
                ilock.unlock();
            }
            
        }
        
        public def notifyTerminationAndGetMap(t:Task) {
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called");
            var map:HashMap[Task,Int] = null;
            try {
                ilock.lock();
                lc--;
                
                if (lc < 0) {
                    for (var i:Long = 0 ; i < 100; i++) {
                        debug("FATAL ERROR: notifyTerminationAndGetMap(id="+id+") reached a negative local count");
                        assert false: here + " FATAL ERROR: notifyTerminationAndGetMap(id="+id+") reached a negative local count";
                    }
                }
                
                if (lc == 0n) {
                    map = getReportMapUnsafe();
                }
                
                //NOLOG if (verbose>=1) printMap(map);
                //NOLOG if (verbose>=3) dump();
            } finally {
                ilock.unlock();
            }
            return map;
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
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, DUMMY_INT, DUMMY_INT, srcId, dstId, kind);
                FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            try {
                val lc = notifyReceived(Task(srcId, ASYNC));
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + lc);
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
                val lc = notifyReceived(Task(srcId, AT));
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, localCount = " + lc);
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
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
                val parentId = UNASSIGNED;
                val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, DUMMY_INT,  DUMMY_INT, srcId, dstId, kind);
                req.ex = t;
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
            });
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
        }
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val lc = notifyReceived(Task(srcId, ASYNC));
            //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + lc);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null) {
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }
            
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
                val req = FinishRequest.makeTermMulRequest(id, parentId, DUMMY_INT,  DUMMY_INT, dstId, map);
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
            });
        }
        
        def pushException(t:CheckedThrowable):void {
            val parentId = UNASSIGNED;
            //NOLOG if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, DUMMY_INT,  DUMMY_INT, t);
            val resp = FinishReplicator.exec(req);
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
            val req = FinishRequest.makeTermMulRequest(id, parentId, DUMMY_INT,  DUMMY_INT, dstId, map);
            val resp = FinishReplicator.exec(req);
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
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        var backupPlaceId:Int = FinishReplicator.getBackupPlace(here.id as Int);  //may be updated in notifyPlaceDeath
        var backupChanged:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        val optSrc:Place; //the source place that initiated the task that created this finish
        val optKind:Int; //the kind of the task that created the finish
        var lc:Int = 1n;
        var isAdopted:Boolean = false; //set to true when backup recreates a master
        var migrating:Boolean = false; //used to stop updates to the object during recovery
    
        //initialize from backup values
        def this(_id:Id, _parentId:Id, _numActive:Long, _sent:HashMap[Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _optSrc:Place, _optKind:Int,
                _backupPlaceId:Int) {
            this.id = _id;
            this.parentId = _parentId;
            this.numActive = _numActive;
            this.sent = _sent;
            this.backupPlaceId = _backupPlaceId;
            this.excs = _excs;
            this.optSrc = _optSrc;
            this.optKind = _optKind;
            this.strictFinish = false;
            this.parent = null;
            this.latch = new SimpleLatch();
            this.isGlobal = true;
            this.backupChanged = true;
            this.lc = 0n;
            this.isAdopted = true; // will have to call notifyParent
        }
        
        def this(id:Id, parent:FinishState, src:Place, kind:Int) {
            this.latch = new SimpleLatch();
            this.id = id;
            this.numActive = 1;
            this.parent = parent;
            this.optSrc = src;
            this.optKind = kind;
            sent = new HashMap[Edge,Int]();
            increment(sent, Edge(id.home, id.home, FinishResilient.ASYNC));
            if (parent instanceof FinishResilientOptimistic) {
                parentId = (parent as FinishResilientOptimistic).id;
            }
            else {
                parentId = UNASSIGNED;
            }
        }
        
        public def getId() = id;
        
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
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (sent.size() > 0) {
                s.add("        sent:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
                
        public def lc_Get() {
            try {
                latch.lock();
                return lc;
            } finally {
                latch.unlock();
            }
        }
        
        public def lc_incrementAndGet() {
            try {
                latch.lock();
                return ++lc;
            } finally {
                latch.unlock();
            }
        }
        
        public def lc_decrementAndGet() {
            try {
                latch.lock();
                return --lc;
            } finally {
                latch.unlock();
            }
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
        
        def localFinishReleased() {
            try { 
                latch.lock();
                if (!isGlobal) {
                    //NOLOG if (verbose>=1) debug(">>>> localFinishReleased(id="+id+") true: zero localCount on local finish; releasing latch");
                    latch.release();
                    return true;
                } 
                //NOLOG if (verbose>=1) debug("<<<< localFinishReleased(id="+id+") false: global finish");
                return false;
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
                //don't decrement 'sent' unless src=dst
                if (srcId == dstId) /*signalling the termination of the root finish task, needed for correct counting of localChildren*/
                    decrement(sent, e);
                
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
            //don't decrement 'sent' unless srcId = dstId
            if (srcId == dstId) /*signalling the termination of the root finish task*/
                deduct(sent, e, cnt);
            numActive-=cnt;
            assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            resp.backupPlaceId = backupPlaceId;
            resp.backupChanged = backupChanged;
        }
        
        def quiescent():Boolean {
            //NOLOG if (verbose>=2) debug(">>>> Master(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!");
                dump();
                assert false : "COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!";
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
            val srcId = optSrc.id as Int;
            val dstId = id.home;
            val dpe:CheckedThrowable;
            if (optKind == FinishResilient.ASYNC) {
                dpe = new DeadPlaceException(Place(dstId));
                dpe.fillInStackTrace();
            } else {
                dpe = null;
            }
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") called");
                val req = FinishRequest.makeTermRequest(parentId, FinishResilient.UNASSIGNED, FinishResilient.UNASSIGNED, DUMMY_INT, DUMMY_INT, srcId, dstId, optKind);
                req.ex = dpe;
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") returning");
            });
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
                //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
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
                val lc = lc_incrementAndGet();
                //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
                //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
                FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
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
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
                val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
                req.ex = t;
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
            });
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") called");
            //no need to call notifyActivityCreation() since it does NOOP in Root finish
            Runtime.submitUncounted( ()=>{
                notifyActivityTermination(srcPlace, ASYNC);
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
            });
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, t);
            val resp = FinishReplicator.exec(req);
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
            val lc = lc_decrementAndGet();
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") called, decremented localCount to "+lc);
            if (lc > 0) {
                return;
            }
            if (lc < 0) {
                for (var i:Long = 0 ; i < 100; i++) {
                    debug("FATAL ERROR: Root(id="+id+").notifyActivityTermination reached a negative local count");
                    assert false: here + " FATAL ERROR: Root(id="+id+").notifyActivityTermination reached a negative local count";
                }
            }
            if (localFinishReleased()) {
                //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                return;
            }
            //NOLOG if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
            val resp = FinishReplicator.exec(req);
            //NOLOG if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
            //NOLOG if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called, lc = " + lc_Get() );

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
            for (e in sent.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.src) || newDead.contains(edge.dst))
                    return true;
            }
            return false;
        }
        
        public def convertToDead(newDead:HashSet[Int], countBackups:HashMap[FinishResilient.Id, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").convertToDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) && edge.dst != edge.src ) {
                    val t1 = e.getValue();
                    val t2 = countBackups.get(id);
                    assert t1 > 0 : here + " Master(id="+id+").convertToDead FATAL error, t1 must be positive";
                    if (t1 >= t2) {
                        val count = t1-t2;
                        val oldActive = numActive;
                        numActive -= count;
                        
                        //NOLOG if (verbose>=1) debug("==== Master(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
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
                    else assert false: here + " Master(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met";
                }
            }
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Master(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Master(id="+id+").convertFromDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
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
                    
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            
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
        var excs:GrowableRail[CheckedThrowable]; 
        var isAdopted:Boolean = false;
        var migrating:Boolean = false;
        var placeOfMaster:Int = -1n; //will be updated during adoption
        val optSrc:Place;
        val optKind:Int;
        var isReleased:Boolean = false;
        
        def this(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, kind:Int) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            FinishResilient.increment(sent, FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC));
            this.placeOfMaster = id.home;
            this.optSrc = src;
            this.optKind = kind;
        }        
        
        def this(id:FinishResilient.Id, _parentId:FinishResilient.Id, src:Place, kind:Int, _numActive:Long, 
                _sent:HashMap[FinishResilient.Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int) {
            this.id = id;
            this.numActive = _numActive;
            this.parentId = _parentId;
            for (e in _sent.entries()) {
                this.sent.put(e.getKey(), e.getValue());
            }
            this.placeOfMaster = _placeOfMaster;
            this.excs = _excs;
            this.optSrc = src;
            this.optKind = kind;
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Backup dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (sent.size() > 0) {
                s.add("        sent:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
        def sync(_numActive:Long, _sent:HashMap[FinishResilient.Edge,Int],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int) {
            try {
                lock();
                this.numActive = _numActive;
                this.sent.clear();
                for (e in _sent.entries()) {
                    this.sent.put(e.getKey(), e.getValue());
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
                val t = FinishResilient.Task(dstId, kind);
                //don't decrement 'sent', it holds increasing counts
                if (srcId == dstId)
                    FinishResilient.decrement(sent, e);
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
                val t = FinishResilient.Task(dstId, kind);
                //don't decrement 'sent' unless src=dst
                if (srcId == dstId)
                    FinishResilient.deduct(sent, e, cnt);
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
            val srcId = optSrc.id as Int;
            val dstId = id.home;
            val dpe:CheckedThrowable;
            if (optKind == FinishResilient.ASYNC) {
                dpe = new DeadPlaceException(Place(dstId));
                dpe.fillInStackTrace();
            } else 
                dpe = null;
            Runtime.submitUncounted( ()=>{
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") called");
                val req = FinishRequest.makeTermRequest(parentId, FinishResilient.UNASSIGNED, FinishResilient.UNASSIGNED, DUMMY_INT, DUMMY_INT, srcId, dstId, optKind);
                req.ex = dpe;
                val resp = FinishReplicator.exec(req);
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") returning");
            });
        }

        public def exec(req:FinishRequest) {
            val resp = new BackupResponse();
            val reqMaster = req.masterPlaceId;
            try {
                lock();
                if (migrating) {
                    resp.excp = new MasterDied();
                    return resp;
                }
                if (reqMaster != placeOfMaster) {
                    resp.excp = new MasterChanged(id, placeOfMaster);
                    return resp;
                }
            } finally {
                unlock();
            }
            if (req.reqType == FinishRequest.TRANSIT) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] called");
                try{
                    inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                } catch (t:Exception) {
                    resp.excp = t;
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
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + ", ex=" + ex + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    addException(ex);
                } catch (t:Exception) {
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TERM_MUL) {
                val map = req.map;
                val dstId = req.dstId;
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
                    resp.excp = t;
                }
                //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
            }
            return resp;
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
        
        public def convertToDead(newDead:HashSet[Int], countBackups:HashMap[FinishResilient.Id, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").convertToDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) && edge.src != edge.dst ) {
                    val t1 = e.getValue();
                    val t2 = countBackups.get(id);
                    assert t1 > 0 : here + " Backup(id="+id+").convertToDead FATAL error, t1 must be positive";
                    if (t1 >= t2) {
                        val count = t1-t2;
                        val oldActive = numActive;
                        numActive -= count;
                        //NOLOG if (verbose>=1) debug("==== Backup(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
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
                    else assert false: here + " Backup(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met";
                }
            }
            
            if (quiescent()) {
                isReleased = true;
                notifyParent();
                FinishReplicator.removeBackup(id);
            }
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]) {
            //NOLOG if (verbose>=1) debug(">>>> Backup(id="+id+").convertFromDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
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
                    
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            
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
            val old = sent.getOrElse(edge, 0n);
            
            if (localChildrenCount == 0n)
                sent.remove(edge);
            else
                sent.put(edge, localChildrenCount);
            //NOLOG if (verbose>=3) dump();
            var count:Long = 0;
            if (old != localChildrenCount)
                count = old - localChildrenCount;
            val oldActive = numActive;
            numActive -= count;
            //NOLOG if (verbose>=1) debug("<<<< Backup(id="+id+").setLocalTaskCount returning, old["+old+"] new["+localChildrenCount+"] numActive="+oldActive +" changing numActive to " + numActive);
        }
        
    }
    
    /*********************************************************************/
    /*******************   Failure Recovery Methods   ********************/
    /*********************************************************************/
    static def aggregateCountingRequests(newDead:HashSet[Int],
            masters:HashSet[FinishMasterState], backups:HashSet[FinishBackupState]) {
        val countingReqs = new HashMap[Int,ResolveRequest]();
        if (!masters.isEmpty()) {
            for (dead in newDead) {
                val backup = FinishReplicator.getBackupPlace(dead);
                for (mx in masters) {
                    val m = mx as OptimisticMasterState; 
                    m.lock();
                    for (entry in m.sent.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:ResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new ResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countBackups.put(BackupQueryId(m.id /*parent id*/, dead), -1n);
                        } else if (dead == edge.src) {
                            var rreq:ResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new ResolveRequest();
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
                    for (entry in b.sent.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:ResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new ResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countBackups.put(BackupQueryId(b.id, dead), -1n);
                        } else if (dead == edge.src) {
                            var rreq:ResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new ResolveRequest();
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
    
    def printResolveReqs(countingReqs:HashMap[Int,ResolveRequest]) {
        val s = new x10.util.StringBuilder();
        if (countingReqs.size() > 0) {
            for (e in countingReqs.entries()) {
                val pl = e.getKey();
                val reqs = e.getValue();
                val bkps = reqs.countBackups;
                val recvs = reqs.countReceived;
                s.add("\nRecovery requests:\n");
                s.add("   To place: " + pl + "\n");
                if (bkps.size() > 0) {
                    s.add("  countBackups:{ ");
                    for (b in bkps.entries()) {
                        s.add(b.getKey() + " ");
                    }
                    s.add("}\n");
                }
                if (recvs.size() > 0) {
                    s.add("  countReceives:{");
                    for (r in recvs.entries()) {
                        s.add("<id="+r.getKey().id+",src="+r.getKey().src + ">, ");
                    }
                    s.add("}\n");
                }
            }
        }
        debug(s.toString());
    }
    
    static def processCountingRequests(countingReqs:HashMap[Int,ResolveRequest]) {
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
        val outputGr = GlobalRef[HashMap[Int,ResolveRequest]](countingReqs);
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                val requests = countingReqs.getOrThrow(p);
                //NOLOG if (verbose>=1) debug("==== processCountingRequests  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("counting_request") async {
                        //NOLOG if (verbose>=1) debug("==== processCountingRequests  reached from " + gr.home + " to " + here);
                        val countBackups = requests.countBackups;
                        val countReceived = requests.countReceived;
                        if (countBackups.size() > 0) {
                            for (b in countBackups.entries()) {
                                val parentId = b.getKey().parentId;
                                val src = b.getKey().src;
                                val count = FinishReplicator.countBackups(parentId, src);
                                countBackups.put(b.getKey(), count);   
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
                            val output = (outputGr as GlobalRef[HashMap[Int,ResolveRequest]]{self.home == here})().getOrThrow(me);
                            
                            for (ve in countBackups.entries()) {
                                output.countBackups.put(ve.getKey(), ve.getValue());
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
            throw new Exception("FATAL ERROR: another place failed during recovery ...");
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
                val _excs = b.excs;
                val _optSrc = b.optSrc;
                val _optKind = b.optKind;
                
                val master = Place(places(i));
                if (master.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (master) @Immediate("create_masters") async {
                        val newM = new OptimisticMasterState(_id, _parentId, _numActive, _sent,
                                _excs, _optSrc, _optKind, _backupPlaceId);
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
        if (masters.size() == 0) {
            FinishReplicator.nominateBackupPlaceIfDead(here.id as Int);
            //NOLOG if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning, zero size");
            return;
        }
        
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
                val optSrc = m.optSrc;
                val optKind = m.optKind;
                val numActive = m.numActive;
                val sent = m.sent;
                val excs = m.excs;
                if (backup.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (backup) @Immediate("create_or_sync_backup") async {
                        FinishReplicator.createOptimisticBackupOrSync(id, parentId, optSrc, optKind, numActive, 
                                sent, excs, placeOfMaster);
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
        val countBackups = new HashMap[FinishResilient.Id, Int]();
        val countReceived = new HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]();
        for (e in countingReqs.entries()) {
            val v = e.getValue();
            for (ve in v.countBackups.entries()) {
                countBackups.put(ve.getKey().parentId, ve.getValue());
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
            master.convertToDead(newDead, countBackups);
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
            FinishReplicator.nominateBackupPlaceIfDead(here.id as Int);
        }

        val newMasters = new HashSet[FinishMasterState]();
        val activeBackups = new HashSet[FinishBackupState]();
        //sorting the backups is not needed. notifyParent calls are done using uncounted activities that will retry until the parent is available
        for (b in backups) {
            val backup = b as OptimisticBackupState;
            
            backup.lock();
            
            val localChildren = countLocalChildren(backup.id, backups);
                     
            backup.setLocalTaskCount(localChildren);
            
            //convert to dead
            backup.convertToDead(newDead, countBackups);
            
            if (!backup.isReleased) {
                //convert from dead
                backup.convertFromDead(newDead, countReceived);
            }
            
            if (!backup.isReleased){
                activeBackups.add(backup);
            }
            backup.unlock(); //as long as we keep migrating = true, no changes will be permitted on backup
        }
        
        if (backups.size() > 0) {
            createMasters(activeBackups);
        } else {
            //NOLOG if (verbose>=1) debug("==== createMasters bypassed ====");
            return;
        }
        
        //NOLOG if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
    
}

class ResolveRequest {
    val countBackups = new HashMap[FinishResilientOptimistic.BackupQueryId, Int]();
    val countReceived = new HashMap[FinishResilientOptimistic.ReceivedQueryId, Int]();
}