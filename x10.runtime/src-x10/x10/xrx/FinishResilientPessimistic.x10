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
import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.xrx.freq.FinishRequest;
import x10.xrx.freq.AddChildRequestPes;
import x10.xrx.freq.ExcpRequestPes;
import x10.xrx.freq.LiveRequestPes;
import x10.xrx.freq.TermRequestPes;
import x10.xrx.freq.TransitRequestPes;
import x10.xrx.freq.TransitTermRequestPes;

/**
 * Distributed Resilient Finish (records transit and live tasks)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 * Implementation notes: remote objects are not shared and are not persisted in a static hashmap (no special GC needed)
 * Failure assumptions:  master & backup failure not permitted                         
 */
class FinishResilientPessimistic extends FinishResilient implements CustomSerialization {
    private static val DUMMY_INT = -1n;
    
    protected transient var me:FinishResilient; // local finish object
    val id:Id;

    public def toString():String { 
        return me.toString();
    }
    
    /* Recovery related structs*/
    protected static struct ChildQueryId(id:Id, childId:Id) {
        public def toString() = "<childQuery id=" + id + " childId=" + childId +">";
        def this(id:Id, childId:Id) {
            property(id, childId);
        }
    }
    
    def notifySubActivitySpawn(place:Place):void { me.notifySubActivitySpawn(place); } /*Blocking replication*/
    def notifyShiftedActivitySpawn(place:Place):void { me.notifyShiftedActivitySpawn(place); } /*Blocking replication*/
    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean { return me.notifyActivityCreation(srcPlace, activity); }
    def notifyShiftedActivityCreation(srcPlace:Place):Boolean { return me.notifyShiftedActivityCreation(srcPlace); } /*Blocking replication*/
    def notifyRemoteContinuationCreated():void { me.notifyRemoteContinuationCreated(); }
    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { me.notifyActivityCreationFailed(srcPlace, t); }
    def notifyActivityCreatedAndTerminated(srcPlace:Place):void { me.notifyActivityCreatedAndTerminated(srcPlace); }
    def pushException(t:CheckedThrowable):void { me.pushException(t); } /*Blocking replication*/
    def notifyActivityTermination(srcPlace:Place):void { me.notifyActivityTermination(srcPlace); }
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def spawnRemoteActivity(place:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { me.spawnRemoteActivity(place, body, prof); }
    def waitForFinish():void { me.waitForFinish(); }
    //def globalInit() /*Blocking replication*/
    
    def notifyActivityTermination(srcPlace:Place,t:CheckedThrowable):void {
        if (me instanceof PessimisticRemoteState)
            me.notifyActivityTermination(srcPlace, t);
        else
            super.notifyActivityTermination(srcPlace, t);
    }
    
    def notifyActivityCreatedAndTerminated(srcPlace:Place,t:CheckedThrowable):void { 
        if (me instanceof PessimisticRemoteState)
            me.notifyActivityCreatedAndTerminated(srcPlace, t);
        else
            super.notifyActivityCreatedAndTerminated(srcPlace, t);
    }
    
    def notifyShiftedActivityCompletion(srcPlace:Place,t:CheckedThrowable):void {
        if (me instanceof PessimisticRemoteState)
            me.notifyShiftedActivityCompletion(srcPlace, t);
        else
            super.notifyShiftedActivityCompletion(srcPlace, t);
    }
    
    //create root finish
    public def this (parent:FinishState) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        val grlc = GlobalRef[AtomicInteger](new AtomicInteger(1n));
        me = new PessimisticMasterState(id, parent, grlc);
        FinishReplicator.addMaster(id, me as FinishMasterState);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+", grlc=1) created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        val lc = deser.readAny() as GlobalRef[AtomicInteger];
        if (lc.home == here) {
            me = new PessimisticRemoteState(id, lc);
            if (verbose>=1) debug("<<<< RemoteFinish(id="+id+",lcHome="+lc.home+") createdA");    
        } else {
            val grlc = GlobalRef[AtomicInteger](new AtomicInteger(1n));
            me = new PessimisticRemoteState(id, grlc);
            if (verbose>=1) debug("<<<< RemoteFinish(id="+id+",lcHome="+lc.home+") createdB with new lc=1");
        }
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        return new FinishResilientPessimistic(parent);
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        if (me instanceof PessimisticMasterState) {
            val me2 = (me as PessimisticMasterState); 
            if (!me2.isGlobal)
                me2.globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
        }
        ser.writeAny(id);
        val grlc = me instanceof PessimisticMasterState ?
              (me as PessimisticMasterState).grlc : (me as PessimisticRemoteState).grlc ;
        ser.writeAny(grlc);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    //REMOTE
    public static final class PessimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
        private val id:Id; //parent root finish
        private var adopterId:Id = UNASSIGNED;
        private val ilock = new Lock(); //instance lock
        private val grlc:GlobalRef[AtomicInteger];
        private def localCount():AtomicInteger = (grlc as GlobalRef[AtomicInteger]{self.home == here})();
        
        private var ex:CheckedThrowable = null;
        
        public def this (val id:Id, grlc:GlobalRef[AtomicInteger]) {
            this.id = id;
            this.grlc = grlc;
        }
        
        private def forgetGlobalRefs():void {
            (grlc as GlobalRef[AtomicInteger]{self.home==here}).forget();
        }
        
        private def setAdopter(aid:Id) {
            if (id != UNASSIGNED) {
                ilock.lock();
                adopterId = aid;
                if (verbose>=1) debug(">>>> Remote(id="+id+").setAdopter to " + aid);
                ilock.unlock();
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
                val lc = localCount().incrementAndGet();
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+here.id + " dstId="+dstId+" kind="+kind+") called locally, localCount now "+lc);
            } else {
                val parentId = UNASSIGNED;
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(parentId="+parentId+",adopterId="+adopterId+",srcId="+here.id + " dstId="+dstId+" kind="+kind+") called ");
                val req = FinishRequest.makePesTransitRequest(id, parentId, adopterId, srcId, dstId, kind);
                val resp = FinishReplicator.exec(req);
                setAdopter(resp.adopterId);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(parentId="+parentId+",adopterId="+adopterId+",srcId="+here.id + " dstId="+dstId+" kind="+kind+") returning");
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
            if (verbose>=1) debug(">>>> Remote(id="+id+").spawnRemoteActivity(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId+",dstId="+dstId+",kind="+kind+",bytes="+bytes.size+") called");
            val req = FinishRequest.makePesTransitRequest(id, parentId, adopterId, srcId, dstId, kind);
            val num = req.num;
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
                if (adopterId != UNASSIGNED)
                    this.setAdopter(adopterId);
            };
            val lc = localCount().incrementAndGet();// synthetic activity to keep finish locally live during async replication
            if (verbose>=1) debug("<<<< Remote(id="+id+").spawnRemoteActivity(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId+",dstId="+dstId+",kind="+kind+") incremented localCount to " + lc);
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         * Since the replication protocol is blocking, we create an uncounted activity to perform replication.
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val kind = FinishResilient.ASYNC;
            if (srcId == dstId) {
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") called locally. no action required");
                return true;
            }
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(parentId="+parentId+",adopterId="+adopterId+",srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesLiveRequest(id, parentId, adopterId, srcId, dstId, kind);
            val preSendAction = ()=>{};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (submit) {
                    if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") finally pushing activity ["+activity+"]");
                    Runtime.worker().push(activity);
                } else {
                    if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") not pushing activity");
                }
                if (adopterId != UNASSIGNED)
                    this.setAdopter(adopterId);
            };
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(parentId="+parentId+",adopterId="+adopterId+",isrcId=" + srcId + ",dstId="+dstId+",kind="+kind+") returning");
            return false;
        }
        
        /*
         * See similar method: notifyActivityCreation
         * Blocking is allowed here.
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val kind = FinishResilient.AT;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyShiftedActivityCreation(parentId="+parentId+",adopterId="+adopterId+",srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesLiveRequest(id, parentId, adopterId, srcId, dstId, kind);
            val resp = FinishReplicator.exec(req);
            setAdopter(resp.adopterId);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(parentId="+parentId+",adopterId="+adopterId+",srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") returning (submit="+resp.submit+")");
            return resp.submit;
        }
        
        def notifyRemoteContinuationCreated():void { /*noop for remote finish*/ }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        //we cannot block in this method, because it can be called from an immediate thread
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(parentId="+parentId+",adopterId="+adopterId+",srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            val req = FinishRequest.makePesTransitRequest(id, parentId, adopterId, srcId, dstId, kind);
            val preSendAction = ()=>{};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (adopterId != UNASSIGNED)
                    this.setAdopter(adopterId);
            };
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(parentId="+parentId+",adopterId="+adopterId+",srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, null);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int, t:CheckedThrowable) {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            if (t != null)
                ex = t;
            if (srcId == dstId) {
                //perform termination steps, but use TRANSIT_TERM request rather than TERM
                val lc = localCount().decrementAndGet();

                if (lc > 0) {
                    if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated called, decremented localCount to "+lc);
                    return;
                }
                
                if (lc < 0) {
                    for (var i:Long = 0 ; i < 100; i++) {
                        debug("FATAL ERROR: Remote(id="+id+").notifyActivityCreatedAndTerminated a negative local count");
                        assert false: "FATAL ERROR: Remote(id="+id+").notifyActivityCreatedAndTerminated reached a negative local count";
                    }
                }
                
                // If this is not the root finish, we are done with the finish state.
                // If this is the root finish, it will be kept alive because waitForFinish
                // is an instance method and it is on the stack of some activity.
                forgetGlobalRefs();
            }
            //we cannot block in this method, because it can be called from an immediate thread
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") called");
            val req = FinishRequest.makePesTransitTermRequest(id, parentId, adopterId, srcId, dstId, kind, ex);
            val preSendAction = ()=>{};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (adopterId != UNASSIGNED)
                    this.setAdopter(adopterId);
            };
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            assert (Runtime.activity() != null) : here + " >>>> Remote(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+",adopterId="+adopterId+") called");
            val req = FinishRequest.makePesExcpRequest(id, parentId, adopterId, t);
            val resp = FinishReplicator.exec(req, null);
            if (resp.adopterId != UNASSIGNED)
                this.setAdopter(resp.adopterId);
            if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+",adopterId="+adopterId+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, null);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, null);
        }
        def notifyActivityTermination(srcPlace:Place, kind:Int, t:CheckedThrowable):void {
            val lc = localCount().decrementAndGet();
            if (t != null)
                ex = t;
            if (lc > 0) {
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination called, decremented localCount to "+lc);
                return;
            }
            
            if (lc < 0) {
                assert false: "FATAL ERROR: Remote(id="+id+").notifyActivityTermination reached a negative local count";
            }
            
            // If this is not the root finish, we are done with the finish state.
            // If this is the root finish, it will be kept alive because waitForFinish
            // is an instance method and it is on the stack of some activity.
            forgetGlobalRefs();
            
            val parentId = UNASSIGNED;
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") called");
            val req = FinishRequest.makePesTermRequest(id, parentId, adopterId, srcId, dstId, kind, ex);                                    
            val preSendAction = ()=>{};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (adopterId != UNASSIGNED)
                    this.setAdopter(adopterId);
            };
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(parentId="+parentId+",adopterId="+adopterId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") returning");
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
        
        def notifyActivityTermination(srcPlace:Place,t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, ASYNC, t);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place,t:CheckedThrowable):void {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, t);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place,t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, AT, t);
        }
    }
    
    //ROOT
    public static final class PessimisticMasterState extends FinishMasterState implements x10.io.Unserializable {
        val id:Id;
        val parentId:Id; //the direct parent of this finish (could be a remote a finish) 
        val parent:FinishState; //the direct parent finish object (used in globalInit for recursive initializing)
        val latch:SimpleLatch = new SimpleLatch(); //latch for blocking the finish activity, also used as the instance lock

        //resilient finish counter set
        var numActive:Long;
        val live = new HashMap[Task,Int](); 
        var transit:HashMap[Edge,Int] = null; // lazily allocated
        var liveAdopted:HashMap[Task,Int] = null; // lazily allocated 
        var transitAdopted:HashMap[Edge,Int] = null; // lazily allocated
        
        var children:HashSet[Id] = null; // lazily allocated
        var isGlobal:Boolean = false; //flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        
        //may be updated in notifyPlaceDeath
        var backupPlaceId:Int = FinishReplicator.getBackupPlace(here.id as Int);
        var backupChanged:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        val grlc:GlobalRef[AtomicInteger];
        var migrating:Boolean = false; 
        
        private def localCount():AtomicInteger = (grlc as GlobalRef[AtomicInteger]{self.home == here})();
                
        private def forgetGlobalRefs():void {
            (grlc as GlobalRef[AtomicInteger]{self.home==here}).forget();
        }
        
        def this(id:Id, parent:FinishState, grlc:GlobalRef[AtomicInteger]) {
            this.id = id;
            this.numActive = 1;
            this.parent = parent;
            this.grlc = grlc;
            live.put(Task(id.home, ASYNC), 1n);
            if (parent instanceof FinishResilientPessimistic) {
                parentId = (parent as FinishResilientPessimistic).id;
            } else {
                parentId = UNASSIGNED;
            }
            if (verbose>=1) debug("<<<< Root(id="+id+") constructor returning, backupPlaceId="+backupPlaceId);
        }
        
        /**
         * makeBackup is true only when a parent finish is forced to be global by its child,
         * otherwise, backup is created with the first transit request 
         * 
         * We couldn't avoid calling globalInit in new instaces
         * due to the need to call add child on parents who may be remote.
         * */
        private def globalInit(makeBackup:Boolean) {
            latch.lock();
            if (!isGlobal) {
                if (verbose>=1) debug(">>>> doing globalInit for id="+id);
                if (parent instanceof FinishResilientPessimistic) {
                    val frParent = parent as FinishResilientPessimistic;
                    if (frParent.me instanceof PessimisticMasterState) {
                        val x = (frParent.me as PessimisticMasterState);
                        if (x.isGlobal) {
                            val req = FinishRequest.makePesAddChildRequest(parentId /*id*/, id /*child_id*/);
                            FinishReplicator.exec(req);
                        } else {
                            x.addChild(id);
                            x.globalInit(true);
                        }
                    }
                }
                if (makeBackup)
                    createBackup(backupPlaceId);
                isGlobal = true;
                strictFinish = true;
                if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            latch.unlock();
        }
        
        private def createBackup(backupPlaceId:Int) {
            //TODO: redo if backup is dead
            if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
            val backup = Place(backupPlaceId);
            if (backup.isDead()) {
                if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
                return false;
            }
            val myId = id; //don't copy this
            val myParentId = parentId;
            val myChildren = children;
            val rCond = ResilientCondition.make(backup);
            val closure = (gr:GlobalRef[Condition]) => {
                at (backup) @Immediate("backup_create") async {
                    val bFin = FinishReplicator.pesFindBackupOrCreate(myId, myParentId, myChildren);
                    at (gr) @Immediate("backup_create_response") async {
                        gr().release();
                    }
                }; 
            };
            rCond.run(closure);
            if (rCond.failed()) {
                val excp = new DeadPlaceException(backup);
            }
            rCond.forget();
            return true;
        }
        
        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Root dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("     localCount:"); s.add(localCount().get()); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (children != null) {
                s.add("       children: {"); 
                for (e in children) {
                    s.add( e + " ");
                }
                s.add("}\n");
            }
            //s.add("      adopterId: " + adopterId); s.add('\n');
            if (live.size() > 0) {
                s.add("           live:\n");
                for (e in live.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (liveAdopted != null && liveAdopted.size() > 0) {
                s.add("    liveAdopted:\n"); 
                for (e in liveAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (transit != null && transit.size() > 0) {
                s.add("        transit:\n"); 
                for (e in transit.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (transitAdopted != null && transitAdopted.size() > 0) {
                s.add(" transitAdopted:\n"); 
                for (e in transitAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
        public def getId() = id;
        public def getBackupId() = backupPlaceId;
        
        public def lock() {
            latch.lock();
        }
        
        public def unlock() {
            latch.unlock();
        }
        
        def liveAdopted() {
            if (liveAdopted == null) liveAdopted = new HashMap[Task,Int]();
            return liveAdopted;
        }

        def transit() {
            if (transit == null) transit = new HashMap[Edge,Int]();
            return transit;
        }

        def transitAdopted() {
            if (transitAdopted == null) transitAdopted = new HashMap[Edge,Int]();
            return transitAdopted;
        }
        
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
            if (verbose>=1) debug("<<<< addExceptionUnsafe(id="+id+") t="+t.getMessage() + " exceptions size = " + excs.size());
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
            if (t == null)
                return;
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
                    if (verbose>=1) debug(">>>> localFinishExceptionPushed(id="+id+") true");
                    addExceptionUnsafe(t);
                    return true;
                } 
                if (verbose>=1) debug("<<<< localFinishExceptionPushed(id="+id+") false: global finish");
                return false;
            } finally {
                latch.unlock();
            }
        }
       
        def addChild(child:Id) {
            if (verbose>=1) debug(">>>> Master(id="+id+").addChild(child=" + child + ") called");
            try {
                latch.lock();
                if (children == null) {
                    children = new HashSet[Id]();
                }
                children.add(child);
                if (verbose>=1) debug("<<<< Master(id="+id+").addChild(child=" + child + ") returning");
            } finally {
                latch.unlock();
            }
        }
        
        def addChild(child:Id, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").addChild(child=" + child + ") called");
            try {
                latch.lock();
                if (children == null) {
                    children = new HashSet[Id]();
                }
                children.add(child);
                if (verbose>=1) debug("<<<< Master(id="+id+").addChild(child=" + child + ") returning");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").inTransit called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                if (!toAdopter) {
                    increment(transit(), e);
                    numActive++;
                } else {
                    increment(transitAdopted(), e);
                    numActive++;
                }
                if (verbose>=3) debug("==== Master(id="+id+").inTransit "+tag+" after update for: "+srcId + " ==> "+dstId+" kind="+kind);
                if (verbose>=3) dump();
                
                if (verbose>=1) debug("<<<< Master(id="+id+").inTransit returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                 latch.unlock();
            }
        }
        
        def transitToLive(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToLive srcId=" + srcId + ", dstId=" + dstId + " called");
            try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                val t = Task(dstId, kind);
                if (!toAdopter) {
                    increment(live,t);
                    decrement(transit(), e);
                } else {
                    increment(liveAdopted(), t);
                    decrement(transitAdopted(), e);
                }
                if (verbose>=3) debug("==== Master(id="+id+").transitToLive "+tag+" after update for: "+srcId + " ==> "+dstId+" kind="+kind);
                if (verbose>=3) dump();
                
                if (verbose>=1) debug("<<<< Master(id="+id+").transitToLive returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, toAdopter:Boolean, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") called");
            try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                if (!toAdopter) {
                    decrement(transit(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        releaseLatch();
                        FinishReplicator.removeMaster(id);
                    }
                } else {
                    decrement(transitAdopted(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        releaseLatch();
                        FinishReplicator.removeMaster(id);
                    }
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") returning");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, ex:CheckedThrowable, tag:String, toAdopter:Boolean, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").liveToCompleted called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ")");
            try {
                latch.lock();
                if (ex != null)
                    addExceptionUnsafe(ex);
                val t = Task(dstId, kind);
                if (!toAdopter) {
                    decrement(live, t);
                    numActive--;
                    if (quiescent()) {
                        releaseLatch();
                        FinishReplicator.removeMaster(id);
                    }
                } else {
                    decrement(liveAdopted(), t);
                    numActive--;
                    if (quiescent()) {
                        releaseLatch();
                        FinishReplicator.removeMaster(id);
                    }
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").liveToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ")");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Master(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!");
                dump();
                assert false : "COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            if (verbose>=3) dump();
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Master(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        
        def releaseLatch() {
            val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
            if (verbose>=2) debug("Master(id="+id+") releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));
            latch.release();
            if (verbose>=2) debug("Master(id="+id+").releaseLatch returning");
        }
        
        public def exec(xreq:FinishRequest) {
            val id = xreq.id;
            val resp = new MasterResponse();
            
            /**AT_FINISH HACK**/
            if (id == Id(0n,0n) && xreq.isToAdopter()) //ignoring lost at_finish requests forward to <0,0>
                return resp; 
            
            try {
                lock();
                if (migrating && !xreq.isLocal) {
                    resp.errMasterMigrating = true;
                    if (verbose>=1) debug("<<<< Master(id="+id+").exec returning migrating["+migrating+"] isLocal["+xreq.isLocal+"]");
                    return resp;
                }
            } finally {
                unlock();
            }
            
            if (xreq instanceof AddChildRequestPes) {
                val req = xreq as AddChildRequestPes;
                val childId = req.childId;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=ADD_CHILD, masterId="+id+", childId="+childId+"] called");
                addChild(childId, resp);
                resp.submit = true;
                resp.parentIdHome = parentId.home;
                resp.parentIdSeq = parentId.id;
                if (verbose>=1) debug("<<<< Master(id="+id+").exec returning [req=ADD_CHILD, masterId="+id+", childId="+childId+"]");
            } else if (xreq instanceof TransitRequestPes) {
                val req = xreq as TransitRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + " ] called");
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                } else if (Place(dstId).isDead()) {
                    if (kind == ASYNC) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                        addDeadPlaceException(dstId, resp);
                        resp.transitSubmitDPE = true;
                    } else {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                    }
                } else {
                    inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter, resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit="+resp.submit+" ] returning");
            } else if (xreq instanceof LiveRequestPes) {
                val req = xreq as LiveRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                resp.submit = false;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + " ] called");
                var msg:String = kind == ASYNC ? "notifyActivityCreation":"notifyShiftedActivityCreation";
                if (Place(srcId).isDead() || Place(dstId).isDead()) {
                    // NOTE: no state updates or DPE processing here.
                    //       Must happen exactly once and is done
                    //       when Place0 is notified of a dead place.
                    if (verbose>=1) debug("==== Master(id="+id+").exec "+msg+" suppressed: "+srcId + " ==> "+dstId+" kind="+kind);
                } else {
                    transitToLive(srcId, dstId, kind, msg, toAdopter, resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit=" + resp.submit + " ] returning");
            } else if (xreq instanceof TermRequestPes) {
                val req = xreq as TermRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + " ] called");
                if (Place(dstId).isDead()) {
                    // NOTE: no state updates or DPE processing here.
                    //       Must happen exactly once and is done
                    //       when Place0 is notified of a dead place.
                    if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
                } else {
                    liveToCompleted(srcId, dstId, kind, ex, "notifyActivityTermination", toAdopter, resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit=" + resp.submit + " ] returning");
            } else if (xreq instanceof ExcpRequestPes) {
                val req = xreq as ExcpRequestPes;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                addException(ex, resp);
                resp.submit = true;
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (xreq instanceof TransitTermRequestPes) {
                val req = xreq as TransitTermRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ex+" ] called");
                if (Place(srcId).isDead() || Place(dstId).isDead()) {
                    // NOTE: no state updates or DPE processing here.
                    //       Must happen exactly once and is done
                    //       when Place0 is notified of a dead place.
                    if (verbose>=1) debug("==== notifyActivityCreationFailed(id="+id+") suppressed: "+srcId + " ==> "+dstId+" kind="+kind);
                } else {
                    transitToCompleted(srcId, dstId, kind, ex, toAdopter, resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ex+" ] returning");
            } else {
                if (verbose>=1) debug(">>>> Master(id="+id+").exec FATAL unknown request type");
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
            if (dstId == here.id as Int) {
                val lc = localCount().incrementAndGet();
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makePesTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind);
                FinishReplicator.exec(req, this);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            globalInit(false); // go global now, and don't rely on asyncExec to do so, because globalInit is a blocking operation
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            
            if (verbose>=1) debug(">>>> Root(id="+id+").spawnRemoteActivity(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesTransitRequest(id, parentId, UNASSIGNED, srcId, dstId, kind);
            val num = req.num;
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
            };
            val lc = localCount().incrementAndGet(); // synthetic activity to keep finish locally live during async replication
            FinishReplicator.asyncExec(req, this, preSendAction, postSendAction);
            if (verbose>=1) debug("<<<< Root(id="+id+").spawnRemoteActivity(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, incremented localCount to " + lc);
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         * Since the replication protocol is blocking, we create an uncounted activity to perform replication.
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val kind = ASYNC;
            if (srcId == dstId) {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called locally. no action required");
                return true;
            }
            
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesLiveRequest(id, parentId, UNASSIGNED, srcId, dstId, kind);
            val preSendAction = ()=>{};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (submit) {
                    if (verbose>=1) debug("==== Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") finally pushing activity ["+activity+"]");
                    Runtime.worker().push(activity);
                } else {
                    if (verbose>=1) debug("==== Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") not pushing activity");
                }
            };
            FinishReplicator.asyncExec(req, this, preSendAction, postSendAction);
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returned");
            return false;
        }
        
        /*
         * See similar method: notifyActivityCreation
         * Blocking is allowed here.
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val kind = AT;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyShiftedActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesLiveRequest(id, parentId, UNASSIGNED, srcId, dstId, kind);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyShiftedActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning (submit="+resp.submit+")");
            return resp.submit;
        }
        
        def notifyRemoteContinuationCreated():void { 
            strictFinish = true;
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyRemoteContinuationCreated() isGlobal = "+isGlobal);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            //we cannot block in this method, because it can be called from an immediate thread
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            val req = FinishRequest.makePesTransitTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, t);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (srcId == dstId) {
                val lc = localCount().decrementAndGet();

                if (lc > 0) {
                    if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated() called, decremented localCount to "+lc);
                    return;
                }
                
                if (lc < 0) {
                    assert false: "FATAL ERROR: Root(id="+id+").notifyActivityCreatedAndTerminated() reached a negative local count";
                }
                
                // If this is not the root finish, we are done with the finish state.
                // If this is the root finish, it will be kept alive because waitForFinish
                // is an instance method and it is on the stack of some activity.
                forgetGlobalRefs();
                
                if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                    if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated() returning");
                    latch.release();
                    return;
                }
            }
            //we cannot block in this method, because it can be called from an immediate thread
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesTransitTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, null);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            assert (Runtime.activity() != null) : here + " >>>> Root(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makePesExcpRequest(id, parentId, UNASSIGNED, t);
            FinishReplicator.exec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT);
        }
        def notifyActivityTermination(srcPlace:Place, kind:Int):void {
            val lc = localCount().decrementAndGet();

            if (lc > 0) {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination() called, decremented localCount to "+lc);
                return;
            }
            
            assert lc == 0n: "FATAL ERROR: Root(id="+id+").notifyActivityTermination() reached a negative local count";
            
            // If this is not the root finish, we are done with the finish state.
            // If this is the root finish, it will be kept alive because waitForFinish
            // is an instance method and it is on the stack of some activity.
            forgetGlobalRefs();
            
            if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                if (verbose>=1) debug(">>>> notifyActivityTermination(id="+id+") zero localCount on local finish; releasing latch");
                latch.release();
                return;
            }
            
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makePesTermRequest(id, parentId, UNASSIGNED, srcId, dstId, kind, null);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
            if (verbose>=1) debug(">>>> waitForFinish(id="+id+") called, lc = " + localCount().get() );

            // terminate myself
            notifyActivityTermination(here);

            // If we haven't gone remote with this finish yet, see if this worker
            // can execute other asyncs that are governed by the finish before waiting on the latch.
            if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS || !strictFinish)) {
                if (verbose>=2) debug("calling worker.join for id="+id);
                Runtime.worker().join(this.latch);
            }

            // wait for the latch release
            if (verbose>=2) debug("calling latch.await for id="+id);
            latch.await(); // wait for the termination (latch may already be released)
            if (verbose>=2) debug("returned from latch.await for id="+id);

            // no more messages will come back to this finish state 
            forgetGlobalRefs();
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("RootFinish(id="+id+") throwing MultipleExceptions size=" + excs.size());
                throw new MultipleExceptions(excs);
            }
            
            if (id == TOP_FINISH) {
                //blocks until final replication messages sent from place 0
                //are responded to.
                FinishReplicator.finalizeReplication();
            }
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        public def isImpactedByDeadPlaces(newDead:HashSet[Int]) {
            if (!isGlobal)
                return false;
            if (newDead.contains(backupPlaceId))
                return true;
            for (e in live.entries()) {
                val task = e.getKey();
                if (newDead.contains(task.place))
                    return true;
            }

            if (transit != null) {
                for (e in transit.entries()) {
                    val edge = e.getKey();
                    if (newDead.contains(edge.src) || newDead.contains(edge.dst))
                        return true;
                }
            }
            
            if (liveAdopted != null) {
                for (e in liveAdopted.entries()) {
                    val task = e.getKey();
                    if (newDead.contains(task.place))
                        return true;
                }
            }
            
            if (transitAdopted != null) {
                for (e in transitAdopted.entries()) {
                    val edge = e.getKey();
                    if (newDead.contains(edge.src) || newDead.contains(edge.dst))
                        return true;
                }
            }
            
            if (children != null) {
                for (child in children) {
                    if (newDead.contains(child.home))
                        return true;
                }
            }
            
            return false;
        }
        
        public def adoptChildren(set:HashSet[ChildAdoptionResponse]) {
            if (set == null || set.size() == 0)
                return;
            for (child in set) {
            	if (!child.found) {
            		if (verbose>=1) debug("Root(id="+id+") ignore released child ["+child+"]");
            		continue;
            	}
            	if (verbose>=1) debug("Root(id="+id+") adopting child ["+child+"]");
                val asla = liveAdopted();
                for (entry in child.live.entries()) {
                    val task = entry.getKey();
                    asla.put(task, asla.getOrElse(task,0n) + entry.getValue());
                }
                
                for (entry in child.liveAdopted.entries()) {
                    val task = entry.getKey();
                    asla.put(task, asla.getOrElse(task,0n) + entry.getValue());
                }

                val asta = transitAdopted();
                for (entry in child.transit.entries()) {
                    val edge = entry.getKey();
                    asta.put(edge, asta.getOrElse(edge,0n) + entry.getValue());
                }
                for (entry in child.transitAdopted.entries()) {
                    val edge = entry.getKey();
                    asta.put(edge, asta.getOrElse(edge,0n) + entry.getValue());
                }
                numActive += child.numActive;
                
                if (verbose>=3) debug("Root(id="+id+") state after adopting child["+child+"]");
                if (verbose>=3) dump();
            }
        }
        
        def convertDeadActivities(newDead:HashSet[Int]) {
            // NOTE: can't say for (p in Place.places()) because we need to see the dead places
            for (dead in newDead) {
                val deadTasks = new HashSet[Task]();
                for (k in live.keySet()) {
                    if (k.place == dead) deadTasks.add(k);
                }
                
                for (dt in deadTasks) {
                    val count = live.remove(dt);
                    numActive -= count;
                    if (dt.kind == ASYNC) {
                        for (1..count) {
                            if (verbose>=3) debug("adding DPE to "+id+" for live async at "+dead);
                            val dpe = new DeadPlaceException(Place(dead));
                            dpe.fillInStackTrace();
                            addExceptionUnsafe(dpe);
                        }
                    }
                }

                if (liveAdopted != null) {
                    val deadWards = new HashSet[Task]();
                    for (k in liveAdopted.keySet()) {
                        if (k.place == dead) deadWards.add(k);
                    }
                    for (dw in deadWards) {
                        val count = liveAdopted.remove(dw);
                        numActive -= count;
                    }
                }
                  
                if (transit != null) {
                    val deadEdges = new HashSet[Edge]();
                    for (k in transit.keySet()) {
                        if (k.src == dead || k.dst == dead) deadEdges.add(k);
                    }
                    for (de in deadEdges) {
                        val count = transit.remove(de);
                        numActive -= count;
                        if (de.kind == ASYNC && de.dst == dead) {
                            for (1..count) {
                                if (verbose>=3) debug("adding DPE to "+id+" for transit asyncs("+de.src+","+dead+")");
                                val dpe = new DeadPlaceException(Place(dead));
                                dpe.fillInStackTrace();
                                addExceptionUnsafe(dpe);
                            }
                        }
                    }
                }

                if (transitAdopted != null) {
                    val deadEdges = new HashSet[Edge]();
                    for (k in transitAdopted.keySet()) {
                        if (k.src == dead || k.dst == dead) deadEdges.add(k);
                    }
                    for (de in deadEdges) {
                        val count = transitAdopted.remove(de);
                        numActive -= count;
                    }
                }
            }
            
            if (quiescent()) {
                releaseLatch();
                FinishReplicator.removeMaster(id);
            }
        }
    }
    
    //BACKUP
    public static final class PessimisticBackupState extends FinishBackupState implements x10.io.Unserializable {
        val ilock = new Lock(); //instance lock
        val id:Id; //finish id 
        val parentId:Id; //the root parent id

        //resilient finish counter set
        var numActive:Long;
        var live:HashMap[FinishResilient.Task,Int] = null; //lazily allocated
        var transit:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
        var liveAdopted:HashMap[FinishResilient.Task,Int] = null; // lazily allocated 
        var transitAdopted:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
        
        var children:HashSet[Id] = null; //the nested finishes within this finish scope
        var excs:GrowableRail[CheckedThrowable];  //not really needed in backup
        var migrating:Boolean = false;  //used to prevent updating the backup during recovery
        
        var isAdopted:Boolean = false;
        var adopterId:Id;  //the adopter id 
        var placeOfAdopter:Int = -1n; //the place of the adopter
        
        var isReleased:Boolean = false;

        def this(id:Id, parentId:Id) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            this.live = new HashMap[FinishResilient.Task,Int]();
            increment(live, Task(id.home, FinishResilient.ASYNC));
        }
        
        def this(id:Id, parentId:Id, children:HashSet[Id]) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            this.live = new HashMap[FinishResilient.Task,Int]();
            this.children = children;
            increment(live, Task(id.home, FinishResilient.ASYNC));
        }
        
        def this(_id:Id, _parentId:Id, _numActive:Long, _live:HashMap[FinishResilient.Task,Int],
                _transit:HashMap[FinishResilient.Edge,Int], _liveAdopted:HashMap[FinishResilient.Task,Int],
                _transitAdopted:HashMap[FinishResilient.Edge,Int], _children:HashSet[Id],
                _excs:GrowableRail[CheckedThrowable]) {
            this.id = _id;
            this.numActive = _numActive;
            this.parentId = _parentId;
            this.live = _live;
            if (live == null)
                live = new HashMap[FinishResilient.Task,Int]();
            this.transit = _transit;
            this.liveAdopted = _liveAdopted; 
            this.transitAdopted = _transitAdopted;
            this.children = _children;
            this.excs = _excs;
        }
        
        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Backup dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (children != null) {
                s.add("       children: {"); 
                for (e in children) {
                    s.add( e + " ");
                }
                s.add("}\n");
            }
            //s.add("      adopterId: " + adopterId); s.add('\n');
            if (live.size() > 0) {
                s.add("           live:\n");
                for (e in live.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (liveAdopted != null && liveAdopted.size() > 0) {
                s.add("    liveAdopted:\n"); 
                for (e in liveAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (transit != null && transit.size() > 0) {
                s.add("        transit:\n"); 
                for (e in transit.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (transitAdopted != null && transitAdopted.size() > 0) {
                s.add(" transitAdopted:\n"); 
                for (e in transitAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
        def sync(_numActive:Long, _live:HashMap[FinishResilient.Task,Int],
                _transit:HashMap[FinishResilient.Edge,Int], _liveAdopted:HashMap[FinishResilient.Task,Int],
                _transitAdopted:HashMap[FinishResilient.Edge,Int], _children:HashSet[Id],
                _excs:GrowableRail[CheckedThrowable]):void {
            if (verbose>=3) {
                debug(">>>> Backup.sync changing numActivty from("+this.numActive+") to("+_numActive+")");
                dump();
            }
            this.numActive = _numActive;
            this.live = _live;
            if (live == null)
                live = new HashMap[FinishResilient.Task,Int]();
            this.transit = _transit;
            this.liveAdopted = _liveAdopted; 
            this.transitAdopted = _transitAdopted;
            this.children = _children;
            this.excs = _excs;
            if (verbose>=3) dump();
        }
        
        //call by the adopter to acquire a child finish through its backup
        def acquire(newDead:HashSet[Int], _adopterId:Id, resp:ChildAdoptionResponse) {
            try {
                lock();
                if (verbose>=3) debug("==== Backup(id="+id+").acquire (adopterId=" + _adopterId + ") dumping state");
                if (verbose>=3) dump();
                resp.found = true;
                if (children != null) {
                    for (child in children) {
                        if (child.home == id.home) { //local child
                            val bFin = FinishReplicator.findBackup(child);
                            if (bFin != null) {
                                (bFin as PessimisticBackupState).acquire(newDead, _adopterId, resp);
                            }
                        }
                        else if (newDead.contains(child.home)) { //grandchildren that should be adopted too
                            resp.children.add(child); 
                        }
                    }
                }
                
                if (live.size() > 0) {
                    for (e in live.entries()) {
                        resp.live.put(e.getKey(), e.getValue());
                    }
                }
                if (transit != null && transit.size() > 0) {
                    for (e in transit.entries()) {
                        resp.transit.put(e.getKey(), e.getValue());
                    }
                }
                if (liveAdopted != null && liveAdopted.size() > 0) {
                    for (e in liveAdopted.entries()) {
                        resp.liveAdopted.put(e.getKey(), e.getValue());
                    }
                }
                if (transitAdopted != null && transitAdopted.size() > 0) {
                    for (e in transitAdopted.entries()) {
                        resp.transitAdopted.put(e.getKey(), e.getValue());
                    }
                }
                
                resp.numActive = numActive;
                
                adopterId = _adopterId;
                placeOfAdopter = _adopterId.home;
                isAdopted = true;
                migrating = false;
                live = null;
                transit = null;
                liveAdopted = null;
                transitAdopted = null;
                numActive = 0;
                excs = null;
                children = null;
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
                return placeOfAdopter;
            } finally {
                unlock();
            }
        }
        
        //waits until backup is adopted
        public def getNewMasterBlocking() {
            if (verbose>=1) debug(">>>> Backup(id="+id+").getNewMasterBlocking called, parentId="+parentId);
            if (parentId == UNASSIGNED){ /**AT_FINISH HACK**/
                if (verbose>=1) debug("<<<< Backup(id="+id+").getNewMasterBlocking returning, newMaster=" + Id(0n,0n));
                //forward at_finish requests to main finish, then ignore them.
                return Id(0n,0n);
            }
            Runtime.increaseParallelism();
            ilock.lock();
            while (!isAdopted || migrating) {
                ilock.unlock();
                System.threadSleep(0); // release the CPU to more productive pursuits
                ilock.lock();
            }
            ilock.unlock();
            Runtime.decreaseParallelism(1n);
            if (verbose>=1) debug("<<<< Backup(id="+id+").getNewMasterBlocking returning, newMaster=" + adopterId);
            return adopterId;
        }
           
        def liveAdopted() {
            if (liveAdopted == null) liveAdopted = new HashMap[Task,Int]();
            return liveAdopted;
        }

        def transit() {
            if (transit == null) transit = new HashMap[Edge,Int]();
            return transit;
        }

        def transitAdopted() {
            if (transitAdopted == null) transitAdopted = new HashMap[Edge,Int]();
            return transitAdopted;
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
            if (t == null)
                return;
            try {
                ilock.lock();
                addExceptionUnsafe(t);
            } finally {
                ilock.unlock();
            }
        }
        
        def addChild(child:Id) {
            if (verbose>=1) debug(">>>> Backup(id="+id+").addChild called (child=" + child + ")");
            try {
                ilock.lock();
                if (children == null) {
                    children = new HashSet[Id]();
                }
                children.add(child);
                if (verbose>=1) debug("<<<< Backup(id="+id+").addChild returning (child=" + child +")");
            } finally {
                ilock.unlock();
            }
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").inTransit called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = Edge(srcId, dstId, kind);
                if (!toAdopter) {
                    increment(transit(), e);
                    numActive++;
                } else {
                    increment(transitAdopted(), e);
                    numActive++;
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").inTransit returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def transitToLive(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").transitToLive called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = Edge(srcId, dstId, kind);
                val t = Task(dstId, kind);
                if (!toAdopter) {
                    increment(live,t);
                    decrement(transit(), e);
                } else {
                    increment(liveAdopted(), t);
                    decrement(transitAdopted(), e);
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").transitToLive returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, toAdopter:Boolean) {
            if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") called");
            try {
                ilock.lock();
                val e = Edge(srcId, dstId, kind);
                if (!toAdopter) {
                    decrement(transit(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        isReleased = true;
                        FinishReplicator.removeBackup(id);
                    }
                } else {
                    decrement(transitAdopted(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        isReleased = true;
                        FinishReplicator.removeBackup(id);
                    }
                }
            } finally {
                ilock.unlock();
            }
            if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") returning");
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, ex:CheckedThrowable, tag:String, toAdopter:Boolean) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").liveToCompleted(numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") called");
                if (ex != null)
                    addExceptionUnsafe(ex);
                val t = Task(dstId, kind);
                if (!toAdopter) {
                    assert live.keySet().contains(t) : here + " ["+Runtime.activity()+"] FATAL ERROR Backup(id="+id+").liveToCompleted(numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") live doesn't contain task@" + dstId;
                    decrement(live, t);
                    numActive--;
                    if (quiescent()) {
                        isReleased = true;
                        FinishReplicator.removeBackup(id);
                    }
                } else {
                    assert liveAdopted().keySet().contains(t) : here + " ["+Runtime.activity()+"] FATAL ERROR Backup(id="+id+").liveToCompleted(numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") liveAdopted doesn't contain task@" + dstId;
                    decrement(liveAdopted(), t);
                    numActive--;
                    if (quiescent()) {
                        isReleased = true;
                        FinishReplicator.removeBackup(id);
                    }
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").liveToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Backup(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!");
                assert false : "COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Backup(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        public def exec(xreq:FinishRequest) {
            val resp = new BackupResponse();
            val reqMaster = xreq.masterPlaceId;
            try {
                lock();
                if (migrating) {
                    resp.errMasterDied = true;
                    return resp;
                }
            } finally {
                unlock();
            }
            
            if (xreq instanceof AddChildRequestPes) {
                val req = xreq as AddChildRequestPes;
                val childId = req.childId;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=ADD_CHILD, childId="+childId+"] called");
                addChild(childId);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec returning [req=ADD_CHILD, childId="+childId+"]");
            } else if (xreq instanceof TransitRequestPes) {
                val req = xreq as TransitRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ",kind=" + kind + ",subDPE="+req.transitSubmitDPE+" ] called");
                if (req.transitSubmitDPE)
                    addDeadPlaceException(dstId);
                else
                    inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ",kind=" + kind + " ] returning");
            } else if (xreq instanceof LiveRequestPes) {
                val req = xreq as LiveRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId=" + dstId + ",kind=" + kind + " ] called");
                var msg:String = kind == FinishResilient.ASYNC ? "notifyActivityCreation" : "notifyShiftedActivityCreation";
                transitToLive(srcId, dstId, kind, msg , toAdopter);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId=" + dstId + ",kind=" + kind + " ] returning");
            } else if (xreq instanceof TermRequestPes) {
                val req = xreq as TermRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + " ] called");
                liveToCompleted(srcId, dstId, kind, ex, "notifyActivityTermination", toAdopter);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + " ] returning");
            } else if (xreq instanceof ExcpRequestPes) {
                val req = xreq as ExcpRequestPes;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                addException(ex);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (xreq instanceof TransitTermRequestPes) {
                val req = xreq as TransitTermRequestPes;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ex+" ] called");
                transitToCompleted(srcId, dstId, kind, ex, toAdopter);
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ex+" ] returning");
            }
            return resp;
        }
    }
    
    /*********************************************************************/
    /*******************   Failure Recovery Methods   ********************/
    /*********************************************************************/
    static def getChildAdoptionRequests(newDead:HashSet[Int],
            masters:HashSet[FinishMasterState]) {
        if (verbose>=1) debug(">>>> getChildAdoptionRequests(masters="+masters.size()+") called");
        val reqs_resps = new HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]();
        if (!masters.isEmpty()) {
            for (dead in newDead) {
                var backup:Int = FinishReplicator.getBackupPlace(dead);
                if (Place(backup).isDead())
                    backup = FinishReplicator.searchBackup(dead, backup);
                for (mx in masters) {
                    val m = mx as PessimisticMasterState; 
                    m.lock();
                    if (m.children != null) {
                        for (child in m.children) {
                            if (dead == child.home) {
                                var rreq:HashMap[ChildQueryId, ChildAdoptionResponse] = reqs_resps.getOrElse(backup, null);
                                if (rreq == null){
                                    rreq = new HashMap[ChildQueryId, ChildAdoptionResponse]();
                                    reqs_resps.put(backup, rreq);
                                }
                                rreq.put(ChildQueryId(m.id /*parent id*/, child), new ChildAdoptionResponse(child));
                                if (verbose>=1) debug("==== getChildAdoptionRequests masterId["+m.id+"] has dead childId["+child+"]");
                            }
                        }
                    } else {
                        if (verbose>=1) debug("==== getChildAdoptionRequests masterId["+m.id+"] has no children");
                    }
                    m.unlock();
                }
            }
        }
        if (verbose>=1) debug("<<<< getChildAdoptionRequests(masters="+masters.size()+") returning");
        return reqs_resps;
    }
    
    static def acquireChildrenBackupsRecursively(newDead:HashSet[Int], placeReqs:HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]) {
        if (verbose>=1) debug(">>>> acquireChildrenBackupsRecursively(size="+placeReqs.size()+") called");
        if (placeReqs.size() == 0) {
            if (verbose>=1) debug("<<<< acquireChildrenBackupsRecursively(size="+placeReqs.size()+") returning, zero size");
            return placeReqs;
        }
        
        var reqs_resps:HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]] = placeReqs;

        val fullResult = new HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]();
        var grandChildren:HashSet[ChildQueryId];
        
        do {
            acquireChildrenBackups(newDead, reqs_resps);
            grandChildren = new HashSet[ChildQueryId]();
            //check if orghan grand children exist
            for (placeEntry in reqs_resps.entries()) {
                //hashmap of children requests and their responses
                val childReqRes = placeEntry.getValue();
                for (e in childReqRes.entries()) {
                    val query = e.getKey();
                    val resp = e.getValue();
                    val parentId = query.id;
                    val children = resp.children;
                    for (child in children) {
                        grandChildren.add (new ChildQueryId(parentId, child));
                    }
                    resp.children.clear();
                    
                    
                    val pl = placeEntry.getKey();
                    var placeResult:HashMap[ChildQueryId, ChildAdoptionResponse] = fullResult.getOrElse(pl, null);
                    if (placeResult == null) {
                        placeResult = new HashMap[ChildQueryId, ChildAdoptionResponse]();
                        fullResult.put(pl,placeResult);
                    }
                    placeResult.put(query, resp);
                }
            }
            
            if (grandChildren.size() > 0) {
                reqs_resps = new HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]();
                for (childQuery in grandChildren) {
                    var backup:Int = FinishReplicator.getBackupPlace(childQuery.childId.home);
                    if (Place(backup).isDead())
                        backup = FinishReplicator.searchBackup(childQuery.childId.home, backup);
                    var rreq:HashMap[ChildQueryId, ChildAdoptionResponse] = reqs_resps.getOrElse(backup, null);
                    if (rreq == null){
                        rreq = new HashMap[ChildQueryId, ChildAdoptionResponse]();
                        reqs_resps.put(backup, rreq);
                    }
                    rreq.put(childQuery, new ChildAdoptionResponse(childQuery.childId));
                }
            }
        } while (grandChildren.size() > 0);
        
        return fullResult;
    }
    
    static def acquireChildrenBackups(newDead:HashSet[Int], reqs_resps:HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]) {
        if (verbose>=1) debug(">>>> acquireChildrenBackups(size="+reqs_resps.size()+") called");
        if (reqs_resps.size() == 0) {
            if (verbose>=1) debug("<<<< acquireChildrenBackups(size="+reqs_resps.size()+") returning, zero size");
            return;
        }
        val places = new Rail[Int](reqs_resps.size());
        val iter = reqs_resps.keySet().iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val pl = iter.next();
            places(i++) = pl;
        }
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val outputGr = GlobalRef[HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]](reqs_resps);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                if (verbose>=1) debug("==== acquireChildrenBackups  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    if (verbose>=1) debug("==== acquireChildrenBackups  moving from " + here + " to " + Place(p) + "  failed - place is dead");
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    val preq = reqs_resps.getOrThrow(p);
                    at (Place(p)) @Immediate("acquire_child_backup_request") async {
                        if (verbose>=1) debug("==== acquireChildrenBackups  reached from " + gr.home + " to " + here);
                        val requests = preq;
                        if (requests.size() > 0) {
                            for (e in requests.entries()) {
                                val query = e.getKey();
                                val childId = query.childId; //we don't need to deny the backup, no harm if it gets created later on
                                val acquireResp = e.getValue();
                                val bFin = FinishReplicator.findBackup(childId);
                                if (bFin != null) {//null means bFin was released
                                	if (verbose>=1) debug("==== acquireChildrenBackups  childId["+childId+"] found");
                                	(bFin as PessimisticBackupState).acquire(newDead, query.id, acquireResp); // get the counts and the nested children if dead 
                                	requests.put(e.getKey(), acquireResp);
                                }
                                else {
                                	if (verbose>=1) debug("==== acquireChildrenBackups  childId["+childId+"] is null (was released)");
                                }
                            }
                        }
                        
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== acquireChildrenBackups  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("acquire_child_backup_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,HashMap[ChildQueryId, ChildAdoptionResponse]]]{self.home == here})().getOrThrow(me);
                            
                            for (x in requests.entries()) {
                                output.put(x.getKey(), x.getValue());
                            }
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception(here + " activity["+Runtime.activity()+"] FATAL ERROR in acquireChildrenBackups: another place failed during recovery ...");

        //add grandchildren children to master, and try again
    }
    
    //FIXME: should nominate another backup if the nominated one is dead
    static def createOrSyncBackups(newDead:HashSet[Int], masters:HashSet[FinishMasterState]) {
        if (verbose>=1) debug(">>>> createOrSyncBackups(size="+masters.size()+") called");
        val places = new Rail[Int](masters.size());
        val newBackups = new HashMap[Id,Int](); //id to backup, -1n means 
        val iter = masters.iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val m = iter.next() as PessimisticMasterState;
            val newB = FinishReplicator.nominateBackupPlaceIfDead(m.id.home);
            places(i) = newB;
            //don't update m.backupPlaceId until the recovered backups are created. Otherwise, pleases that are calling getNewBackup may reach the backup before it is created
            newBackups.put(m.id, newB); 
            i++;
            
            if (verbose>=3) {
                debug(">>>> sync from master to backup ("+m.id+") newB["+newB+"] m.backupPlaceId["+m.backupPlaceId+"] m.backupChanged["+m.backupChanged+"]");
                m.dump();
            }
        }
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val placeOfMaster = here.id as Int;
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (mx in masters) {
                val m = mx as PessimisticMasterState;
                val backup = Place(newBackups.getOrThrow(m.id));
                val id = m.id;
                val parentId = m.parentId;
                val numActive = m.numActive;
                val live = m.live;
                val transit = m.transit;
                val liveAdopted = m.liveAdopted;
                val transitAdopted = m.transitAdopted;
                val children = m.children;
                val excs = m.excs;
                
                if (verbose>=1) debug("==== createOrSyncBackups id="+id+" going to backup["+backup+"]");
                
                if (backup.isDead()) {
                    if (verbose>=1) debug("==== createOrSyncBackups id="+id+" going to backup["+backup+"] FAILED, backup is dead");
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (backup) @Immediate("create_or_sync_backup") async {
                        FinishReplicator.createPessimisticBackupOrSync(id, parentId, numActive, live,
                                transit, liveAdopted, transitAdopted, children, excs);
                        val me = here.id as Int;
                        at (gr) @Immediate("create_or_sync_backup_response") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception(here + " activity["+Runtime.activity()+"] FATAL ERRORin createOrSyncBackups: another place failed during recovery ...");
        
        for (mx in masters) {
            val m = mx as PessimisticMasterState;
            m.lock();
            val newB2 = newBackups.getOrThrow(m.id);
            if (newB2 != m.backupPlaceId) {
                m.backupPlaceId = newB2;
                m.backupChanged = true;
            }
            m.migrating = false;
            m.unlock();
        }
        if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning");
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (Runtime.activity() == null) {
            if (verbose>=1) debug(">>>> notifyPlaceDeath returning, IGNORED REQUEST FROM IMMEDIATE THREAD");
            return; 
        }
        val newDead = FinishReplicator.getNewDeadPlaces();
        if (newDead == null || newDead.size() == 0) //occurs at program termination
            return;
        val hereId = here.id as Int;
        
        val masters = FinishReplicator.getImpactedMasters(newDead); //any master who contacted the dead place or whose backup was lost or his child was lost
        val backups = FinishReplicator.getImpactedBackups(newDead); //any backup who lost its master.
        val myBackupDied = newDead.contains(FinishReplicator.getBackupPlace(hereId));
        if (myBackupDied)
            debug(">>>> notifyPlaceDeath my backup died");
        
        //prevent updates on backups since they are based on decisions made by a dead master
        for (bx in backups) {
            val b = bx as PessimisticBackupState;
            b.lock();
            if (!b.isAdopted)
                b.migrating = true; // their adopters will set this flag to false
            b.unlock();
        }
        
        val reqs_resps = getChildAdoptionRequests(newDead, masters); // combine all requests targetted to a specific place
        
        for (m in masters) {
            val master = m as PessimisticMasterState;
            master.migrating = true; //to avoid processing forwarded requests for an adopted child, before we adjust our counters
        }
        
        val resps = acquireChildrenBackupsRecursively(newDead, reqs_resps); //obtain the counts and disable the backups

        val map = new HashMap[Id, HashSet[ChildAdoptionResponse]]();
        for (placeEntry in resps.entries()) {
            val placeMap = placeEntry.getValue();
            for (e in placeMap.entries()) {
                val parentId = e.getKey().id;
                var set:HashSet[ChildAdoptionResponse] = map.getOrElse(parentId, null);
                if (set == null) {
                    set = new HashSet[ChildAdoptionResponse]();
                    map.put(parentId, set);
                }
                set.add(e.getValue());
            }
        }
        
        
        //update counts and check if quiecent reached
        for (m in masters) {
            val master = m as PessimisticMasterState;
            
            master.lock();
            //convert to dead
            master.adoptChildren(map.get(master.id));
            
            master.convertDeadActivities(newDead);
            
            master.unlock();
        }
        
        if (masters.size() > 0)
            createOrSyncBackups(newDead, masters);
        else {
            if (myBackupDied) {
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
        }
        if (verbose>=1) debug("==== handling non-blocking pending requests ====");
        FinishReplicator.submitDeadBackupPendingRequests(newDead);
        FinishReplicator.submitDeadMasterPendingRequests(newDead);
        if (verbose>=1) debug(">>>> notifyPlaceDeath returned");
    }
}

class ChildAdoptionResponse(childId:FinishResilient.Id) {
    val children = new HashSet[FinishResilient.Id]();
    val live = new HashMap[FinishResilient.Task,Int]();
    val transit = new HashMap[FinishResilient.Edge,Int]();
    val liveAdopted = new HashMap[FinishResilient.Task,Int]();
    val transitAdopted = new HashMap[FinishResilient.Edge,Int]();
    var numActive:Long = 0;
    var found:Boolean = false;
    public def toString() = "childId="+childId+", found="+found;
}