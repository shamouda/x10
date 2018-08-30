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
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.util.concurrent.Condition;
import x10.xrx.freq.FinishRequest;
import x10.xrx.freq.ExcpRequestOpt;
import x10.xrx.freq.TermMulRequestOpt;
import x10.xrx.freq.TermRequestOpt;
import x10.xrx.freq.RemoveGhostChildRequestOpt;
import x10.xrx.freq.TransitRequestOpt;
import x10.xrx.freq.MergeSubTxRequestOpt;
import x10.util.Set;

class FinishNonResilientCustom extends FinishState implements CustomSerialization {
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
    def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) { me.notifyTxActivityTermination(srcPlace, readOnly, t); }
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def waitForFinish():void { me.waitForFinish(); }
    def spawnRemoteActivity(place:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { me.spawnRemoteActivity(place, body, prof); }
    
    def notifyActivityTermination(srcPlace:Place, t:CheckedThrowable):void {
        if (me instanceof NonResilientCustomRemoteState) {
            if (t == null) me.notifyActivityTermination(srcPlace);
            else me.notifyActivityTermination(srcPlace, t);
        }
        else
            super.notifyActivityTermination(srcPlace, t);
    }
    
    def notifyActivityCreatedAndTerminated(srcPlace:Place, t:CheckedThrowable):void { 
        if (me instanceof NonResilientCustomRemoteState) {
            if (t == null) me.notifyActivityCreatedAndTerminated(srcPlace);
            else me.notifyActivityCreatedAndTerminated(srcPlace, t);
        }
        else
            super.notifyActivityCreatedAndTerminated(srcPlace, t);
    }
    
    def notifyShiftedActivityCompletion(srcPlace:Place, t:CheckedThrowable):void {
        if (me instanceof NonResilientCustomRemoteState) {
            if (t == null) me.notifyShiftedActivityCompletion(srcPlace);
            else me.notifyShiftedActivityCompletion(srcPlace, t);
        }
        else
            super.notifyShiftedActivityCompletion(srcPlace, t);
    }
    
    //create root finish
    public def this (parent:FinishState) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        me = new NonResilientCustomMasterState(id, parent);
        FinishReplicator.addMaster(id, me as FinishMasterState);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = NonResilientCustomRemoteState.getOrCreateRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        return new FinishNonResilientCustom(parent);
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    def registerFinishTx(tx:Tx, rootTx:Boolean):void {
        me.registerFinishTx(tx, rootTx);
    }
    
    //REMOTE
    public static final class NonResilientCustomRemoteState extends FinishState implements x10.io.Unserializable {
        val id:Id; //parent root finish
        private static val remoteLock = new Lock();
        private static val remotes = new HashMap[Id, OptimisticRemoteState]() ; //a cache for remote finish objects

        var exceptions:GrowableRail[CheckedThrowable];
        var lock:Lock = @Embed new Lock();
        var count:Int = 0n;
        var remoteActivities:HashMap[Long,Int]; // key is place id, value is count for that place.
        val local = new AtomicInteger(0n); // local count
        
        private var tx:Boolean = false;
        private var txReadOnly:Boolean = true;
        
        public def toString() {
            return "OptimisticRemote(id="+id+", localCount="+lc.get()+")";
        }
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        public static def deleteObjects(gcReqs:Set[Id]) {
            if (gcReqs == null) {
                if (verbose>=1) debug(">>>> deleteObjects gcReqs = NULL");
                return;
            }
            try {
                remoteLock.lock();
                for (idx in gcReqs) {
                    if (verbose>=1) debug(">>>> deleting remote object(id="+idx+")");
                    remotes.delete(idx);
                }
            } finally {
                remoteLock.unlock();
            }
        }
        
        public static def deleteObject(id:Id) {
            try {
                remoteLock.lock();
                if (verbose>=1) debug(">>>> deleting remote object(id="+id+")");
                remotes.delete(id);
            } finally {
                remoteLock.unlock();
            }
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Remote dump:\n");
            s.add("           here:" + here.id); s.add('\n');
            s.add("             id:" + id); s.add('\n');            
            s.add("     localCount:"); s.add(lc); s.add('\n');
            debug(s.toString());
        }
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:OptimisticRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
                    remoteState = new OptimisticRemoteState(id);
                    remotes.put(id, remoteState);
                    if (verbose>=1) debug("<<<< getOrCreateRemote(id="+id+") added a new remote ...");
                }
                return remoteState;
            } finally {
                remoteLock.unlock();
            }
        }
        def notifySubActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, ASYNC);
        }
        
        def notifyShiftedActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, AT);
        }
        
        def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {//done
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
            lock.lock();
            if (place.id == here.id) {
                count++;
                lock.unlock();
                return;
            }
            ensureRemoteActivities();
            val old = remoteActivities.getOrElse(place.id, 0n);
            remoteActivities.put(place.id, old+1n);
            lock.unlock();
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            val parentId = UNASSIGNED;
            val id = this.id; //don't serialize this
            
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            

            //FILL
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {//done
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            local.getAndIncrement();
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning");
            return true;
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean { //done
            return notifyActivityCreation(srcPlace, null);
        }
        
        def notifyRemoteContinuationCreated():void { /*noop for remote finish*/ }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { //done
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            notifyActivityCreation(srcPlace, null);
            pushException(t);
            notifyActivityTermination(srcPlace);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, null);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, t);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int, t:CheckedThrowable) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            if (t != null) pushException(t);
            notifyActivityCreation(srcPlace, null);
            notifyActivityTermination(srcPlace);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            lock.lock();
            if (null == exceptions) exceptions = new GrowableRail[CheckedThrowable]();
            exceptions.add(t);
            lock.unlock();
            if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, null, false, false);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, null, false, false);
        }
        
        def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) {
            notifyActivityTermination(srcPlace, ASYNC, t, true, readOnly);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, t:CheckedThrowable, isTx:Boolean, readOnly:Boolean):void {
            val id = this.id;
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") called ");
            lock.lock();
            if (t != null) {
                if (null == exceptions) exceptions = new GrowableRail[CheckedThrowable]();
                exceptions.add(t);
            }
            tx = tx | isTx;
            txReadOnly = txReadOnly & readOnly;
            count--;
            if (local.decrementAndGet() > 0) {
                lock.unlock();
                return;
            }
            val cTx = tx;
            val cTxReadyOnly = txReadOnly;
            
            val subTxMem = Runtime.activity() == null? null : Runtime.activity().subMembers;
            val subTxRO = Runtime.activity() == null? true : Runtime.activity().subReadOnly;
            val excs = exceptions == null || exceptions.isEmpty() ? null : exceptions.toRail();
            exceptions = null;
            val home = id.home as Long;
            if (remoteActivities != null && remoteActivities.size() != 0) {
                remoteActivities.put(here.id, count); // put our own count into the table
                // pre-serialize the hashmap here
                val serializer = new x10.io.Serializer();
                serializer.writeAny(remoteActivities);
                val serializedTable:Rail[Byte] = serializer.toRail();
                remoteActivities.clear();
                count = 0n;
                lock.unlock();
                if (null != excs) {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_1") async {
                        FinishReplicator.findMaster(id).notify(serializedTable, excs, cTx, cTxReadyOnly, subTxMem, subTxRO);
                    }
                } else {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_2") async {
                        FinishReplicator.findMaster(id).notify(serializedTable, cTx, cTxReadyOnly, subTxMem, subTxRO);
                    }
                }
            } else {
                val message = new Pair[Long, Int](here.id, count);
                count = 0n;
                lock.unlock();
                if (null != excs) {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_3") async {
                        (FinishReplicator.findMaster(id) as NonResilientCustomMasterState).notify(message, excs, cTx, cTxReadyOnly, subTxMem, subTxRO);
                    }
                } else {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_4") async {
                        (FinishReplicator.findMaster(id) as NonResilientCustomMasterState).notify(message, cTx, cTxReadyOnly, subTxMem, subTxRO);
                    }
                }
            }
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") returning");
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
        
        def notifyActivityTermination(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, ASYNC, t, false, false);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, AT, t, false, false);
        }
    }
    
    //ROOT
    public static final class NonResilientCustomMasterState extends FinishMasterState implements x10.io.Unserializable, Releasable {
    
        //finish variables
        val parent:FinishState; //the direct parent finish object (used in globalInit for recursive initializing)
        val latch:SimpleLatch; //latch for blocking the finish activity, and a also used as the instance lock
        var lc:Int;
        
        //resilient-finish variables
        val id:Id;
        val parentId:Id; //the direct parent of this finish (could be a remote finish) 
        var numActive:Long;
        var excs:GrowableRail[CheckedThrowable];
        val sent:HashMap[Edge,Int]; //increasing counts    
        val transit:HashMap[Edge,Int];
        var ghostChildren:HashSet[Id] = null;  //lazily allocated in addException
        var ghostsNotifiedEarly:HashSet[Id] = null; //used to save racing (remove ghost requests when the master is yet to calculate the ghostChildren)
        var isRecovered:Boolean = false; //set to true when backup recreates a master
        
        var migrating:Boolean = false; //used to stop updates to the object during recovery
        var backupPlaceId:Int = FinishReplicator.updateBackupPlaceIfDead(here.id as Int);  //may be updated in notifyPlaceDeath
        var backupChanged:Boolean = false;
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;

        private var txFlag:Boolean = false;
        private var txReadOnlyFlag:Boolean = true;
    
        private var txStarted:Boolean = false; //if true - this.notifyPlaceDeath() will call tx.notifyPlaceDeath();
    
        public def registerFinishTx(old:Tx, rootTx:Boolean):void {
            latch.lock();
            this.isRootTx = rootTx;
            if (rootTx)
                this.tx = old;
            else
                this.tx = Tx.clone(old);
            this.tx.initialize(id, backupPlaceId);
            latch.unlock();
        }
    
        //initialize from backup values
        def this(_id:Id, _parentId:Id, _numActive:Long, _sent:HashMap[Edge,Int],
        		_transit:HashMap[Edge,Int], ghostChildren:HashSet[Id],
                _excs:GrowableRail[CheckedThrowable], _backupPlaceId:Int, _tx:Tx, _rootTx:Boolean,
                _tx_mem:Set[Int], _tx_excs:GrowableRail[CheckedThrowable], _tx_ro:Boolean) {
            this.id = _id;
            this.parentId = _parentId;
            this.numActive = _numActive;
            this.sent = _sent;
            this.transit = _transit;
            this.ghostChildren = ghostChildren;
            this.backupPlaceId = _backupPlaceId;
            this.excs = _excs;
            this.strictFinish = false;
            this.parent = null;
            this.latch = new SimpleLatch();
            this.isGlobal = true;
            this.backupChanged = true;
            this.lc = 0n;
            this.isRecovered = true; // will have to call notifyParent
            if (_tx != null) {
                this.tx = _tx;
                this.isRootTx = _rootTx;
                this.tx.initializeNewMaster(_id, _tx_mem, _tx_excs, _tx_ro, _backupPlaceId);
            }
            if (verbose>=1) debug("<<<< recreated master(id="+id+") using backup values");
        }
        
        def this(id:Id, parent:FinishState) {
            this.latch = new SimpleLatch();
            this.id = id;
            this.numActive = 1;
            this.parent = parent;
            this.lc = 1n;
            sent = new HashMap[Edge,Int]();
            sent.put(Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            transit = new HashMap[Edge,Int]();
            transit.put(Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            if (parent instanceof FinishNonResilientCustom) {
                parentId = (parent as FinishNonResilientCustom).id;
            }
            else {
                parentId = UNASSIGNED;
            }
        }
        
        public def toString() {
            return "OptimisticRoot(id="+id+", parentId="+parentId+", localCount="+lc_get()+")";
        }
        
        public def getId() = id;
        public def getBackupId() = backupPlaceId;
        
        def lc_get() {
            var x:Int = 0n;
            latch.lock();
            x = lc;
            latch.unlock();
            return x;
        }
        
        def lc_incrementAndGet() {
            var x:Int = 0n;
            latch.lock();
            x = ++lc;
            latch.unlock();
            return x;
        }
        
        def lc_decrementAndGet() {
            var x:Int = 0n;
            latch.lock();
            x = --lc;
            latch.unlock();
            return x;
        }
        
        def globalInit() {
            latch.lock();
            if (!isGlobal) {
                strictFinish = true;
                if (verbose>=1) debug(">>>> doing globalInit for id="+id);
                
                if (parent instanceof FinishNonResilientCustom) {
                    val frParent = parent as FinishNonResilientCustom;
                    if (frParent.me instanceof NonResilientCustomMasterState) {
                        val par = (frParent.me as NonResilientCustomMasterState);
                        if (!par.isGlobal) par.globalInit();
                    }
                }
                
                createBackup(backupPlaceId);
                isGlobal = true;
                if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            latch.unlock();
        }
        
        private def createBackup(backupPlaceId:Int) {
            //TODO: redo if backup dies
            if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
            val backup = Place(backupPlaceId);
            if (backup.isDead()) {
                 if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
                 return false;
            }
            val myId = id; //don't copy this
            val home = here;
            val myParentId = parentId;
            val myTx = tx;
            val myRootTx = isRootTx;
            val rCond = ResilientCondition.make(backup);
            val closure = (gr:GlobalRef[Condition]) => {
                at (backup) @Immediate("backup_create") async {
                    val bFin = FinishReplicator.findBackupOrCreate(myId, myParentId, myTx, myRootTx);
                    at (gr) @Immediate("backup_create_response") async {
                        gr().release();
                    }
                }; 
            };
            rCond.run(closure, true);
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
            s.add("     localCount:"); s.add(lc); s.add('\n');
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
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " called");
            try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                increment(sent, e);
                increment(transit, e);
                numActive++;
                if (verbose>=3) debug("==== Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
                if (verbose>=3) dump();
                if (verbose>=1) debug("<<<< Master(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                 latch.unlock();
            }
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, isTx:Boolean,
                isTxRO:Boolean, tag:String, resp:MasterResponse) {
            var callRel:Boolean = false;
            try {
                latch.lock();
                
                if (isTx && tx != null) {
                    tx.addMember(dstId as Int, isTxRO, 0n);
                }
                
                if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
                val e = Edge(srcId, dstId, kind);
                decrement(transit, e);
                //don't decrement 'sent' 
                
                numActive--;
                assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
                if (t != null) addExceptionUnsafe(t);
                if (quiescent()) {
                    callRel = true; //call tryRelease outside of the latch scope
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
            if (callRel) {
                tryRelease();
            }
        }
        
        def removeGhostChild(childId:Id, t:CheckedThrowable, subMembers:Set[Int], subReadOnly:Boolean, resp:MasterResponse) {
            var callRel:Boolean = false;
            try {
                latch.lock();
                if (tx != null && subMembers != null){
                    tx.addSubMembers(subMembers, subReadOnly, 4444n);
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").removeGhostChild childId=" + childId + ", t=" + t + " called");
                if (ghostChildren == null || !ghostChildren.contains(childId)) {
                    if (ghostsNotifiedEarly == null)
                        ghostsNotifiedEarly = new HashSet[Id]();
                    ghostsNotifiedEarly.add(childId);
                    if (verbose>=1) debug("<<<< Master(id="+id+").removeGhostChild returning - child buffered (childId="+childId+", ex=" + t + ") ");
                } else {
                    ghostChildren.remove(childId);
                    numActive--;
                    assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        callRel = true;
                    }
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").removeGhostChild childId=" + childId + ", t=" + t + " returning");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
            if (callRel) {
                tryRelease();
            }
        }
        
        def addSubTxMembers(subMembers:Set[Int], subReadOnly:Boolean, resp:MasterResponse) {
            try {
                latch.lock();
                if (tx != null) {
                    tx.addSubMembers(subMembers, subReadOnly, 5555n);
                }
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }

        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, ex:CheckedThrowable , tag:String, resp:MasterResponse) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " called");
            
            if (ex != null) addExceptionUnsafe(ex);
            
            val e = Edge(srcId, dstId, kind);
            deduct(transit, e, cnt);
            //don't decrement 'sent' 
            
            numActive-=cnt;
            resp.backupPlaceId = backupPlaceId;
            resp.backupChanged = backupChanged;
            if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " set backup="+resp.backupPlaceId+ " set backupChanged="+resp.backupChanged);
            if (quiescent()) {
                return true;
            }
            return false;
        }
        
        def quiescent():Boolean {
            val quiet = numActive == 0;
            if (verbose>=3) dump();
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Master(id="+id+").quiescent returning " + quiet + " tx="+tx);
            return quiet;
        }
        
        def tryRelease() {
            if (verbose>=1) debug(">>>> Master.tryRelease(id="+id+").tryRelease called tx="+tx);
            if (tx == null || tx.isEmpty() || !isRootTx) {
                releaseLatch();
                notifyParent();
                FinishReplicator.removeMaster(id);
            } else {
                if (verbose>=1) debug("==== Master.tryRelease(id="+id+").tryRelease calling finalizeWithBackup ");
                txStarted = true;
                var abort:Boolean = false;
                if (excs != null && excs.size() > 0) {
                    abort = true;
                }
                tx.finalizeWithBackup(this, abort, backupPlaceId, isRecovered); //this also performs gc
            }
            if (verbose>=1) debug("<<<< Master.tryRelease(id="+id+").tryRelease returning tx="+tx);
        }
        
        public def releaseFinish(excs:GrowableRail[CheckedThrowable]) {
            if (verbose>=1) debug(">>>> Master(id="+id+").releaseFinish called");
            if (tx != null && !(tx instanceof TxResilientNoSlaves) ) {
                val myId = id;
                at (Place(backupPlaceId)) @Immediate("optdist_release_backup") async {
                    if (verbose>=1) debug("==== releaseFinish(id="+myId+") reached backup");
                    FinishReplicator.removeBackupOrMarkToDelete(myId);
                }
            }
            if (excs != null) {
                for (t in excs.toRail())
                    addExceptionUnsafe(t);//unsafe: only one thread reaches this method
            }
            releaseLatch();
            notifyParent();
            FinishReplicator.removeMaster(id);
            if (verbose>=1) debug("<<<< Master(id="+id+").releaseFinish returning");
        }
        
        def releaseLatch() {
            if (isRecovered) {
                if (verbose>=1) debug("releaseLatch(id="+id+") called on adopted finish; not releasing latch");
            } else {
                latch.release();
            }
            if (verbose>=2) debug("Master(id="+id+").releaseLatch returning");
        }
        
        def notifyParent() {
            if (!isRecovered || parentId == FinishResilient.UNASSIGNED) {
                if (verbose>=1) debug("<<< Master(id="+id+").notifyParent returning, not adopted or parent["+parentId+"] does not exist");
                return;
            }
            var subMembers:Set[Int] = null;
            var subReadOnly:Boolean = true;
            if (tx != null && !isRootTx) {
                subMembers = tx.getMembers();
                subReadOnly = tx.isReadOnly();
            }
            val mem = subMembers;
            val ro = subReadOnly;
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Master(id="+id+").notifyParent(parentId="+parentId+") called");
                val req = FinishRequest.makeOptRemoveGhostChildRequest(parentId, id, mem, ro);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Master(id="+id+").notifyParent(parentId="+parentId+") returning");
            });
        }
        
        public def exec(xreq:FinishRequest) {
            val id = xreq.id;
            val resp = new MasterResponse();
            try {
                lock();
                if (migrating) {
                    resp.errMasterMigrating = true;
                    return resp;
                }
            } finally {
                unlock();
            }
            if (xreq instanceof TransitRequestOpt) {
                val req = xreq as TransitRequestOpt;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + " ] called");
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                } else if (Place(dstId).isDead()) {
                    if (kind == FinishResilient.ASYNC) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                        addDeadPlaceException(dstId, resp);
                        resp.transitSubmitDPE = true;
                    } else {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                    }
                } else {
                    inTransit(srcId, dstId, kind, "notifySubActivitySpawn", resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit="+resp.submit+" ] returning");
            } else if (xreq instanceof TermRequestOpt) {
                val req = xreq as TermRequestOpt;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                val ex = req.ex;
                val isTx = req.isTx;
                val isTxRO = req.isTxRO;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] called");
                if (Place(dstId).isDead()) {
                    // drop termination messages from a dead place; only simulated termination signals are accepted
                    if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
                } else {
                    transitToCompleted(srcId, dstId, kind, ex, isTx, isTxRO, "notifyActivityTermination", resp);
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] returning");
            } else if (xreq instanceof ExcpRequestOpt) {
                val req = xreq as ExcpRequestOpt;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                addException(ex, resp);
                resp.submit = true;
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (xreq instanceof TermMulRequestOpt) {
                val req = xreq as TermMulRequestOpt;
                val map = req.map;
                val dstId = req.dstId;
                val ex = req.ex;
                val isTx = req.isTx;
                val isTxRO = req.isTxRO;
                
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+",isTx="+isTx+",isTxRO="+isTxRO+"] called");
                if (Place(dstId).isDead()) {
                    // drop termination messages from a dead place; only simulated termination signals are accepted
                    if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId);
                } else {
                    var callRel:Boolean = false;
                    try {
                        latch.lock();
                        if (isTx && tx != null) {
                            tx.addMember(dstId as Int, isTxRO, 1n);
                        }
                        resp.backupPlaceId = -1n;
                        for (e in map.entries()) {
                            val srcId = e.getKey().place;
                            val kind = e.getKey().kind;
                            val cnt = e.getValue();
                            if (transitToCompletedUnsafe(srcId, dstId, kind, cnt, ex, "notifyActivityTermination", resp))
                                callRel = true;
                        }
                    } finally {
                        latch.unlock();
                    }
                    if (callRel) {
                        tryRelease(); //call tryRelease outside of the latch scope
                    }
                    resp.submit = true;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+",isTx="+isTx+",isTxRO="+isTxRO+"] returning, backup="+resp.backupPlaceId);
            } else if (xreq instanceof RemoveGhostChildRequestOpt) {
                val req = xreq as RemoveGhostChildRequestOpt;
                val childId = req.childId;
                val mem = req.subMembers;
                val ro = req.subReadOnly;
                val dpe = new DeadPlaceException(Place(childId.home));
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=REM_GHOST, child="+childId+", ex="+dpe+" ] called");
                removeGhostChild(childId, dpe, mem, ro, resp);
                resp.submit = true;
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=REM_GHOST, child="+childId+", ex="+dpe+" ] returning");
            } else if (xreq instanceof MergeSubTxRequestOpt) {
                val req = xreq as MergeSubTxRequestOpt;
                val mem = req.subMembers;
                val ro = req.subReadOnly;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=MERGE_SUB_TX] called");
                addSubTxMembers(mem, ro, resp);
                resp.submit = true;
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=MERGE_SUB_TX] returning");
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
                lc_incrementAndGet();
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                
                isGlobal = true;
                //globalize parent if not global - cannot postpone this till sendPendingAct because it is called in an immediate thread and globalInit is blocking
                if (parent instanceof FinishNonResilientCustom) {
                    val frParent = parent as FinishNonResilientCustom;
                    if (frParent.me instanceof NonResilientCustomMasterState) {
                        val par = (frParent.me as NonResilientCustomMasterState);
                        if (!par.isGlobal) par.globalInit();
                    }
                }
                
                val req = FinishRequest.makeOptTransitRequest(id, parentId, srcId, dstId, kind, tx, isRootTx);
                FinishReplicator.exec(req, this);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            val id = this.id;
            
            isGlobal = true;
            //globalize parent if not global - cannot postpone this till sendPendingAct because it is called in an immediate thread and globalInit is blocking
            if (parent instanceof FinishNonResilientCustom) {
                val frParent = parent as FinishNonResilientCustom;
                if (frParent.me instanceof NonResilientCustomMasterState) {
                    val par = (frParent.me as NonResilientCustomMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            
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
            val req = FinishRequest.makeOptTransitRequest(id, parentId, srcId, dstId, kind, tx, isRootTx);
            val num = req.num;
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
            };
            val count = lc_incrementAndGet(); // synthetic activity to keep finish locally live during async replication
            FinishReplicator.asyncExec(req, this, preSendAction, postSendAction);
            if (verbose>=1) debug("<<<< Root(id="+id+").spawnRemoteActivity(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, incremented localCount to " + count);
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +") returning");
            return true;
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
            
            isGlobal = true;
            //globalize parent if not global - cannot postpone this till sendPendingAct because it is called in an immediate thread and globalInit is blocking
            if (parent instanceof FinishNonResilientCustom) {
                val frParent = parent as FinishNonResilientCustom;
                if (frParent.me instanceof NonResilientCustomMasterState) {
                    val par = (frParent.me as NonResilientCustomMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            val req = FinishRequest.makeOptTermRequest(id, parentId, srcId, dstId, kind, t, null, false, false, false);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") called");
            //no need to call notifyActivityCreation() since it does NOOP in Root finish
            notifyActivityTermination(srcPlace, ASYNC, false, false);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            assert (Runtime.activity() != null) : here + " >>>> Root(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            
            isGlobal = true;
            //globalize parent if not global - cannot postpone this till sendPendingAct because it is called in an immediate thread and globalInit is blocking
            if (parent instanceof FinishNonResilientCustom) {
                val frParent = parent as FinishNonResilientCustom;
                if (frParent.me instanceof NonResilientCustomMasterState) {
                    val par = (frParent.me as NonResilientCustomMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            
            if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeOptExcpRequest(id, parentId, t);
            FinishReplicator.exec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, false, false);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, false, false);
        }
        
        def setTxFlags(isTx:Boolean, isTxRO:Boolean) {
            if (verbose>=1) debug(">>>> Root(id="+id+").setTxFlags("+isTx+","+isTxRO+")");
            if (tx == null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").setTxFlags("+isTx+","+isTxRO+") returning, tx is null");
                return;
            }
            
            try {
                latch.lock();
                tx.addMember(here.id as Int, isTxRO, 2n);
                txFlag = txFlag | isTx;
                txReadOnlyFlag = txReadOnlyFlag & isTxRO;
                if (verbose>=1) debug("<<<< Root(id="+id+").setTxFlags("+isTx+","+isTxRO+") returning ");
            } catch (e:Exception) {
                if (verbose>=1) debug("<<<< Root(id="+id+").setTxFlags("+isTx+","+isTxRO+") returning with exception " + e.getMessage());
                throw e;
            } finally {
                latch.unlock();
            }
        }
        
        def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) {
            notifyActivityTermination(srcPlace, ASYNC, true, readOnly);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, isTx:Boolean, isTxRO:Boolean):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (isTx)
                setTxFlags(isTx, isTxRO);
            val count = lc_decrementAndGet();
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind
                    +",isTx="+isTx+",isTxRO="+isTxRO+") called, decremented localCount to "+count);
            if (count > 0) {
                return;
            }
            if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                tryReleaseLocal();
                return;
            }
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+",isRootTx="+isRootTx+", txFlag="+txFlag+", txReadOnlyFlag="+txReadOnlyFlag+") called");
            val req = FinishRequest.makeOptTermRequest(id, parentId, srcId, dstId, kind, null, tx, isRootTx, txFlag, txReadOnlyFlag);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }
        
        
        private def tryReleaseLocal() {
            if (verbose>=1) debug(">>>> Root(id="+id+").tryReleaseLocal called ");
            if (tx == null || tx.isEmpty() || !isRootTx) {
                latch.release();
                FinishReplicator.removeMaster(id);
            } else {
                tx.addMember(here.id as Int, txReadOnlyFlag, 3n);
                var abort:Boolean = false;
                if (excs != null && excs.size() > 0) {
                    abort = true;
                    if (verbose>=1) {
                        var s:String = "";
                        for (var i:Long = 0; i < excs.size(); i++) {
                            s += excs(i) + ", ";
                        }
                        debug("==== Master.tryRelease(id="+id+").tryReleaseLocal finalizeLocal abort because["+s+"]");
                    }
                }
                tx.finalizeLocal(this, abort);
            }
            if (verbose>=1) debug("<<<< Root(id="+id+").tryReleaseLocal returning ");
        }
        
        def waitForFinish():void {
            if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called, lc = " + lc );

            // terminate myself
            if (Runtime.activity().tx)
                notifyActivityTermination(here, ASYNC, true, Runtime.activity().txReadOnly);
            else
                notifyActivityTermination(here, ASYNC, false, false);
            
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

            if (isGlobal)
                notifyRootTx();
            else {
              //local finish -> for cases activities other than the finish main activity have created Tx work 
                if (tx != null && !tx.isEmpty() && !isRootTx) {
                    //notify parent of transaction status
                    if (parent instanceof FinishNonResilientCustom) {
                        val frParent = parent as FinishNonResilientCustom;
                        if (frParent.me instanceof NonResilientCustomMasterState) {
                            if (verbose>=1) debug("local finish with id="+id+" set Tx flag to root parent");
                            (frParent.me as NonResilientCustomMasterState).tx.addMember(here.id as Int, tx.isReadOnly(), 897n);
                        } else {
                            if (verbose>=1) debug("local finish with id="+id+" set Tx flag to remote parent");
                            (frParent.me as NonResilientCustomRemoteState).setTxFlags(true, tx.isReadOnly());
                        }
                    }
                }
            }
            // no more messages will come back to this finish state 
            //if (REMOTE_GC) gc();   Backup is in charge of GC because the remote object may be checked when the backup recovers the master
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").waitForFinish returning with exceptions, size=" + excs.size() );
                throw new MultipleExceptions(excs);
            }
        }
        
        def notifyRootTx() {
            if (tx != null && !tx.isEmpty() && !isRootTx) {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyRootTx called");
                val req = FinishRequest.makeOptMergeSubTxRequest(parentId, tx.getMembers(), tx.isReadOnly());
                val myId = id;
                val myBackupId = backupPlaceId;
                FinishReplicator.exec(req, this);
                at (Place(myBackupId)) @Immediate("optdist_release_backup2") async {
                    if (verbose>=1) debug("==== releaseFinish(id="+myId+") reached backup");
                    FinishReplicator.removeBackupOrMarkToDelete(myId);
                }
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyRootTx returning");
            }
        }
    }
    
    static def setToString(set:HashSet[Id]) {
        if (set == null)
            return "set = NULL";
        var str:String = "{";
        for (e in set)
            str += e + "  ";
        str += "}";
        return str;
    }
}