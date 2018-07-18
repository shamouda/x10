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
import x10.util.Set;
import x10.util.concurrent.Lock;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.util.concurrent.Condition;
import x10.util.Timer;
import x10.util.resilient.localstore.TxConfig;

/**
 * Place0-based Resilient Finish using the optimistic counting protocol
 */
class FinishResilientPlace0Optimistic extends FinishResilient implements CustomSerialization {
    protected transient var me:FinishResilient; // local finish object
    val id:Id;

	public def toString():String {
        return me.toString();
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
    def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) { me.notifyTxActivityTermination(srcPlace, readOnly, t); }
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def spawnRemoteActivity(place:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { me.spawnRemoteActivity(place, body, prof); }
    def waitForFinish():void { me.waitForFinish(); }
    
    def notifyActivityTermination(srcPlace:Place,t:CheckedThrowable):void {
        if (me instanceof P0OptimisticRemoteState) {
            if (t == null) me.notifyActivityTermination(srcPlace);
            else me.notifyActivityTermination(srcPlace, t);
        }
        else
            super.notifyActivityTermination(srcPlace, t);
    }
    
    def notifyActivityCreatedAndTerminated(srcPlace:Place,t:CheckedThrowable):void { 
        if (me instanceof P0OptimisticRemoteState) {
            if (t == null) me.notifyActivityCreatedAndTerminated(srcPlace);
            else me.notifyActivityCreatedAndTerminated(srcPlace, t);
        }
        else
            super.notifyActivityCreatedAndTerminated(srcPlace, t);
    }
    
    def notifyShiftedActivityCompletion(srcPlace:Place,t:CheckedThrowable):void {
        if (me instanceof P0OptimisticRemoteState) {
            if (t == null) me.notifyShiftedActivityCompletion(srcPlace);
            else me.notifyShiftedActivityCompletion(srcPlace, t);
        }
        else
            super.notifyShiftedActivityCompletion(srcPlace, t);
    }
    
    //create root finish
    public def this (parent:FinishState) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        me = new P0OptimisticMasterState(id, parent);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = P0OptimisticRemoteState.getOrCreateRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        return new FinishResilientPlace0Optimistic(parent);
    }
    
    def getSource():Place {
        if (me instanceof P0OptimisticMasterState)
            return here;
        else
            return Runtime.activity().srcPlace;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
    	if (me instanceof P0OptimisticMasterState) {
    	    val me2 = (me as P0OptimisticMasterState); 
    		if (!me2.isGlobal)
    	        me2.globalInit(); // Once we have more than 1 copy of the finish state, we must go global
    	}
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    def registerFinishTx(tx:Tx, rootTx:Boolean):void {
        me.registerFinishTx(tx, rootTx);
    }
    
    /**
     * State of a single finish; always stored in Place0
     */
    private static final class State implements x10.io.Unserializable, Releasable {
        val gfs:GlobalRef[P0OptimisticMasterState]; // root finish state
        val id:Id;
        val parentId:Id; // id of parent (UNASSIGNED means no parent / parent is UNCOUNTED)
    
        var numActive:Long = 0;
        var excs:GrowableRail[CheckedThrowable] = null;  // lazily allocated in addException
        val sent = new HashMap[Edge,Int](); //always increasing 
        val transit = new HashMap[Edge,Int]();
        var ghostChildren:HashSet[Id] = null;  //lazily allocated in addException
        var isAdopted:Boolean = false;  //set to true when backup recreates a master
        
        val tx:Tx;
        val isRootTx:Boolean;
        var txStarted:Boolean = false;
        
        
        /**Place0 states**/
        private static val states = (here.id==0) ? new HashMap[Id, State]() : null;
        private static val statesLock = (here.id==0) ? new Lock() : null;
        private static val place0 = Place.FIRST_PLACE;

        private def this(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], tx:Tx, rootTx:Boolean) {
            this.id = id;
            this.parentId = parentId; 
            this.gfs = gfs;
            this.numActive = 1;
            this.tx = tx;
            this.isRootTx = rootTx;
            if ((TxConfig.get().TM_DEBUG || verbose>=1) && tx != null)
                Console.OUT.println("Finish["+id+"] has Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id) );
            sent.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            transit.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
        }
        
        static def printTxStates() {
            try {
                statesLock.lock();
                for (e in states.entries()) {
                    val state = e.getValue();
                    if (state.tx != null) {
                        if (TxConfig.get().TM_DEBUG || verbose>=1) {
                            Console.OUT.println("printTxStates: Finish["+state.id+"] has Tx["+state.tx.id+"] " + TxConfig.txIdToString (state.tx.id)+ " isRootTx["+state.isRootTx+"] numActive["+state.numActive+"] ");
                            state.dump();
                        }
                    }
                }
            } finally {
                statesLock.unlock();
            }
        }
        
        def gc() {
            val id = this.id;
            val set = new HashSet[Int]();
            for (e in sent.entries()) {
                val dst = e.getKey().dst;
                if (dst != here.id as Int && !set.contains(dst) && (tx == null || !tx.contains(dst))) {
                    set.add(dst);
                    at(Place(dst)) @Immediate("optp0_remoteFinishCleanup") async {
                        FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObject(id);
                    }
                }
            }
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        /*count number of remote children created under a given parent (acting as adopter)*/
        def addGhostsUnsafe(newDead:HashSet[Int]):Long {
            var count:Long = 0;
            for (e in states.entries()) {
                val otherState = e.getValue(); 
                if (newDead.contains(otherState.id.home) && otherState.parentId == id) {
                    otherState.isAdopted = true;
                    if (ghostChildren == null)
                        ghostChildren = new HashSet[Id]();
                    ghostChildren.add(otherState.id);
                    count++;
                }
            }
            return count;
            //no more states under this parent from that src should be created
        }
        
        static def updateGhostChildrenAndGetRemoteQueries(newDead:HashSet[Int]):HashMap[Int,OptResolveRequest] {
            val countingReqs = new HashMap[Int,OptResolveRequest]();
            try {
                statesLock.lock();
                val toRemoveState = new HashSet[State]();
                val result = new HashSet[State]();
                for (s in states.entries()) {
                    val id = s.getKey();
                    val state = s.getValue();
                    var calGhosts:Boolean = false;
                    val toRemove = new HashSet[Edge]();
                    for (e in state.transit.entries()) {
                        val edge = e.getKey();
                        if (newDead.contains(edge.dst)) {
                            calGhosts = true;
                            val count = e.getValue();
                            toRemove.add(edge);
                            state.numActive -= count;
                            if (state.numActive < 0)
                                throw new Exception ( here + " State(id="+id+").convertToDead FATAL error, numActive must not be negative");
                            
                            if (edge.kind == ASYNC) {
                                for (1..count) {
                                    if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                                    val dpe = new DeadPlaceException(Place(edge.dst));
                                    dpe.fillInStackTrace();
                                    state.addException(dpe);
                                }
                            }
                        }
                        
                        //prepare list of queries to destination places whose source place died
                        if (!toRemove.contains(edge) && newDead.contains(edge.src)) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(edge.dst, rreq);
                            }
                            val sent = state.sent.get(edge);
                            rreq.countDropped.put(DroppedQueryId(state.id, edge.src, edge.dst, edge.kind, sent), -1n);
                        }
                    }
                    
                    for (e in toRemove)
                        state.transit.remove(e);
                    
                    if (calGhosts) {
                        val count = state.addGhostsUnsafe(newDead);
                        state.numActive += count;
                    }
                    
                    if (state.quiescent()) {
                        toRemoveState.add(state);
                    }
                    
                }
                
                for (s in toRemoveState) {
                    if (s.txStarted)
                        (s.tx as TxResilient).notifyPlaceDeath();
                    else
                        s.tryRelease();
                }
                return countingReqs;
            } finally {
                statesLock.unlock();
            }
        }
        
        static def convertDeadActivities(droppedCounts:HashMap[DroppedQueryId, Int]) {
            try {
                statesLock.lock();
                val toRemoveState = new HashSet[State]();
                for (e in droppedCounts.entries()) {
                    val query = e.getKey();
                    val dropped = e.getValue();
                    if (dropped > 0) {
                        val state = states(query.id);
                        //FATAL IF STATE IS NULL
                        val edge = Edge(query.src, query.dst, query.kind);
                        val oldTransit = state.transit.get(edge);
                        val oldActive = state.numActive;
                        
                        if (oldActive < dropped)
                            throw new Exception(here + " FATAL: dropped tasks counting error id = " + state.id);
                        
                        state.numActive -= dropped;
                        if (oldTransit - dropped == 0n) {
                            state.transit.remove(edge);
                        }
                        else {
                            state.transit.put(edge, oldTransit - dropped);
                        }
                        
                        if (state.quiescent()) {
                            toRemoveState.add(state);
                        }
                    }
                }
                
                for (s in toRemoveState) {
                    if (s.txStarted)
                        (s.tx as TxResilient).notifyPlaceDeath();
                    else
                        s.tryRelease();
                }
            } finally {
                statesLock.unlock();
            }
        }
        
        //P0FUNC
        private static final def getOrCreateState(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], tx:Tx, rootTx:Boolean):State {
            var state:State = states(id);
            if (state == null) {
                if (Place(id.home).isDead()) {
                    if (verbose>=1) debug("<<<< getOrCreateState(id="+id+") failed, state from dead src["+id.home+"] denied");
                    //throw new Exception("state creation from a dead src denied");
                    //no need to handle this exception; the caller has died.
                } else {
                    if (verbose>=1) debug(">>>> initializing state for id="+ id);
                    state = new State(id, parentId, gfs, tx, rootTx);
                    if (tx != null) {
                        tx.initialize(id);
                    }
                    if (verbose>=1) debug(">>>> creating new State id="+ id +" parentId="+parentId + " tx="+tx);
                    states.put(id, state);
                }
            }
            return state;
        }
        
        static def p0CreateState(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], tx:Tx, rootTx:Boolean) {
            Runtime.runImmediateAt(place0, ()=>{
                try {
                    statesLock.lock();
                    getOrCreateState(id, parentId, gfs, tx, rootTx);
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        //called by a remote finish only - the state must be existing at place0
        static def p0TransitGlobal(id:Id, srcId:Int, dstId:Int, kind:Int) {
            Runtime.runImmediateAt(place0, ()=>{
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                    } else if (Place(dstId).isDead()) {
                        if (kind == ASYNC) {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                            states(id).addDeadPlaceException(dstId);
                        } else {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                        }
                    } else {
                        states(id).inTransit(srcId, dstId, kind, "runImmediateAt notifySubActivitySpawn");
                    }
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        
        static def p0FinalizeLocalTx(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], tx:Tx, isRO:Boolean, excs:GrowableRail[CheckedThrowable]) {
            at (place0) @Immediate("p0FinalizeLocalTx_to_zero") async {
                try {
                    statesLock.lock();
                    if (Place(id.home).isDead()) {
                        if (verbose>=1) debug("<<<< finalizeLocalTx(id="+id+") failed, state from dead src["+id.home+"] denied");
                        //throw new Exception("state creation from a dead src denied");
                        //no need to handle this exception; the caller has died.
                    } else {
                        if (verbose>=1) debug(">>>> initializing state for id="+ id);
                        val rootTx = true;
                        val state = new State(id, parentId, gfs, tx, rootTx);
                        if (tx != null) {
                            tx.initialize(id);
                            tx.addMember(id.home, isRO);
                        }
                        if (verbose>=1) debug(">>>> finalizeLocalTx: creating new State id="+ id +" parentId="+parentId + " tx="+tx);
                        state.numActive = 0;
                        state.transit.remove(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC));
                        states.put(id, state);
                        state.excs = excs;
                        state.tryRelease();
                    }
                } finally {
                    statesLock.unlock();
                }
            }
        }
        
        static def p0RemoteSpawnSmallGlobal(id:Id, srcId:Int, dstId:Int, bytes:Rail[Byte],
        		gfs:GlobalRef[FinishState]) {
            at (place0) @Immediate("p0optGlobal_spawnRemoteActivity_to_zero") async {
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                    } else {
                        val state = states(id);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(small async)");
                        }
                    } 
                } finally {
                    statesLock.unlock();
                }
                                
                try {
                    at (Place(dstId)) @Immediate("p0optGlobal_spawnRemoteActivity_dstPlace") async {
                        if (verbose >= 1) debug("==== spawnRemoteActivity(id="+id+") submitting activity from "+here.id+" at "+dstId);
                        val wrappedBody = ()=> {
                            // defer deserialization to reduce work on immediate thread
                            val deser = new Deserializer(bytes);
                            val bodyPrime = deser.readAny() as ()=>void;
                            bodyPrime();
                        };
                        val fs = (gfs as GlobalRef[FinishState]{self.home == here})();
                        Runtime.worker().push(new Activity(42, wrappedBody, fs, Place(srcId)));
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the place just died there is no need to worry about submitting the activity
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_dstPlace for "+id);
                }
            }
        }
        
        static def p0RemoteSpawnBigGlobal(id:Id, srcId:Int, dstId:Int, bytes:Rail[Byte],
        		gfs:GlobalRef[FinishState]) {
            val wrappedBody = ()=> @AsyncClosure {
                val deser = new Deserializer(bytes);
                val bodyPrime = deser.readAny() as ()=>void;
                bodyPrime();
            };
            val wbgr = GlobalRef(wrappedBody);
            at (place0) @Immediate("p0optGlobal_spawnRemoteActivity_big_async_to_zero") async {
                var markedInTransit:Boolean = false;
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                    } else {
                        val state = states(id);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(large async)");
                            markedInTransit = true;
                        }
                    }
                } finally {
                    statesLock.unlock();
                }
                try {
                    val mt = markedInTransit;
                    at (wbgr) @Immediate("p0optGlobal_spawnRemoteActivity_big_back_to_spawner") async {
                        val fs = (gfs as GlobalRef[FinishState]{self.home == here})();
                        try {
                            if (mt) x10.xrx.Runtime.x10rtSendAsync(dstId, wbgr(), fs, null, null);
                        } catch (dpe:DeadPlaceException) {
                            // not relevant to immediate thread; DPE raised in convertDeadActivities
                            if (verbose>=2) debug("caught and suppressed DPE from x10rtSendAsync from spawnRemoteActivity_big_back_to_spawner for "+id);
                        }
                        wbgr.forget();
                        fs.notifyActivityTermination(Place(srcId));
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the src place just died there is nothing left to do.
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_big_back_to_spawner for "+id);
                }
            }
        }
        
        static def p0RemoteSpawnSmall(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, bytes:Rail[Byte],
        		ref:GlobalRef[FinishState], tx:Tx, rootTx:Boolean) {
            at (place0) @Immediate("p0opt_spawnRemoteActivity_to_zero") async {
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                    } else {
                    	val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(small async)");
                        }
                    }
                } finally {
                    statesLock.unlock();
                }

                try {
                    at (Place(dstId)) @Immediate("p0opt_spawnRemoteActivity_dstPlace") async {
                        if (verbose >= 1) debug("==== spawnRemoteActivity(id="+id+") submitting activity from "+here.id+" at "+dstId);
                        val wrappedBody = ()=> {
                            // defer deserialization to reduce work on immediate thread
                            val deser = new Deserializer(bytes);
                            val bodyPrime = deser.readAny() as ()=>void;
                            bodyPrime();
                        };
                        val fs = (ref as GlobalRef[FinishState]{self.home == here})();
                        Runtime.worker().push(new Activity(42, wrappedBody, fs, Place(srcId)));
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the place just died there is no need to worry about submitting the activity
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_dstPlace for "+id);
                }
            }
        }
        
        static def p0RemoteSpawnBig(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, bytes:Rail[Byte],
        		ref:GlobalRef[FinishState], tx:Tx, rootTx:Boolean) {
            val wrappedBody = ()=> @AsyncClosure {
                val deser = new Deserializer(bytes);
                val bodyPrime = deser.readAny() as ()=>void;
                bodyPrime();
            };
            val wbgr = GlobalRef(wrappedBody);
            at (place0) @Immediate("p0opt_spawnRemoteActivity_big_async_to_zero") async {
                var markedInTransit:Boolean = false;
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                    } else {
                    	val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(large async)");
                            markedInTransit = true;
                        }
                    } 
                } finally {
                    statesLock.unlock();
                }
                try {
                    val mt = markedInTransit;
                    at (wbgr) @Immediate("p0opt_spawnRemoteActivity_big_back_to_spawner") async {
                        val fs = (ref as GlobalRef[FinishState]{self.home == here})();
                        try {
                            if (mt) x10.xrx.Runtime.x10rtSendAsync(dstId, wbgr(), fs, null, null);
                        } catch (dpe:DeadPlaceException) {
                            // not relevant to immediate thread; DPE raised in convertDeadActivities
                            if (verbose>=2) debug("caught and suppressed DPE from x10rtSendAsync from spawnRemoteActivity_big_back_to_spawner for "+id);
                        }
                        wbgr.forget();
                        fs.notifyActivityTermination(Place(srcId));
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the src place just died there is nothing left to do.
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_big_back_to_spawner for "+id);
                }
            }
        }
        
        //called by root finish - may need to create the State object
        static def p0Transit(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, kind:Int, tx:Tx, rootTx:Boolean) {
            Runtime.runImmediateAt(place0, ()=>{
                try {
                    statesLock.lock();
                    if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                    } else if (Place(dstId).isDead()) {
                        if (kind == ASYNC) {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                            val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                            state.addDeadPlaceException(dstId);
                        } else {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                        }
                    } else {
                        val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                        state.inTransit(srcId, dstId, kind, "blocking runImmediateAt notifySubActivitySpawn");
                    }
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        static def p0PushExceptionGlobal(id:Id, t:CheckedThrowable) {
            Runtime.runImmediateAt(place0, ()=>{ 
                try {
                    statesLock.lock();
                    val state = states(id);
                    if (!state.isAdopted) { // If adopted, the language semantics are to suppress exception.
                        state.addException(t);
                    }
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        static def p0PushException(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], t:CheckedThrowable, tx:Tx, rootTx:Boolean) {
            Runtime.runImmediateAt(place0, ()=>{ 
                try {
                    statesLock.lock();
                    val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                    if (!state.isAdopted) { // If adopted, the language semantics are to suppress exception.
                        state.addException(t);
                    }
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        static def p0TransitToCompletedGlobal(id:Id, srcId:Int, dstId:Int, kind:Int, t:CheckedThrowable) {
            at (place0) @Immediate("p0OptGlobal_notifyActivityCreationFailed_to_zero") async {
                try {
                    statesLock.lock();
                    if (Place(dstId).isDead()) {
                        // drop termination messages from a dead place; only simulated termination signals are accepted
                        if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
                    } else {
                        if (verbose>=1) debug(">>>> State.p0TransitToCompletedGlobal(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") called");
                        states(id).transitToCompleted(srcId, dstId, kind, t, false, false);
                        if (verbose>=1) debug("<<<< State.p0TransitToCompletedGlobal(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") returning");
                    }
                } finally {
                    statesLock.unlock();
                }
            }
        }
        
        static def p0TransitToCompleted(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, 
                kind:Int, t:CheckedThrowable, tx:Tx, rootTx:Boolean, isTx:Boolean, isTxRO:Boolean) {
            at (place0) @Immediate("p0Opt_notifyActivityCreationFailed_to_zero") async {
                try {
                    statesLock.lock();
                    if (Place(dstId).isDead()) {
                        // drop termination messages from a dead place; only simulated termination signals are accepted
                        if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
                    } else {
                        val state = getOrCreateState(id, parentId, gfs, tx, rootTx);
                        if (verbose>=1) debug(">>>> State.p0TransitToCompleted(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") called");
                        state.transitToCompleted(srcId, dstId, kind, t, isTx, isTxRO);
                        if (verbose>=1) debug("<<<< State.p0TransitToCompleted(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") returning");
                    }
                } finally {
                    statesLock.unlock();
                }
           }
        }
        
        static def p0TermMultiple(id:Id, dstId:Int, map:HashMap[Task,Int], t:CheckedThrowable, isTx:Boolean, isTxRO:Boolean) {
            if (verbose>=1) debug(">>>> State(id="+id+").p0TermMultiple [dstId=" + dstId +", size="+map.size()+", isTx="+isTx+", isTxRO="+isTxRO+" ] called");
            at (place0) @Immediate("p0Opt_notifyTermMul_to_zero") async {
                if (map == null )
                    throw new Exception(here + " FATAL ERROR p0TermMultiple(id="+id+", dstId="+dstId+", map="+map+") map is null");
                
                if (verbose>=1) debug("==== State(id="+id+").p0TermMultiple [dstId=" + dstId +", mapSize="+map.size()+"] called");
                try {
                    statesLock.lock();
                    val state = states(id);
                    if (t != null) state.addException(t);
                    if (isTx && state.tx != null) {
                        state.tx.addMember(dstId, isTxRO);
                    }
                    for (e in map.entries()) {
                        val srcId = e.getKey().place;
                        val kind = e.getKey().kind;
                        val cnt = e.getValue();
                        state.transitToCompletedMul(srcId, dstId, kind, cnt); // drops the term msg if dst is dead
                    }
                    if (verbose>=1) debug("<<<< State(id="+id+").p0TermMultiple [dstId=" + dstId +", size="+map.size() + ", isTx="+isTx+", isTxRO="+isTxRO+", tx="+state.tx+" ] returning");
                } finally {
                    statesLock.unlock();
                }
            }
        }
        
        static def p0HereTermMultiple(id:Id, dstId:Int, map:HashMap[Task,Int], t:CheckedThrowable, isTx:Boolean, isTxRO:Boolean) {           
            if (verbose>=1) debug(">>>> State(id="+id+").p0HereTermMultiple [dstId=" + dstId +", size="+map.size()+", isTx="+isTx+", isTxRO="+isTxRO+" ] called");
            try {
                statesLock.lock();
                val state = states(id);
                if (t != null) state.addException(t);
                if (isTx && state.tx != null) {
                    state.tx.addMember(dstId, isTxRO);
                }
                for (e in map.entries()) {
                    val srcId = e.getKey().place;
                    val kind = e.getKey().kind;
                    val cnt = e.getValue();
                    state.transitToCompletedMul(srcId, dstId, kind, cnt); // drops the term msg if dst is dead
                }
                if (verbose>=1) debug("<<<< State(id="+id+").p0HereTermMultiple [dstId=" + dstId +", size="+map.size() + ", isTx="+isTx+", isTxRO="+isTxRO+", tx="+state.tx+" ] returning");
            } finally {
                statesLock.unlock();
            }
        }
        
        def addException(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
            if (verbose>=1) debug(">>>> State(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " tag="+tag+" called");
            val e = Edge(srcId, dstId, kind);
            increment(transit, e);
            increment(sent, e);
            numActive++;
            if (verbose>=3) debug("==== State(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
            if (verbose>=3) dump();
            if (verbose>=1) debug("<<<< State(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, isTx:Boolean, isTxRO:Boolean) {
            if (verbose>=1) debug(">>>> State(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
            if (Place(dstId).isDead()) {
                // NOTE: no state updates or DPE processing here.
                //       Must happen exactly once and is done
                //       when Place0 is notified of a dead place.
                if (verbose>=1) debug("<<<< State(id="+id+").transitToCompleted returning suppressed dead dst id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            }
            else {
                if (isTx && tx != null) {
                    tx.addMember(dstId as Int, isTxRO);
                }
                
                val e = Edge(srcId, dstId, kind);
                decrement(transit, e);
                assert transit.getOrElse(e, 0n) >= 0n : here + " ["+Runtime.activity()+"] FATAL error, transit reached negative id="+id;
                //don't decrement 'sent'
                numActive--;
                assert numActive>=0 : here + " FATAL error, State(id="+id+").numActive reached -ve value";
                if (t != null) addException(t);
                if (quiescent()) {
                    tryRelease();
                }
                if (verbose>=1) debug("<<<< State(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            }
        }

        public def releaseFinish(excs:GrowableRail[CheckedThrowable]) {
            if (excs != null) {
                for (t in excs.toRail())
                    addException(t);
            }
            try {
                statesLock.lock();
                releaseLatch();
                notifyParent();
                removeFromStates();
            } finally {
                statesLock.unlock();
            }
        }
        
        def tryRelease() {
            if (tx == null || tx.isEmpty() || !isRootTx) {
                releaseLatch();
                notifyRootTx();
                notifyParent();
                removeFromStates();
            } else { //start the commit procedure
                txStarted = true;
                var abort:Boolean = false;
                if (excs != null && excs.size() > 0) {
                    abort = true;
                }
                tx.finalize(this, abort); //this also performs gc
            }
        }
        
        def removeGhostChild(childId:Id) {
            if (verbose>=1) debug(">>>> State(id="+id+").removeGhostChild childId=" + childId + " called");
            if (ghostChildren == null || !ghostChildren.contains(childId))
                throw new Exception(here + " FATAL error, State(id="+id+") does not has the ghost child " + childId);
            ghostChildren.remove(childId);
            numActive--;
            if (numActive < 0)
                throw new Exception (here + " FATAL error, State(id="+id+").numActive reached -ve value" );
            addException(new DeadPlaceException(Place(childId.home)));
            if (quiescent()) {
                tryRelease();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").removeGhostChild childId=" + childId + " returning");
        }
        
        def transitToCompletedMul(srcId:Long, dstId:Long, kind:Int, cnt:Int) {
            if (verbose>=1) debug(">>>> State(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + ", kind="+kind +" called");
            if (verbose>=3) dump();
            if (Place(dstId).isDead()) {
                // NOTE: no state updates or DPE processing here.
                //       Must happen exactly once and is done
                //       when Place0 is notified of a dead place.
                if (verbose>=1) debug("<<<< State(id="+id+").transitToCompletedMul returning suppressed dead dst id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            }
            else {
                val e = Edge(srcId, dstId, kind);
                deduct(transit, e, cnt);
                
                if (transit.getOrElse(e, 0n) < 0n)
                    throw new Exception(here + " FATAL error, transit reached negative id="+id);
                
                //don't decrement 'sent'
                numActive-=cnt;
                
                if ( numActive < 0 )
                    throw new Exception(here + " FATAL error, State(id="+id+").numActive reached -ve value");
                
                if (quiescent()) {
                    tryRelease();
                }
                if (verbose>=1) debug("<<<< State(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            }
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> State(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: State(id="+id+").quiescent negative numActive!!!");
                dump();
                assert false : "COUNTING ERROR: State(id="+id+").quiescent negative numActive!!!";
                return true; 
            }
        
            val quiet = numActive == 0;
            if (verbose>=3) dump();
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< State(id="+id+").quiescent returning " + quiet);
            return quiet;
        }

        def releaseLatch() {
            
            if (REMOTE_GC) gc();
            
            if (isAdopted) { //
                if (verbose>=1) debug("releaseLatch(id="+id+") called on lost finish; not releasing latch");
            } else {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                if (verbose>=2) debug("releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));

                val mygfs = gfs;
                try {
                    at (mygfs.home) @Immediate("p0Opt_releaseLatch_gfs_home") async {
                        val fs = mygfs();
                        if (verbose>=2) debug("performing release for "+fs.id+" at "+here);
                        if (exceptions != null) {
                            fs.latch.lock();
                            if (fs.excs == null) fs.excs = new GrowableRail[CheckedThrowable](exceptions.size);
                            fs.excs.addAll(exceptions);
                            fs.latch.unlock();
                        }
                        fs.latch.release();
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the place is dead there is no need to unlatch a waiting activity there
                    if (verbose>=2) debug("caught and suppressed DPE when attempting to release latch for "+id);
                }
            }
            if (verbose>=2) debug("releaseLatch(id="+id+") returning");
        }
        
        def notifyRootTx() {
            if (verbose>=1) debug(">>>> State(id="+id+").notifyRootTx(parentId=" + parentId +") called");
            if ( tx == null || isRootTx) {
                if (verbose>=1) debug("<<< State(id="+id+").notifyParent returning, not a sub transaction tx["+tx+"] isRoot["+isRootTx+"]");
                return;
            }
            val parentState = states(parentId);
            parentState.tx.addSubMembers(tx.getMembers(), tx.isReadOnly());
            if (verbose>=1) debug("<<<< State(id="+id+").notifyRootTx(parentId=" + parentId +", parentTxId="+parentState.tx.id+", parentTx="+parentState.tx+") returning");
        }
        
        def notifyParent() {
            if (verbose>=1) debug(">>>> State(id="+id+").notifyParent(parentId=" + parentId + ", isAdopted="+isAdopted+") called");
            if ( !isAdopted || parentId == FinishResilient.UNASSIGNED) {
                if (verbose>=1) debug("<<< State(id="+id+").notifyParent returning, not lost or parent["+parentId+"] does not exist");
                return;
            }
            val parentState = states(parentId);
            parentState.removeGhostChild(id);
            if (verbose>=1) debug("<<<< State(id="+id+").notifyParent(parentId=" + parentId + ", isAdopted="+isAdopted+") returning");
        }
        
        def removeFromStates() {
            states.remove(id);
            if (verbose>=1) debug("<<<< State(id="+id+").removeFromStates() returning");
        }

        def addDeadPlaceException(placeId:Long) {
            val e = new DeadPlaceException(Place(placeId));
            e.fillInStackTrace();
            addException(e);
        }

        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("State dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (sent.size() > 0) {
                s.add("   sent-transit:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+" - "+transit.getOrElse(e.getKey(),0n)+"\n");
                }
            }
            debug(s.toString());
        }
        
    }
    
    //REMOTE
    public static final class P0OptimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
        val id:Id; //parent root finish
        val ilock = new Lock(); //instance level lock
        val received = new HashMap[Task,Int](); //increasing counts
        val reported = new HashMap[Task,Int](); //increasing counts
        var taskDeny:HashSet[Int] = null; // lazily allocated
        val lc = new AtomicInteger(0n);//each time lc reaches zero, we report reported-received to root finish and set reported=received
        
        private static val remoteLock = new Lock();
        private static val remotes = new HashMap[Id, P0OptimisticRemoteState](); //a cache for remote finish objects
        private static val remoteDeny = new HashSet[Id](); //remote deny list
        
        private var txFlag:Boolean = false;
        private var txReadOnlyFlag:Boolean = true;
        
        private var ex:CheckedThrowable = null;
        
        
        public def toString() {
            return "P0OptimisticRemote(id="+id+", localCount="+lc.get()+")";
        }
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        public static def deleteObjects(gcReqs:Set[Id]) {
            if (gcReqs == null) {
                return;
            }
            try {
                remoteLock.lock();
                for (id in gcReqs) {
                    remotes.delete(id);
                }
            } finally {
                remoteLock.unlock();
            }
        }
        
        public static def deleteObject(id:Id) {
            try {
                remoteLock.lock();
                remotes.delete(id);
            } finally {
                remoteLock.unlock();
            }
        }
        
        def setTxFlagsUnsafe(isTx:Boolean, isTxRO:Boolean) {
            txFlag = txFlag | isTx;
            txReadOnlyFlag = txReadOnlyFlag & isTxRO;
        }
        
        public static def countDropped(id:Id, src:Int, kind:Int, sent:Int) {
            if (verbose>=1) debug(">>>> countDropped(id="+id+", src="+src+", kind="+kind+", sent="+sent+") called");
            var dropped:Int = 0n;
            try {
                remoteLock.lock();
                val remote = remotes.getOrElse(id, null);
                if (remote != null) {
                    val received = remote.receivedFrom(src, kind);
                    dropped = sent - received;
                } else {
                    //no remote finish for this id should be created here
                    remoteDeny.add(id);
                    dropped = sent;
                }
            } finally {
                remoteLock.unlock();
            }
            if (verbose>=1) debug("<<<< countDropped(id="+id+", src="+src+", kind="+kind+", sent="+sent+") returning, dropped="+dropped);
            return dropped;
        }
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:P0OptimisticRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
                    //FIXME: return a dummy remote rather than a fatal RemoteCreationDenied
                    if (remoteDeny.contains(id)) {
                        throw new RemoteCreationDenied();
                    }
                    remoteState = new P0OptimisticRemoteState(id);
                    remotes.put(id, remoteState);
                }
                return remoteState;
            } finally {
                remoteLock.unlock();
            }
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
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Remote dump:\n");
            s.add("           here:" + here.id); s.add('\n');
            s.add("             id:" + id); s.add('\n');            
            s.add("     localCount:"); s.add(lc); s.add('\n');
            if (received.size() > 0) {
                s.add("    recv-report:\n");
                for (e in received.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+" - "+reported.getOrElse(e.getKey(), 0n)+"\n");
                }
            }
            debug(s.toString());
        }
        
        //Calculates the delta between received and reported
        private def getReportMapUnsafe() {
                if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMap called");
                var map:HashMap[Task,Int] = null;
                val iter = received.keySet().iterator();
                while (iter.hasNext()) {
                    val t = iter.next();
                    if (t.place == here.id as Int) {
                        if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] ignored");
                        continue;
                    }
                    val rep = reported.getOrElse(t, 0n);
                    val rec = received.getOrThrow(t);
                    
                    if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] rep="+rep + " rec = " + rec);
                    if ( rep < rec) {
                        if (map == null)
                            map = new HashMap[Task,Int]();
                        map.put(t, rec - rep);
                        reported.put (t, rec);
                        if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] reported.put("+t+","+(rec-rep)+")");
                    }
                }
                
                /*map can be null here: because notifyTerminationAndGetMap doesn't decrement lc and get the map as a one atomic action,  
                  it is possible that two activities reach zero lc and then one of them reports all of the activities, while the other finds no activity to report */
                if (verbose>=1) printMap(map);
                if (verbose>=3) dump();
                if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMap returning");
                return map;
        }
        
        //Calculates the delta between received and reported
        private def getReportMap(exp:CheckedThrowable) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMap called");
                if (exp != null)
                    ex = exp;
                var map:HashMap[Task,Int] = null;
                val iter = received.keySet().iterator();
                while (iter.hasNext()) {
                    val t = iter.next();
                    if (t.place == here.id as Int) {
                        if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] ignored");
                        continue;
                    }
                    val rep = reported.getOrElse(t, 0n);
                    val rec = received.getOrThrow(t);
                    
                    if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] rep="+rep + " rec = " + rec);
                    if ( rep < rec) {
                        if (map == null)
                            map = new HashMap[Task,Int]();
                        map.put(t, rec - rep);
                        reported.put (t, rec);
                        if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] reported.put("+t+","+(rec-rep)+")");
                    }
                }
                
                /*map can be null here: because notifyTerminationAndGetMap doesn't decrement lc and get the map as a one atomic action,  
                  it is possible that two activities reach zero lc and then one of them reports all of the activities, while the other finds no activity to report */
                if (verbose>=1) printMap(map);
                if (verbose>=3) dump();
                if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMap returning");
                return map;
            } finally {
                ilock.unlock();
            }
        }
        
        def recordExp(exp:CheckedThrowable) {
            ilock.lock();
            if (exp != null)
                ex = exp;
            ilock.unlock();
        }
        
        public def notifyTerminationAndGetMap(t:Task, ex:CheckedThrowable) {
            var map:HashMap[Task,Int] = null;
            val count = lc.decrementAndGet();
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called, taskFrom["+t.place+"] lc="+count);
            if (count == 0n) {
                map = getReportMap(ex);
            }
            else {
                recordExp(ex);
            }
            return map;
        }
        
        public def notifyReceived(t:Task) {
            var count:Int = -1n;
            try {
                ilock.lock();
                if (taskDeny != null && taskDeny.contains(t.place) )
                    throw new RemoteCreationDenied();
                increment(received, t);
                count = lc.incrementAndGet();
            } finally {
                ilock.unlock();
            }
            return count;
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
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called locally, no action required");
            } else {
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
                State.p0TransitGlobal(id, srcId, dstId, kind);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") called");
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            val fs = Runtime.activity().finishState();
            val gfs = new GlobalRef[FinishState](fs);
            
            if (bytes.size >= ASYNC_SIZE_THRESHOLD) {
            	val count = notifyReceived(Task(srcId, ASYNC)); // synthetic activity to keep finish locally live during async to Place0
            	if (verbose >= 1) debug("==== Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting indirect (size="+ bytes.size+") lc="+count);
            	State.p0RemoteSpawnBigGlobal(id, srcId, dstId, bytes, gfs);
            } else {
                if (verbose >= 1) debug("====  Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting direct (size="+ bytes.size+") ");
            	State.p0RemoteSpawnSmallGlobal(id, srcId, dstId, bytes, gfs);
            }
            if (verbose>=1) debug("<<<< Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") returning");
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            try {
                val count = notifyReceived(Task(srcId, ASYNC));
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
                return true;
            } catch (e:RemoteCreationDenied) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, task denied");
                return false;
            }
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") called");
            try {
                val count = notifyReceived(Task(srcId, AT));
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, localCount = " + count);
                return true;
            } catch (e:RemoteCreationDenied) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, task denied");
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
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            State.p0TransitToCompletedGlobal(id, srcId, dstId, kind, t);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, null);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int, t:CheckedThrowable) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val count = notifyReceived(Task(srcId, ASYNC));
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind), t);
            if (map == null) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }            
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root, mapSize=" + map.size());
            if (here.id == 0)
                State.p0HereTermMultiple(id, dstId, map, ex, false, false);
            else {
                State.p0TermMultiple(id, dstId, map, ex, false, false);
            }
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            State.p0PushExceptionGlobal(id, t);
            if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+") returning");
        }

        def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) {
            notifyActivityTermination(srcPlace, ASYNC, t, true, readOnly);
        }
        
        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, null, false, false);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, null, false, false);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, t:CheckedThrowable, isTx:Boolean, readOnly:Boolean):void {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            var map:HashMap[Task,Int] = null;
            var tmpTx:Boolean = false;
            var tmpTxRO:Boolean = false;

            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+", readonly="+readOnly+") called ");
            val count = lc.decrementAndGet();
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called, taskFrom["+srcId+"] lc="+count);
            
            if (count == 0n) {
                ilock.lock();
                map = getReportMapUnsafe();
                setTxFlagsUnsafe(isTx, readOnly);
                tmpTx = txFlag;
                tmpTxRO = txReadOnlyFlag;
                if (t != null)
                    ex = t;
                ilock.unlock();
            } else if (isTx) {
                ilock.lock();
                setTxFlagsUnsafe(isTx, readOnly);
                if (t != null)
                    ex = t;
                ilock.unlock();
            }
            
            if (map == null)
                return;
           
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+", tmpTx="+tmpTx+", tmpTxRO="+tmpTxRO+") reporting to root, mapSize=" + map.size());
            if (here.id == 0)
                State.p0HereTermMultiple(id, dstId, map, ex, tmpTx, tmpTxRO);
            else
                State.p0TermMultiple(id, dstId, map, ex, tmpTx, tmpTxRO);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
        
        def notifyActivityTermination(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, ASYNC, t, false, false);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place,t:CheckedThrowable):void {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, t);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place,t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, AT, t, false, false);
        }
    }
    
    //ROOT
    public static final class P0OptimisticMasterState extends FinishResilient implements x10.io.Unserializable, Releasable {
        val ref:GlobalRef[P0OptimisticMasterState] = GlobalRef[P0OptimisticMasterState](this);
        val id:Id;
        val parentId:Id;
        val parent:FinishState;//the direct parent finish object (used in globalInit for recursive initializing)
        val latch = new SimpleLatch();//latch for blocking and releasing the host activity
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        var lc:Int = 1n;
        
        private var txFlag:Boolean = false;
        private var txReadOnlyFlag:Boolean = true;
        
        public def registerFinishTx(old:Tx, rootTx:Boolean):void {
            if (verbose>=1) debug(">>>> registerFinishTx(id="+id+", txId="+old.id+", root="+rootTx+") ");
            this.isRootTx = rootTx;
            if (rootTx)
                this.tx = old;
            else
                this.tx = Tx.clone(old);
            this.tx.initialize(id);
            if (verbose>=1) debug("<<<< registerFinishTx(id="+id+", txId="+old.id+", root="+rootTx+",tx="+this.tx+") returning");
        }
        
        public def toString() {
            return "P0OptimisticRoot(id="+id+", parentId="+parentId+", localCount="+lc_get()+")";
        }
        
        def this(id:Id, parent:FinishState) {
            this.parent = parent;
            if (parent instanceof FinishResilientPlace0Optimistic) {
                this.id = id;
                this.parentId = (parent as FinishResilientPlace0Optimistic).id;                
            } else {
                this.id = id;
                this.parentId = UNASSIGNED;
            }
        }
        
        def globalInit() {
            latch.lock();
            strictFinish = true;
            if (!isGlobal) {
                if (verbose>=1) debug(">>>> doing globalInit for id="+id);
                if (parent instanceof FinishResilientPlace0Optimistic) {
                    val frParent = parent as FinishResilientPlace0Optimistic;
                    if (frParent.me instanceof P0OptimisticMasterState) {
                        val par = (frParent.me as P0OptimisticMasterState);
                        if (!par.isGlobal) par.globalInit();
                    }
                }
                State.p0CreateState(id, parentId, ref, tx, isRootTx);
                isGlobal = true;
                if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            latch.unlock();
        }
        
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
        
        public def lock() {
            latch.lock();
        }
        
        public def unlock() {
            latch.unlock();
        }
                
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
            if (verbose>=1) debug("<<<< addExceptionUnsafe(id="+id+") t="+t.getMessage() + " exceptions size = " + excs.size());
        }
        
        def addDeadPlaceException(placeId:Long) {
            try {
                latch.lock();
                val dpe = new DeadPlaceException(Place(placeId));
                dpe.fillInStackTrace();
                addExceptionUnsafe(dpe);
            } finally {
                latch.unlock();
            }
        }
        
        def addException(t:CheckedThrowable) {
            try {
                latch.lock();
                addExceptionUnsafe(t);
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
        
        def setTxFlags(isTx:Boolean, isTxRO:Boolean) {
            if (verbose>=1) debug(">>>> Root(id="+id+").setTxFlags("+isTx+","+isTxRO+")");
            if (tx == null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").setTxFlags("+isTx+","+isTxRO+") returning, tx is null");
                return;
            }
            
            try {
                latch.lock();
                tx.addMember(here.id as Int, isTxRO);
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
                val count = lc_incrementAndGet();
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+count);
            } else {
                isGlobal = true;
                if (parent instanceof FinishResilientPlace0Optimistic) {
                    val frParent = parent as FinishResilientPlace0Optimistic;
                    if (frParent.me instanceof P0OptimisticMasterState) {
                        val par = (frParent.me as P0OptimisticMasterState);
                        if (!par.isGlobal) par.globalInit();
                    }
                }
                
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            	State.p0Transit(id, parentId, ref, srcId, dstId, kind, tx, isRootTx);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            isGlobal = true;
            if (parent instanceof FinishResilientPlace0Optimistic) {
                val frParent = parent as FinishResilientPlace0Optimistic;
                if (frParent.me instanceof P0OptimisticMasterState) {
                    val par = (frParent.me as P0OptimisticMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") called");
            val start = prof != null ? System.nanoTime() : 0;
            val ser = new Serializer();
            ser.writeAny(body);
            if (prof != null) {
                val end = System.nanoTime();
                prof.serializationNanos += (end-start);
                prof.bytes += ser.dataBytesWritten();
            }
            val bytes = ser.toRail();
            val fs = Runtime.activity().finishState();
            val gfs = new GlobalRef[FinishState](fs);
            if (bytes.size >= ASYNC_SIZE_THRESHOLD) {
            	val count = lc_incrementAndGet(); // synthetic activity to keep finish locally live during async to Place0
            	if (verbose >= 1) debug("==== Root(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting indirect (size="+ bytes.size+") localCount now " + count);
            	State.p0RemoteSpawnBig(id, parentId, ref, srcId, dstId, bytes, gfs, tx, isRootTx);
            } else {
                if (verbose >= 1) debug("====  Root(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting direct (size="+ bytes.size+") ");
            	State.p0RemoteSpawnSmall(id, parentId, ref, srcId, dstId, bytes, gfs, tx, isRootTx);
            }
            if (verbose>=1) debug("<<<< Root(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") returning");
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
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
            if (parent instanceof FinishResilientPlace0Optimistic) {
                val frParent = parent as FinishResilientPlace0Optimistic;
                if (frParent.me instanceof P0OptimisticMasterState) {
                    val par = (frParent.me as P0OptimisticMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            State.p0TransitToCompleted(id, parentId, ref, srcId, dstId, kind, t, tx, isRootTx, false, false);
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
            
            isGlobal = true;
            if (parent instanceof FinishResilientPlace0Optimistic) {
                val frParent = parent as FinishResilientPlace0Optimistic;
                if (frParent.me instanceof P0OptimisticMasterState) {
                    val par = (frParent.me as P0OptimisticMasterState);
                    if (!par.isGlobal) par.globalInit();
                }
            }
            
            
            if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            State.p0PushException(id, parentId, ref, t, tx, isRootTx);
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
        }
        
        def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) {
            notifyActivityTermination(srcPlace, ASYNC, true, readOnly);
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, false, false);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, false, false);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, isTx:Boolean, isTxRO:Boolean):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind
                    +",isTx="+isTx+",isTxRO="+isTxRO+",tx="+tx+") called");           
            if (isTx && tx != null)
                setTxFlags(isTx, isTxRO);
            val count = lc_decrementAndGet();
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind
                    +",isTx="+isTx+",isTxRO="+isTxRO+",tx="+tx+"), decremented localCount to "+count);
            if (count > 0) {
                return;
            }

            if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") returning");
                tryReleaseLocal();
                return;
            }
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+",tx="+tx+","+txFlag+","+txReadOnlyFlag+") called");
            State.p0TransitToCompleted(id, parentId, ref, srcId, dstId, kind, null, tx, isRootTx, txFlag, txReadOnlyFlag);
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+",tx="+tx+","+txFlag+","+txReadOnlyFlag+") returning");
        }

        private def forgetGlobalRefs():void {
            (ref as GlobalRef[P0OptimisticMasterState]{self.home == here}).forget();
        }
        
        public def releaseFinish(excs:GrowableRail[CheckedThrowable]) {
            if (excs != null) {
                for (t in excs.toRail())
                    addExceptionUnsafe(t);//unsafe: only one thread reaches this method
            }
            latch.release();
        }

        private def tryReleaseLocal() {
            if (tx == null || tx.isEmpty() || !isRootTx) {
        		latch.release();
        		//We don't need to do parentTx.addSubMembers because this place is part of the transaction any way
        	} else {
        	    State.p0FinalizeLocalTx(id, parentId, ref, tx, txReadOnlyFlag, excs);
        		/*tx.addMember(here.id as Int, txReadOnlyFlag);
                var abort:Boolean = false;
                if (excs != null && excs.size() > 0) {
                    abort = true;
                }
                tx.finalizeLocal(this, abort);*/
        	}
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

            // no more messages will come back to this finish state 
            forgetGlobalRefs();
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").waitForFinish returning with exceptions, size=" + excs.size() );
                throw new MultipleExceptions(excs);
            }
        }
    }
    
    /*************** Adoption Logic ******************/
    static def processCountingRequests(countingReqs:HashMap[Int,OptResolveRequest]) {
        if (verbose>=1) debug(">>>> processCountingRequests(size="+countingReqs.size()+") called");
        if (countingReqs.size() == 0) {
            if (verbose>=1) debug("<<<< processCountingRequests(size="+countingReqs.size()+") returning, zero size");
            return;
        }
        val places = new Rail[Int](countingReqs.size());
        val iter = countingReqs.keySet().iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val pl = iter.next();
            places(i++) = pl;
        }
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val outputGr = GlobalRef[HashMap[Int,OptResolveRequest]](countingReqs);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                val requests = countingReqs.getOrThrow(p);
                if (verbose>=1) debug("==== processCountingRequests  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("counting_request") async {
                        if (verbose>=1) debug("==== processCountingRequests  reached from " + gr.home + " to " + here);
                        val countDropped = requests.countDropped;
                        
                        if (countDropped.size() > 0) {
                            for (r in countDropped.entries()) {
                                val key = r.getKey();
                                val count = P0OptimisticRemoteState.countDropped(key.id, key.src, key.kind, key.sent);
                                countDropped.put(key, count);
                            }
                        }
                        
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== processCountingRequests  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("counting_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,OptResolveRequest]]{self.home == here})().getOrThrow(me);
                            for (vr in countDropped.entries()) {
                                output.countDropped.put(vr.getKey(), vr.getValue());
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
            throw new Exception("FATAL ERROR: another place failed during recovery ...");
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (here.id != 0) {
            if (verbose>=2) debug(">>>> notifyPlaceDeath returning, not place0");
            return;
        }
       
        val newDead = FinishReplicator.getNewDeadPlaces();
        if (newDead == null || newDead.size() == 0) //occurs at program termination
            return;
        
        val start = Timer.milliTime();
        val countingReqs = State.updateGhostChildrenAndGetRemoteQueries(newDead);
        if (countingReqs.size() > 0) {
            processCountingRequests(countingReqs); //obtain the counts
            
            //merge all results
            val merged = new HashMap[DroppedQueryId, Int]();
            for (e in countingReqs.entries()) {
                val v = e.getValue();
                for (vr in v.countDropped.entries()) {
                    merged.put(vr.getKey(), vr.getValue());
                }
            }
            State.convertDeadActivities(merged);
        }
        if (TxConfig.get().TM_DEBUG) State.printTxStates();
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
        Console.OUT.println("p0FinishRecoveryTime:" + (Timer.milliTime()-start) + "ms");
    }
}