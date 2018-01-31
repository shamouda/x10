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
import x10.util.resilient.concurrent.ResilientLowLevelFinish;
import x10.util.concurrent.Condition;

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
    def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
    def spawnRemoteActivity(place:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { me.spawnRemoteActivity(place, body, prof); }
    def waitForFinish():void { me.waitForFinish(); }
    
    //create root finish
    public def this (parent:FinishState, src:Place, kind:Int) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        me = new P0OptimisticMasterState(id, parent, src, kind);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+", src="+src+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = P0OptimisticRemoteState.getOrCreateRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState, src:Place, kind:Int) {
        return new FinishResilientPlace0Optimistic(parent, src, kind);
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
    		(me as P0OptimisticMasterState).globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
    	}
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    /**
     * State of a single finish; always stored in Place0
     */
    private static final class State implements x10.io.Unserializable {
        val gfs:GlobalRef[P0OptimisticMasterState]; // root finish state
        val id:Id;
        val parentId:Id; // id of parent (UNASSIGNED means no parent / parent is UNCOUNTED)
        val finSrc:Int; //the source place that initiated the task that created this finish
        val finKind:Int; //the kind of the task that created the finish
    
        var numActive:Long = 0;
        var excs:GrowableRail[CheckedThrowable] = null;  // lazily allocated in addException
        val sent = new HashMap[Edge,Int](); //always increasing    
        var isAdopted:Boolean = false;//set to true when backup recreates a master
        
        /**Place0 states**/
        private static val states = (here.id==0) ? new HashMap[Id, State]() : null;
        private static val statesLock = (here.id==0) ? new Lock() : null;
        private static val place0 = Place.FIRST_PLACE;
        
        private def this(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState]) {
            this.id = optId.id;
            this.parentId = optId.parentId; 
            this.gfs = gfs;
            this.finSrc = optId.src;
            this.finKind = optId.kind;
            this.numActive = 1;
            sent.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        public def setLocalTaskCount(localChildrenCount:Int) {
            if (verbose>=1) debug(">>>> State(id="+id+").setLocalTaskCount called, localChildrenCount = " + localChildrenCount);
            val edge = Edge(id.home, id.home, FinishResilient.ASYNC);
            val old = sent.getOrElse(edge, 0n);
            
            if (localChildrenCount == 0n)
                sent.remove(edge);
            else
                sent.put(edge, localChildrenCount);
            if (verbose>=3) dump();
            var count:Long = 0;
            if (old != localChildrenCount)
                count = old - localChildrenCount;
            val oldActive = numActive;
            numActive -= count;
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").setLocalTaskCount returning, old["+old+"] new["+localChildrenCount+"] numActive="+oldActive +" changing numActive to " + numActive);
        }
        
        public def convertToDead(newDead:HashSet[Int], countChildren:HashMap[Id, Int]) {
            if (verbose>=1) debug(">>>> State(id="+id+").convertToDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) && edge.dst != edge.src ) {
                    val t1 = e.getValue();
                    val t2 = countChildren.get(id);
                    assert t1 > 0 : here + " State(id="+id+").convertToDead FATAL error, t1 must be positive";
                    if (t1 >= t2) {
                        val count = t1-t2;
                        val oldActive = numActive;
                        numActive -= count;
                        
                        if (verbose>=1) debug("==== State(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
                        assert numActive >= 0 : here + " State(id="+id+").convertToDead FATAL error, numActive must not be negative";
                        if (edge.kind == ASYNC) {
                            for (1..count) {
                                if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                                val dpe = new DeadPlaceException(Place(edge.dst));
                                dpe.fillInStackTrace();
                                addException(dpe);
                            }
                        }
                    }
                    else assert false: here + " State(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met";
                }
            }
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[ReceivedQueryId, Int]) {
            if (verbose>=1) debug(">>>> State(id="+id+").convertFromDead called");
            /*val toRemove = new HashSet[Edge]();*/
            for (e in sent.entries()) {
                val edge = e.getKey();
                var trns:Int = e.getValue();
                if (newDead.contains(edge.src) && edge.src != edge.dst ) {
                    val sent = sent.get(edge);
                    val rcvId = ReceivedQueryId(id, edge.src, edge.dst, edge.kind);
                    val received = countReceived.get(rcvId);
                    assert sent > 0 && sent >= received : here + " State(id="+id+").convertFromDead FATAL error, transit["+trns+"] sent["+sent+"] received["+received+"]";

                    val dropedMsgs = sent - received;
                    val oldActive = numActive;
                    numActive -= dropedMsgs;
                    trns -= dropedMsgs;
                    if (verbose>=1) debug("==== State(id="+id+").convertFromDead src="+edge.src+" dst="+edge.dst+" transit["+trns+"] sent["+sent+"] received["+received+"] numActive["+oldActive+"] dropedMsgs["+dropedMsgs+"] changing numActive to " + numActive);
                    
                    assert numActive >= 0 : here + " State(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").convertFromDead returning, numActive="+numActive);
        }
        
        static def countLocalChildren(id:Id, states:HashSet[State]) {
            if (verbose>=1) debug(">>>> countLocalChildren(id="+id+") called");
            var count:Int = 0n;
            for (s in states) {
                if (s.parentId == id && s.id.home == s.parentId.home) 
                    count++;
            }
            if (verbose>=1) debug("<<<< countLocalChildren(id="+id+") returning, count = " + count);
            return count;
        }
        
        static def convertDeadActivities(newDead:HashSet[Int], states:HashSet[State], countChildren:HashMap[Id, Int], 
                countReceived:HashMap[ReceivedQueryId, Int]) {
            try {
                statesLock.lock();
                for (state in states) {
                    if (state.numActive > 0) {
                        if (newDead.contains(state.id.home)) {
                            val localChildren = countLocalChildren(state.id, states);
                            state.setLocalTaskCount(localChildren);
                        }
                        if (state.numActive > 0) {
                            state.convertToDead(newDead, countChildren);
                            if (state.numActive > 0)
                                state.convertFromDead(newDead, countReceived);
                        }
                    }
                }
            } finally {
                statesLock.unlock();
            }
        }
        
        def isImpactedByDeadPlacesUnsafe(newDead:HashSet[Int]) {
            if (newDead.contains(id.home)) {
                if (verbose>=1) debug("<<<< isImpactedByDeadPlacesUnsafe(id="+id+") true");
                return true;
            }
            for (e in sent.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.src) || newDead.contains(edge.dst)) {
                    if (verbose>=1) debug("<<<< isImpactedByDeadPlacesUnsafe(id="+id+") true");
                    return true;
                }
            }
            if (verbose>=1) debug("<<<< isImpactedByDeadPlacesUnsafe(id="+id+") false");
            return false;
        }
        
        static def getImpactedStates(newDead:HashSet[Int]) {
            try {
                statesLock.lock();
                val result = new HashSet[State]();
                for (e in states.entries()) {
                    val id = e.getKey();
                    val state = e.getValue();
                    if (state.isImpactedByDeadPlacesUnsafe(newDead)) {
                        result.add(state);
                    }
                }
                return result;
            } finally {
                statesLock.unlock();
            }
        }
        
        static def aggregateCountingRequests(newDead:HashSet[Int], states:HashSet[State]) {
            val countingReqs = new HashMap[Int,OptResolveRequest]();
            val place0 = 0 as Int;
            if (!states.isEmpty()) {
                try {
                    statesLock.lock();
                    for (dead in newDead) {
                        for (s in states) {
                            for (entry in s.sent.entries()) {
                                val edge = entry.getKey();
                                if (dead == edge.dst) {
                                    var rreq:OptResolveRequest = countingReqs.getOrElse(place0, null);
                                    if (rreq == null){
                                        rreq = new OptResolveRequest();
                                        countingReqs.put(place0, rreq);
                                    }
                                    rreq.countChildren.put(ChildrenQueryId(s.id /*parent id*/, dead), -1n);
                                } else if (dead == edge.src) {
                                    var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                                    if (rreq == null){
                                        rreq = new OptResolveRequest();
                                        countingReqs.put(edge.dst, rreq);
                                    }
                                    rreq.countReceived.put(ReceivedQueryId(s.id, dead, edge.dst, edge.kind), -1n);
                                }
                            }
                        }
                    }
                } finally {
                    statesLock.unlock();
                }
            }
            if (verbose >=1) printResolveReqs(countingReqs); 
            return countingReqs;
        }
        
        static def countChildren(parentId:Id, src:Int) {
            var count:Int = 0n;
            try {
                statesLock.lock();
                for (e in states.entries()) {
                    if (e.getValue().parentId == parentId)
                        count++;
                }
                //no more states under this parent from that src should be created
                if (verbose>=1) debug("<<<< countChildren(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
            } finally {
                statesLock.unlock();
            }
            return count;
        }
        
        //P0FUNC
        private static final def getOrCreateState(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState]):State {
            var state:State = states(optId.id);
            if (state == null) {
                if (Place(optId.id.home).isDead()) {
                    if (verbose>=1) debug("<<<< getOrCreateState(id="+optId.id+") failed, state from dead src["+optId.id.home+"] denied");
                    throw new Exception("state creation from a dead src denied");
                    //no need to handle this exception; the caller has died.
                }
                if (verbose>=1) debug(">>>> getOrCreateState(id="+optId.id+") initializing state ");
                state = new State(optId, gfs);
                if (verbose>=1) debug(">>>> creating new State id="+optId.id +" parentId="+optId.parentId);
                states.put(optId.id, state);
            }
            return state;
        }
        
        static def p0CreateState(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState]) {
            Runtime.runImmediateAt(place0, ()=>{ 
                try {
                    statesLock.lock();
                    if (verbose>=1) debug(">>>> creating new State id="+optId.id +" parentId="+optId.parentId);
                    states.put(optId.id, new State(optId, gfs));
                } finally {
                    statesLock.unlock();
                }
            });
        }
        
        //called by a remote finish only - the state must be existing at place0
        static def p0TransitGlobal(id:Id, srcId:Int, dstId:Int, kind:Int) {
            Runtime.runImmediateAt(place0, ()=>{ 
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                } else if (Place(dstId).isDead()) {
                    if (kind == ASYNC) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                        try {
                            statesLock.lock();
                            states(id).addDeadPlaceException(dstId);
                        } finally {
                            statesLock.unlock();
                        }
                    } else {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                    }
                } else {
                    try {
                        statesLock.lock();
                        states(id).inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                    } finally {
                        statesLock.unlock();
                    }
                }
            });
        }
        
        static def p0RemoteSpawnSmallGlobal(id:Id, srcId:Int, dstId:Int, bytes:Rail[Byte],
        		gfs:GlobalRef[FinishState]) {
            at (place0) @Immediate("p0optGlobal_spawnRemoteActivity_to_zero") async {
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                } else {
                    try {
                    	statesLock.lock();
                        val state = states(id);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(small async)");
                        }
                    } finally {
                    	statesLock.unlock();
                    }
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
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") src "+srcId + "is dead; dropping async");
                } else {
                    try {
                    	statesLock.lock();
                        val state = states(id);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(large async)");
                            markedInTransit = true;
                        }
                    } finally {
                    	statesLock.unlock();
                    }
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
        
        static def p0RemoteSpawnSmall(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, bytes:Rail[Byte],
        		ref:GlobalRef[FinishState]) {
            at (place0) @Immediate("p0opt_spawnRemoteActivity_to_zero") async {
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== spawnRemoteActivity(id="+optId.id+") src "+srcId + "is dead; dropping async");
                } else {
                    try {
                    	statesLock.lock();
                    	val state = getOrCreateState(optId, gfs);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+optId.id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(small async)");
                        }
                    } finally {
                    	statesLock.unlock();
                    }
                }
                                
                try {
                    at (Place(dstId)) @Immediate("p0opt_spawnRemoteActivity_dstPlace") async {
                        if (verbose >= 1) debug("==== spawnRemoteActivity(id="+optId.id+") submitting activity from "+here.id+" at "+dstId);
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
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_dstPlace for "+optId.id);
                }
            }
        }
        
        static def p0RemoteSpawnBig(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, bytes:Rail[Byte],
        		ref:GlobalRef[FinishState]) {
            val wrappedBody = ()=> @AsyncClosure {
                val deser = new Deserializer(bytes);
                val bodyPrime = deser.readAny() as ()=>void;
                bodyPrime();
            };
            val wbgr = GlobalRef(wrappedBody);
            at (place0) @Immediate("p0opt_spawnRemoteActivity_big_async_to_zero") async {
                var markedInTransit:Boolean = false;
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== spawnRemoteActivity(id="+optId.id+") src "+srcId + "is dead; dropping async");
                } else {
                    try {
                    	statesLock.lock();
                    	val state = getOrCreateState(optId, gfs);
                        if (Place(dstId).isDead()) {
                            if (verbose>=1) debug("==== spawnRemoteActivity(id="+optId.id+") destination "+dstId + "is dead; pushed DPE");
                            state.addDeadPlaceException(dstId);
                        } else {
                            state.inTransit(srcId, dstId, ASYNC, "spawnRemoteActivity(large async)");
                            markedInTransit = true;
                        }
                    } finally {
                    	statesLock.unlock();
                    }
                }
                try {
                    val mt = markedInTransit;
                    at (wbgr) @Immediate("p0opt_spawnRemoteActivity_big_back_to_spawner") async {
                        val fs = (ref as GlobalRef[FinishState]{self.home == here})();
                        try {
                            if (mt) x10.xrx.Runtime.x10rtSendAsync(dstId, wbgr(), fs, null, null);
                        } catch (dpe:DeadPlaceException) {
                            // not relevant to immediate thread; DPE raised in convertDeadActivities
                            if (verbose>=2) debug("caught and suppressed DPE from x10rtSendAsync from spawnRemoteActivity_big_back_to_spawner for "+optId.id);
                        }
                        wbgr.forget();
                        fs.notifyActivityTermination(Place(srcId));
                    }
                } catch (dpe:DeadPlaceException) {
                    // can ignore; if the src place just died there is nothing left to do.
                    if (verbose>=2) debug("caught and suppressed DPE when attempting spawnRemoteActivity_big_back_to_spawner for "+optId.id);
                }
            }
        }
        
        //called by root finish - may need to create the State object
        static def p0Transit(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, kind:Int) {
            Runtime.runImmediateAt(place0, ()=>{ 
                if (Place(srcId).isDead()) {
                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+optId.id+") src "+srcId + "is dead; dropping async");
                } else if (Place(dstId).isDead()) {
                    if (kind == ASYNC) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+optId.id+") destination "+dstId + "is dead; pushed DPE for async");
                        try {
                            statesLock.lock();
                            val state = getOrCreateState(optId, gfs);
                            state.addDeadPlaceException(dstId);
                        } finally {
                            statesLock.unlock();
                        }
                    } else {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+optId.id+") destination "+dstId + "is dead; dropped at");
                    }
                } else {
                    try {
                        statesLock.lock();
                        val state = getOrCreateState(optId, gfs);
                        state.inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                    } finally {
                        statesLock.unlock();
                    }
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
        
        static def p0PushException(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState], t:CheckedThrowable) {
            Runtime.runImmediateAt(place0, ()=>{ 
                try {
                    statesLock.lock();
                    val state = getOrCreateState(optId, gfs);
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
                    //Unlike place0 pessimistic finish, we don't suppress termination notifications whose dst is dead.
                    //We actually wait for these termination messages to come from (kind of) adopted children
                    statesLock.lock();
                    if (verbose>=1) debug(">>>> State.p0TransitToCompletedGlobal(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") called");
                    states(id).transitToCompleted(srcId, dstId, kind, t);
                    if (verbose>=1) debug("<<<< State.p0TransitToCompletedGlobal(id="+id+", srcId="+srcId+", dstId="+dstId+",t="+t+") returning");
                } finally {
                    statesLock.unlock();
                }
            }
        }
        
        static def p0TransitToCompleted(optId:OptimisticRootId, gfs:GlobalRef[P0OptimisticMasterState], srcId:Int, dstId:Int, kind:Int, t:CheckedThrowable) {
            at (place0) @Immediate("p0Opt_notifyActivityCreationFailed_to_zero") async {
                //Unlike place0 pessimistic finish, we don't suppress termination notifications whose dst is dead.
                //We actually wait for these termination messages to come from (kind of) adopted children
                try {
                    statesLock.lock();
                    val state = getOrCreateState(optId, gfs);
                    if (verbose>=1) debug(">>>> State.p0TransitToCompleted(id="+optId.id+", srcId="+srcId+", dstId="+dstId+",t="+t+") called");
                    state.transitToCompleted(srcId, dstId, kind, t);
                    if (verbose>=1) debug("<<<< State.p0TransitToCompleted(id="+optId.id+", srcId="+srcId+", dstId="+dstId+",t="+t+") returning");
                } finally {
                    statesLock.unlock();
                }
           }
        }
        
        static def p0TermMultiple(id:Id, dstId:Int, map:HashMap[Task,Int]) {
            at (place0) @Immediate("p0Opt_notifyTermMul_to_zero") async {
                if (verbose>=1) debug(">>>> State(id="+id+").p0TermMultiple [dstId=" + dstId +", mapSz="+map.size()+" ] called");
                //Unlike place0 finish, we don't suppress termination notifications whose dst is dead.
                //Because we expect termination messages from these tasks to be notified if the tasks were recieved by a dead dst
                try {
                    statesLock.lock();
                    val state = states(id);
                    for (e in map.entries()) {
                        val srcId = e.getKey().place;
                        val kind = e.getKey().kind;
                        val cnt = e.getValue();
                        state.transitToCompletedMul(srcId, dstId, kind, cnt);
                    }
                } finally {
                    statesLock.unlock();
                }
                if (verbose>=1) debug("<<<< State(id="+id+").p0TermMultiple [dstId=" + dstId +", mapSz="+map.size()+" ] returning");
           }
        }
        
        def addException(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
            if (verbose>=1) debug(">>>> State(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            //increment(transit, e);
            increment(sent, e);
            numActive++;
            if (verbose>=3) debug("==== State(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
            if (verbose>=3) dump();
            if (verbose>=1) debug("<<<< State(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable) {
            if (verbose>=1) debug(">>>> State(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            /*decrement(transit, e);
            assert transit.getOrElse(e, 0n) >= 0n : here + " FATAL error, transit reached negative id="+id;*/
            //don't decrement 'sent' unless src=dst
            if (srcId == dstId) /*signalling the termination of the root finish task, needed for correct counting of localChildren*/
                decrement(sent, e);
            numActive--;
            assert numActive>=0 : here + " FATAL error, State(id="+id+").numActive reached -ve value";
            if (t != null) addException(t);
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def transitToCompletedMul(srcId:Long, dstId:Long, kind:Int, cnt:Int) {
            if (verbose>=1) debug(">>>> State(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            /*deduct(transit, e, cnt);
            assert transit.getOrElse(e, 0n) >= 0n : here + " FATAL error, transit reached negative id="+id;*/
            //don't decrement 'sent' unless srcId = dstId
            if (srcId == dstId) /*signalling the termination of the root finish task*/
                deduct(sent, e, cnt);
            numActive-=cnt;
            assert numActive>=0 : here + " FATAL error, State(id="+id+").numActive reached -ve value";
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< State(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
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
            if (gfs.home.isDead()) { //isAdopted
                if (verbose>=1) debug("releaseLatch(id="+id+") called on lost finish; not releasing latch");
            } else {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                if (verbose>=2) debug("releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));

                val mygfs = gfs;
                try {
                    at (mygfs.home) @Immediate("p0Opt_releaseLatch_gfs_home") async {
                        val fs = mygfs();
                        if (verbose>=2) debug("performing release for "+fs.optId.id+" at "+here);
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
        
        def notifyParent() {
            if (!gfs.home.isDead()/*isAdopted*/ || parentId == FinishResilient.UNASSIGNED) {
                if (verbose>=1) debug("<<< State(id="+id+").notifyParent returning, not lost or parent["+parentId+"] does not exist");
                return;
            }
            val srcId = finSrc;
            val dstId = id.home;
            val dpe:CheckedThrowable;
            if (finKind == FinishResilient.ASYNC) {
                dpe = new DeadPlaceException(Place(dstId));
                dpe.fillInStackTrace();
            } else {
                dpe = null;
            }
            if (verbose>=1) debug(">>>> State(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") called");
            val parentState = states(parentId);
            parentState.transitToCompleted(srcId, dstId, finKind, dpe);
            if (verbose>=1) debug("<<<< State(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+finKind+") returning");
        }

        def removeFromStates() {
            states.remove(id);
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
                s.add("        sent:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
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
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        public static def countReceived(id:Id, src:Int, kind:Int) {
            if (verbose>=1) debug(">>>> countReceived(id="+id+", src="+src+", kind="+kind+") called");
            var count:Int = 0n;
            try {
                remoteLock.lock();
                val remote = remotes.getOrElse(id, null);
                if (remote != null) {
                    count = remote.receivedFrom(src, kind);
                } else {
                    //no remote finish for this id should be created here
                    remoteDeny.add(id);
                }
            } finally {
                remoteLock.unlock();
            }
            if (verbose>=1) debug("<<<< countReceived(id="+id+", src="+src+", kind="+kind+") returning, count="+count);
            return count;
        }
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:P0OptimisticRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
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
        
        //Calculates the delta between received and reported
        private def getReportMap() {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMap called");
                var map:HashMap[Task,Int] = new HashMap[Task,Int]();
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
                        map.put(t, rec - rep);
                        reported.put (t, rec);
                        if (verbose>=1) debug("==== Remote(id="+id+").getReportMap Task["+t+"] reported.put("+t+","+(rec-rep)+")");
                    }
                }
                if (map.size() == 0) /*because notifyTerminationAndGetMap doesn't decrement lc and get the map as a one atomic action, */ 
                    map=null;        /*it is possible that two activities reach zero lc and then one of them reports all of the activities, while the other finds no activity to report */
                if (verbose>=1) printMap(map);
                if (verbose>=3) dump();
                if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMap returning");
                return map;
            } finally {
                ilock.unlock();
            }
        }
        
        public def notifyTerminationAndGetMap(t:Task) {
            var map:HashMap[Task,Int] = null;
            val count = lc.decrementAndGet();
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called, taskFrom["+t.place+"] lc="+count);
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
                if (verbose >= 1) debug("==== Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting indirect (size="+ bytes.size+") ");
            	val count = notifyReceived(Task(srcId, ASYNC)); // synthetic activity to keep finish locally live during async to Place0
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
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
        }
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val count = notifyReceived(Task(srcId, ASYNC));
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }
            
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root, mapSize=" + map.size());
            State.p0TermMultiple(id, dstId, map);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            State.p0PushExceptionGlobal(id, t);
            if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+") returning");
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
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called ");
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null)
                return;
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root, mapSize=" + map.size());
            State.p0TermMultiple(id, dstId, map);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
    }
    
    //ROOT
    public static final class P0OptimisticMasterState extends FinishResilient implements x10.io.Unserializable {
        val ref:GlobalRef[P0OptimisticMasterState] = GlobalRef[P0OptimisticMasterState](this);
        val optId:OptimisticRootId;
        val parent:FinishState;//the direct parent finish object (used in globalInit for recursive initializing)
        val latch = new SimpleLatch();//latch for blocking and releasing the host activity
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        val lc = new AtomicInteger(1n);
        
        def this(id:Id, parent:FinishState, src:Place, kind:Int) {
            this.parent = parent;
            if (parent instanceof FinishResilientPlace0Optimistic) {
                val parentId = (parent as FinishResilientPlace0Optimistic).id;
                optId = OptimisticRootId(id, parentId, src.id as Int, kind);
            } else {
                optId = OptimisticRootId(id, UNASSIGNED, src.id as Int, kind);
            }
        }
        
        def globalInit(createState:Boolean) {
            latch.lock();
            strictFinish = true;
            if (!isGlobal) {
                if (verbose>=1) debug(">>>> globalInit(id="+optId.id+") called");
                val parent = parent;
                if (parent instanceof FinishResilientPlace0Optimistic) {
                    val frParent = parent as FinishResilientPlace0Optimistic;
                    if (frParent.me instanceof P0OptimisticMasterState) (frParent.me as P0OptimisticMasterState).globalInit(true);
                }
                if (createState) {
                    State.p0CreateState(optId, ref);
                }
                isGlobal = true;
                if (verbose>=1) debug("<<<< globalInit(id="+optId.id+") returning");
            }
            latch.unlock();
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
            if (verbose>=1) debug("<<<< addExceptionUnsafe(id="+optId.id+") t="+t.getMessage() + " exceptions size = " + excs.size());
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
                    if (verbose>=1) debug(">>>> localFinishExceptionPushed(id="+optId.id+") true");
                    addExceptionUnsafe(t);
                    return true;
                } 
                if (verbose>=1) debug("<<<< localFinishExceptionPushed(id="+optId.id+") false: global finish");
                return false;
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
                val count = lc.incrementAndGet();
                if (verbose>=1) debug(">>>> Root(id="+optId.id+").notifySubActivitySpawn(parentId="+optId.parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+count);
            } else {
                if (verbose>=1) debug(">>>> Root(id="+optId.id+").notifySubActivitySpawn(parentId="+optId.parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            	State.p0Transit(optId, ref, srcId, dstId, kind);
                if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifySubActivitySpawn(parentId="+optId.parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void {
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") called");
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
            	val count = lc.incrementAndGet(); // synthetic activity to keep finish locally live during async to Place0
            	if (verbose >= 1) debug("==== Root(id="+optId.id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting indirect (size="+ bytes.size+") localCount now " + count);
            	State.p0RemoteSpawnBig(optId, ref, srcId, dstId, bytes, gfs);
            } else {
                if (verbose >= 1) debug("====  Root(id="+optId.id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+"), selecting direct (size="+ bytes.size+") ");
            	State.p0RemoteSpawnSmall(optId, ref, srcId, dstId, bytes, gfs);
            }
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+") returning");
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyShiftedActivityCreation(srcId=" + srcId +") returning");
            return true;
        }
        
        def notifyRemoteContinuationCreated():void { 
            strictFinish = true;
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyRemoteContinuationCreated() isGlobal = "+isGlobal);
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            State.p0TransitToCompleted(optId, ref, srcId, dstId, kind, t);
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") called");
            //no need to call notifyActivityCreation() since it does NOOP in Root finish
            notifyActivityTermination(srcPlace, ASYNC);
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                if (verbose>=1) debug("<<<< Root(id="+optId.id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").pushException(t="+t.getMessage()+") called");
            State.p0PushException(optId, ref, t);
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").pushException(t="+t.getMessage()+") returning");
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
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") called, decremented localCount to "+count);
            if (count > 0) {
                return;
            }
            assert count == 0n : here + " FATAL ERROR: Root(id="+optId.id+").notifyActivityTermination reached a negative local count";

            if (!isGlobal) { //only one activity is here, no need to lock/unlock latch
                if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                latch.release();
                return;
            }
            if (verbose>=1) debug("==== Root(id="+optId.id+").notifyActivityTermination(parentId="+optId.parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            State.p0TransitToCompleted(optId, ref, srcId, dstId, kind, null);
            if (verbose>=1) debug("<<<< Root(id="+optId.id+").notifyActivityTermination(parentId="+optId.parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }

        private def forgetGlobalRefs():void {
            (ref as GlobalRef[P0OptimisticMasterState]{self.home == here}).forget();
        }
        
        def waitForFinish():void {
            if (verbose>=1) debug(">>>> Root(id="+optId.id+").waitForFinish called, lc = " + lc );

            // terminate myself
            notifyActivityTermination(here, ASYNC);

            // If we haven't gone remote with this finish yet, see if this worker
            // can execute other asyncs that are governed by the finish before waiting on the latch.
            if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS || !strictFinish)) {
                if (verbose>=2) debug("calling worker.join for id="+optId.id);
                Runtime.worker().join(this.latch);
            }

            // wait for the latch release
            if (verbose>=2) debug("calling latch.await for id="+optId.id);
            latch.await(); // wait for the termination (latch may already be released)
            if (verbose>=2) debug("returned from latch.await for id="+optId.id);

            // no more messages will come back to this finish state 
            forgetGlobalRefs();
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("<<<< Root(id="+optId.id+").waitForFinish returning with exceptions, size=" + excs.size() );
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
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val outputGr = GlobalRef[HashMap[Int,OptResolveRequest]](countingReqs);
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                val requests = countingReqs.getOrThrow(p);
                if (verbose>=1) debug("==== processCountingRequests  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("counting_request") async {
                        if (verbose>=1) debug("==== processCountingRequests  reached from " + gr.home + " to " + here);
                        val countChildren = requests.countChildren ;
                        val countReceived = requests.countReceived;
                        if (countChildren.size() > 0) {
                            for (b in countChildren.entries()) {
                                val parentId = b.getKey().parentId;
                                val src = b.getKey().src;
                                val count = State.countChildren(parentId, src);
                                countChildren.put(b.getKey(), count);   
                            }
                        }
                        
                        if (countReceived.size() > 0) {
                            for (r in countReceived.entries()) {
                                val key = r.getKey();
                                val count = P0OptimisticRemoteState.countReceived(key.id, key.src, key.kind);
                                countReceived.put(key, count);
                            }
                        }
                        
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== processCountingRequests  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("counting_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,OptResolveRequest]]{self.home == here})().getOrThrow(me);
                            
                            for (ve in countChildren.entries()) {
                                output.countChildren.put(ve.getKey(), ve.getValue());
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
        val states = State.getImpactedStates(newDead);
        if (states.isEmpty()) {
            if (verbose>=2) debug(">>>> notifyPlaceDeath returning, no impacted states");
        }
        
        val countingReqs = State.aggregateCountingRequests(newDead, states); // combine all requests targetted to a specific place
        processCountingRequests(countingReqs); //obtain the counts
        
        //merge all results
        val countChildren = new HashMap[Id, Int]();
        val countReceived = new HashMap[ReceivedQueryId, Int]();
        for (e in countingReqs.entries()) {
            val v = e.getValue();
            for (ve in v.countChildren .entries()) {
                countChildren.put(ve.getKey().parentId, ve.getValue());
            }
            for (vr in v.countReceived.entries()) {
                countReceived.put(vr.getKey(), vr.getValue());
            }
        }
        
        State.convertDeadActivities(newDead, states, countChildren, countReceived);
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}
