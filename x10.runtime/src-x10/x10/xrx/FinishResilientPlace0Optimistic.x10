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

/**
 * Place0-based Resilient Finish using the optimistic counting protocol
 */
class FinishResilientPlace0Optimistic extends FinishResilient implements CustomSerialization {
    private static val DUMMY_INT = -1n;
    protected transient var me:FinishResilient; // local finish object
    val id:Id;
    private static val remotes = new HashMap[Id, P0OptimisticRemoteState](); //a cache for remote finish objects
    private static val remoteDeny = new HashSet[Id](); //remote deny list
    
    private static val states = (here.id==0) ? new HashMap[Id, State]() : null;
    public static statesLock = (here.id==0) ? new Lock() : null;
    private static val place0 = Place.FIRST_PLACE;
    
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
        me = getRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState, src:Place, kind:Int) {
        return new FinishResilientPlace0Optimistic(parent, src, kind);
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }

    private static def countReceived(id:Id, src:Int, kind:Int) {
        if (verbose>=1) debug(">>>> countReceived(id="+id+", src="+src+", kind="+kind+") called");
        var count:Int = 0n;
        try {
            FinishResilient.glock.lock();
            val remote = remotes.getOrElse(id, null);
            if (remote != null) {
                count = remote.countReceived(src, kind);
            } else {
                //no remote finish for this id should be created here
                remoteDeny.add(id);
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        if (verbose>=1) debug("<<<< countReceived(id="+id+", src="+src+", kind="+kind+") returning, count="+count);
        return count;
    }
    
    private static def getRemote(id:Id) {
        try {
            FinishResilient.glock.lock();
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
            FinishResilient.glock.unlock();
        }
    }
    
    private def globalInit(createState:Boolean) {
        if (me instanceof P0OptimisticMasterState) {
            val rootState = me as P0OptimisticMasterState;
            rootState.latch.lock();
            rootState.strictFinish = true;
            if (!rootState.isGlobal) {
                if (verbose>=1) debug(">>>> globalInit(id="+id+") called");
                val parent = rootState.parent;
                if (parent instanceof FinishResilientPlace0Optimistic) {
                    val frParent = parent as FinishResilientPlace0Optimistic;
                    if (frParent.me instanceof P0OptimisticMasterState) (frParent as FinishResilientPlace0Optimistic).globalInit(true);
                }
                if (createState){
                    val gfs = rootState.ref;
                    val myId = this.id;
                    val src = rootState.optSrc;
                    val kind = rootState.optKind;
                    val parentId = rootState.parentId;
                    Runtime.runImmediateAt(place0, ()=>{ 
                        try {
                        	statesLock.lock();
                            val state = new State(myId, parentId, gfs, src, kind);
                            //NOLOG if (verbose>=1) debug(">>>> creating new State id="+myId +" parentId="+parentId);
                            states.put(myId, state);
                        } finally {
                        	statesLock.unlock();
                        }
                    });
                }
                rootState.isGlobal = true;
                if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
            }
            rootState.latch.unlock();
        }
    }
    
    /**
     * State of a single finish; always stored in Place0
     */
    private static final class State implements x10.io.Unserializable {
        val gfs:GlobalRef[P0OptimisticMasterState]; // root finish state
        val id:Id;
        val parentId:Id; // id of parent (UNASSIGNED means no parent / parent is UNCOUNTED)
    	val optSrc:Place; //the source place that initiated the task that created this finish
		val optKind:Int; //the kind of the task that created the finish
    
        var numActive:Long = 0;
        var excs:GrowableRail[CheckedThrowable] = null;  // lazily allocated in addException
        val sent = new HashMap[Edge,Int](); //always increasing    
        /*val transit:HashMap[Edge,Int];*/ // increases and decreases - not needed except for debugging
        var isAdopted:Boolean = false;//set to true when backup recreates a master
        
        private def this(id:Id, parentId:Id, gfs:GlobalRef[P0OptimisticMasterState],
        		src:Place, kind:Int) {
            this.id = id;
            this.parentId = parentId; 
            this.gfs = gfs;
            this.optSrc = src;
            this.optKind = kind;
            this.numActive = 1;
            FinishResilient.increment(sent, FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC));
        }
        
        def addException(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
            if (verbose>=1) debug(">>>> Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            //increment(transit, e);
            increment(sent, e);
            numActive++;
            if (verbose>=3) debug("==== Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
            if (verbose>=3) dump();
            if (verbose>=1) debug("<<<< Master(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            /*decrement(transit, e);
            assert transit.getOrElse(e, 0n) >= 0n : here + " FATAL error, transit reached negative id="+id;*/
            //don't decrement 'sent' unless src=dst
            if (srcId == dstId) /*signalling the termination of the root finish task, needed for correct counting of localChildren*/
                decrement(sent, e);
            numActive--;
            assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
            if (t != null) addException(t);
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def transitToCompletedMul(srcId:Long, dstId:Long, kind:Int, cnt:Int, tag:String) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            /*deduct(transit, e, cnt);
            assert transit.getOrElse(e, 0n) >= 0n : here + " FATAL error, transit reached negative id="+id;*/
            //don't decrement 'sent' unless srcId = dstId
            if (srcId == dstId) /*signalling the termination of the root finish task*/
                deduct(sent, e, cnt);
            numActive-=cnt;
            assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Master(id="+id+").quiescent called");
            if (numActive < 0) {
                debug("COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!");
                dump();
                assert false : "COUNTING ERROR: Master(id="+id+").quiescent negative numActive!!!";
                return true; 
            }
        
            val quiet = numActive == 0;
            if (verbose>=3) dump();
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Master(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        def releaseLatch() {
            if (isAdopted) {
                //NOLOG if (verbose>=1) debug("releaseLatch(id="+id+") called on adopted finish; not releasing latch");
            } else {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                //NOLOG if (verbose>=2) debug("releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));

                val mygfs = gfs;
                try {
                    at (mygfs.home) @Immediate("p0Opt_releaseLatch_gfs_home") async {
                        val fs = mygfs();
                        //NOLOG if (verbose>=2) debug("performing release for "+fs.id+" at "+here);
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
                    //NOLOG if (verbose>=2) debug("caught and suppressed DPE when attempting to release latch for "+id);
                }
            }
            //NOLOG if (verbose>=2) debug("releaseLatch(id="+id+") returning");
        }
        
        def notifyParent() {
            if (!isAdopted || parentId == FinishResilient.UNASSIGNED) {
                if (verbose>=1) debug("<<< Master(id="+id+").notifyParent returning, not adopted or parent["+parentId+"] does not exist");
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
            if (verbose>=1) debug(">>>> Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") called");
            val parentState = states(parentId);
            parentState.transitToCompleted(srcId, dstId, optKind, dpe);
            if (verbose>=1) debug("<<<< Master(id="+id+").notifyParent(srcId=" + srcId + ",dstId="+dstId+",kind="+optKind+") returning");
        }

        def removeFromStates() {
            states.remove(id);
        }

        def addDeadPlaceException(placeId:Long) {
            val e = new DeadPlaceException(Place(placeId));
            e.fillInStackTrace();
            addException(e);
        }

        /*def seekAdoption() {
            if (isAdopted() || !gfs.home.isDead()) return;

            var adopterState:State = states(parentId);
            while (adopterState != null) {
                if (!adopterState.gfs.home.isDead()) break; // found first live ancestor
                adopterState = states(adopterState.parentId);
            }

            if (adopterState == null) {
                //NOLOG if (verbose>=1) debug ("==== seekAdoption "+id+" is becoming an orphan; no live ancestor");
                return;
            }

            //NOLOG if (verbose>=1) debug ("==== seekAdoption "+id+" will be adopted by "+adopterState.id);
            //NOLOG if (verbose>=3) debug("==== seekAdoption: dumping states before adoption");
            //NOLOG if (verbose>=3) dump();
            //NOLOG if (verbose>=3) adopterState.dump();

            val asla = adopterState.liveAdopted();
            for (entry in live.entries()) {
                val task = entry.getKey();
                asla.put(task, asla.getOrElse(task,0n) + entry.getValue());
            }
            if (_liveAdopted != null) {
                for (entry in _liveAdopted.entries()) {
                    val task = entry.getKey();
                    asla.put(task, asla.getOrElse(task,0n) + entry.getValue());
                }
            }

            if (_transit != null || _transitAdopted != null) {
                val asta = adopterState.transitAdopted();
                if (_transit != null) {
                    for (entry in _transit.entries()) {
                        val edge = entry.getKey();
                        asta.put(edge, asta.getOrElse(edge,0n) + entry.getValue());
                    }
                    _transit = null;
                }
                if (_transitAdopted != null) {
                    for (entry in _transitAdopted.entries()) {
                        val edge = entry.getKey();
                        asta.put(edge, asta.getOrElse(edge,0n) + entry.getValue());
                    }
                }
                _transitAdopted = null;
            }

            adopterState.numActive += numActive;

            if (adopterState.adoptees == null) adopterState.adoptees = new GrowableRail[Id]();
            adopterState.adoptees.add(id);
            adopterId = adopterState.id;
            if (adoptees != null) {
                for (w in adoptees.toRail()) {
                    adopterState.adoptees.add(w);
                    states(w).adopterId = adopterState.id;
                    //NOLOG if (verbose>=2) debug ("==== seekAdoption "+id+" transfered ward "+w+" to "+adopterState.id);
                }
                adoptees = null;
            }
    
            //NOLOG if (verbose>=3) debug("==== seekAdoption: dumping adopter state after adoption");
            //NOLOG if (verbose>=3) adopterState.dump();
        }

        def convertDeadActivities() {
            if (isAdopted()) return;

            // NOTE: can't say for (p in Place.places()) because we need to see the dead places
            for (i in 0n..((Place.numPlaces() as Int) - 1n)) {
                if (!Place.isDead(i)) continue;

                val deadTasks = new HashSet[Task]();
                for (k in live.keySet()) {
                    if (k.place == i) deadTasks.add(k);
                }
                for (dt in deadTasks) {
                    val count = live.remove(dt);
                    numActive -= count;
                    if (dt.kind == ASYNC) {
                        for (1..count) {
                            //NOLOG if (verbose>=3) debug("adding DPE to "+id+" for live async at "+i);
                            addDeadPlaceException(i);
                        }
                    }
                }

                if (_liveAdopted != null) {
                    val deadWards = new HashSet[Task]();
                    for (k in _liveAdopted.keySet()) {
                        if (k.place == i) deadWards.add(k);
                    }
                    for (dw in deadWards) {
                        val count = _liveAdopted.remove(dw);
                        numActive -= count;
                    }
                }
                  
                if (_transit != null) {
                    val deadEdges = new HashSet[Edge]();
                    for (k in _transit.keySet()) {
                        if (k.src == i || k.dst == i) deadEdges.add(k);
                    }
                    for (de in deadEdges) {
                        val count = _transit.remove(de);
                        numActive -= count;
                        if (de.kind == ASYNC && de.dst == i) {
                            for (1..count) {
                                //NOLOG if (verbose>=3) debug("adding DPE to "+id+" for transit asyncs("+de.src+","+i+")");
                                addDeadPlaceException(i);
                            }
                        }
                    }
                }

                if (_transitAdopted != null) {
                    val deadEdges = new HashSet[Edge]();
                    for (k in _transitAdopted.keySet()) {
                        if (k.src == i || k.dst == i) deadEdges.add(k);
                    }
                    for (de in deadEdges) {
                        val count = _transitAdopted.remove(de);
                        numActive -= count;
                    }
                }
            }
        }*/

        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Root dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            /*if (transit.size() > 0) {
                s.add("        transit:\n"); 
                for (e in transit.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }*/
            if (sent.size() > 0) {
                s.add("        sent:\n"); 
                for (e in sent.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
        /*public def convertToDead(newDead:HashSet[Int], countBackups:HashMap[FinishResilient.Id, Int]) {
            if (verbose>=1) debug(">>>> Master(id="+id+").convertToDead called");
            //val toRemove = new HashSet[Edge]();
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
                        
                        if (verbose>=1) debug("==== Master(id="+id+").convertToDead t1["+t1+"] t2["+t2+"] numActive["+oldActive+"] changing numActive to " + numActive);
                        
                        //if (t2 == 0n)
                        //    toRemove.add(edge);                            
                        //else
                        //    transit.put(edge, t2);
                        
                        assert numActive >= 0 : here + " Master(id="+id+").convertToDead FATAL error, numActive must not be negative";
                        if (edge.kind == ASYNC) {
                            for (1..count) {
                                if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                                val dpe = new DeadPlaceException(Place(edge.dst));
                                dpe.fillInStackTrace();
                                addExceptionUnsafe(dpe);
                            }
                        }
                    }
                    else assert false: here + " Master(id="+id+").convertToDead FATAL error, t1 >= t2 condition not met";
                }
            }
            
            //for (e in toRemove)
            //    transit.remove(e);
                    
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(newDead:HashSet[Int], countReceived:HashMap[FinishResilient.ReceivedQueryId, Int]) {
            if (verbose>=1) debug(">>>> Master(id="+id+").convertFromDead called");
            //val toRemove = new HashSet[Edge]();
            for (e in sent.entries()) {
                val edge = e.getKey();
                var trns:Int = e.getValue();
                if (newDead.contains(edge.src) && edge.src != edge.dst ) {
                    val sent = sent.get(edge);
                    val rcvId = FinishResilient.ReceivedQueryId(id, edge.src, edge.dst, edge.kind);
                    val received = countReceived.get(rcvId);
                    assert sent > 0 && sent >= received : here + " Master(id="+id+").convertFromDead FATAL error, transit["+trns+"] sent["+sent+"] received["+received+"]";

                    val dropedMsgs = sent - received;
                    val oldActive = numActive;
                    numActive -= dropedMsgs;
                    trns -= dropedMsgs;
                    if (verbose>=1) debug("==== Master(id="+id+").convertFromDead src="+edge.src+" dst="+edge.dst+" transit["+trns+"] sent["+sent+"] received["+received+"] numActive["+oldActive+"] dropedMsgs["+dropedMsgs+"] changing numActive to " + numActive);
                    
                    //if (trns == 0n)
                    //    toRemove.add(edge);
                    //else
                    //    transit.put(edge, trns);
                    
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                    // we don't add DPE when src is dead; we can assume that the message was not sent as long as it was not received.
                }
            }
            //for (e in toRemove)
            //    transit.remove(e);
            if (quiescent()) {
                releaseLatch();
                notifyParent();
                removeFromStates();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").convertFromDead returning, numActive="+numActive);
        }*/
    }
    
    //REMOTE
    public static final class P0OptimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
        val id:Id; //parent root finish
        val ilock = new Lock(); //instance level lock
        val received = new HashMap[Task,Int](); //increasing counts
        val reported = new HashMap[Task,Int](); //increasing counts
        var taskDeny:HashSet[Int] = null; // lazily allocated
        var lc:Int = 0n; //whenever lc reaches zero, report reported-received to root finish and set reported=received
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        public def countReceived(src:Int, kind:Int) {
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
        
        private def getReportMap() {
            if (verbose>=1) debug(">>>> Remote(id="+id+").getReportMap called");
            
            val map = new HashMap[Task,Int]();
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
            if (verbose>=1) debug("<<<< Remote(id="+id+").getReportMap returning");
            if (verbose>=3) dump();
            return map;
        }
        
        public def notifyReceived(t:Task) {
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyReceived called");
            try {
                ilock.lock();
                if (taskDeny != null && taskDeny.contains(t.place) )
                    throw new RemoteCreationDenied();
                increment(received, t);
                ++lc;
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyReceived returning, lc="+lc);
                if (verbose >= 3) dump();
                return lc;
            } finally {
                ilock.unlock();
            }
        }
        
        public def notifyTerminationAndGetMap(t:Task) {
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called");
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
                    map = getReportMap();
                }
                
                if (verbose>=1) {
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
                if (verbose>=3) dump();
            } finally {
                ilock.unlock();
            }
            return map;
        }
        
        def notifySubActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, ASYNC);
        }
        
        def notifyShiftedActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, AT);
        }
        
        def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {
        	/*
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (dstId == here.id as Int) {
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called locally, no action required");
            } else {
                val parentId = UNASSIGNED;
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, DUMMY_INT, DUMMY_INT, srcId, dstId, kind);
                FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }*/
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            try {
                val lc = notifyReceived(Task(srcId, ASYNC));
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + lc);
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
                val lc = notifyReceived(Task(srcId, AT));
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, localCount = " + lc);
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
        	/*
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
                val parentId = UNASSIGNED;
                val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, DUMMY_INT,  DUMMY_INT, srcId, dstId, kind);
                req.ex = t;
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
            });*/
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
        }
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
            /*val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val lc = notifyReceived(Task(srcId, ASYNC));
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + lc);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }
            
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
                val req = FinishRequest.makeTermMulRequest(id, parentId, DUMMY_INT,  DUMMY_INT, dstId, map);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
            });*/
        }
        
        def pushException(t:CheckedThrowable):void {
        	/*
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, DUMMY_INT,  DUMMY_INT, t);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Remote(id="+id+").pushException(t="+t.getMessage()+") returning");
            */
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int):void {
        	/*
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called ");
            val map = notifyTerminationAndGetMap(Task(srcId, kind));
            if (map == null)
                return;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
            val req = FinishRequest.makeTermMulRequest(id, parentId, DUMMY_INT,  DUMMY_INT, dstId, map);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        	*/
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
    }
    
    //ROOT
    public static final class P0OptimisticMasterState extends FinishResilient implements x10.io.Unserializable {
        val ref:GlobalRef[P0OptimisticMasterState] = GlobalRef[P0OptimisticMasterState](this);
        val id:Id;
        val parentId:Id; //the direct parent of this finish (rootFin/remoteFin, here/remote)
        val parent:FinishState;//the direct parent finish object (used in globalInit for recursive initializing)
        val latch = new SimpleLatch();//latch for blocking and releasing the host activity
        var isGlobal:Boolean = false;//flag to indicate whether finish has been resiliently replicated or not
        var strictFinish:Boolean = false;
        var excs:GrowableRail[CheckedThrowable]; 
        var lc:Int = 1n;
    	val optSrc:Place; //the source place that initiated the task that created this finish
    	val optKind:Int; //the kind of the task that created the finish
        
        def this(id:Id, parent:FinishState, src:Place, kind:Int) {
            this.id = id;
            this.parent = parent;
            this.optSrc = src;
            this.optKind = kind;
            if (parent instanceof FinishResilientPlace0Optimistic) {
                parentId = (parent as FinishResilientPlace0Optimistic).id;
            } else {
                parentId = UNASSIGNED;
            }
        }
        
        public def lock() {
            latch.lock();
        }
        
        
        public def unlock() {
            latch.unlock();
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
        
        def localFinishReleased() {
            try { 
                latch.lock();
                if (!isGlobal) {
                    if (verbose>=1) debug(">>>> localFinishReleased(id="+id+") true: zero localCount on local finish; releasing latch");
                    latch.release();
                    return true;
                } 
                if (verbose>=1) debug("<<<< localFinishReleased(id="+id+") false: global finish");
                return false;
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
        
        /*public def exec(req:FinishRequest) {
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
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + " ] called");
                try{
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
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", submit="+resp.submit+" ] returning");
            } else if (req.reqType == FinishRequest.TERM) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] called");
                try{
                    //Unlike place0 finish, we don't suppress termination notifications whose dst is dead.
                    //We actually wait for these termination messages to come from (kind of) adopted children
                    transitToCompleted(srcId, dstId, kind, ex, "notifyActivityTermination", resp);
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId=" + dstId + ", kind=" + kind + ", ex="+ ex + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    addException(ex, resp);
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TERM_MUL) {
                val map = req.map;
                val dstId = req.dstId;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] called");
                try{
                  //Unlike place0 finish, we don't suppress termination notifications whose dst is dead.
                  //We actually wait for any sent termination messages
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
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
            }
            return resp;
        }*/
        
        def notifySubActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, ASYNC);
        }
        
        def notifyShiftedActivitySpawn(place:Place):void {
            notifySubActivitySpawn(place, AT);
        }
        
        def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {
        	/*
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            if (srcId == dstId) {
                val lc = lc_incrementAndGet();
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
                FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }*/
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
            /*val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
                val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
                req.ex = t;
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
            });*/
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            /*val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") called");
            //no need to call notifyActivityCreation() since it does NOOP in Root finish
            Runtime.submitUncounted( ()=>{
                notifyActivityTermination(srcPlace, ASYNC);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
            });*/
        }
        
        def pushException(t:CheckedThrowable):void {
            /*if (localFinishExceptionPushed(t)) {
                if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeExcpRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, t);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
            */
        }

        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int):void {
            /*val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val lc = lc_decrementAndGet();
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") called, decremented localCount to "+lc);
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
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                return;
            }
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makeTermRequest(id, parentId, UNASSIGNED, optSrc.id as Int, optKind, srcId, dstId, kind);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        	*/
        }

        def waitForFinish():void {
        	/*
            if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called, lc = " + lc_Get() );

            // terminate myself
            notifyActivityTermination(here, ASYNC);

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
            //forgetGlobalRefs();
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").waitForFinish returning with exceptions, size=" + excs.size() );
                throw new MultipleExceptions(excs);
            }*/
        }
    }
    
    /*************** Adoption Logic ******************/
    /*
    static def getCountingRequests(newDead:HashSet[Int],
            masters:HashSet[FinishMasterState], backups:HashSet[FinishBackupState]) {
        val resolveReqs = new HashMap[Int,P0ResolveRequest]();
        if (!masters.isEmpty()) {
            for (dead in newDead) {
                val backup = FinishReplicator.getBackupPlace(dead);
                for (mx in masters) {
                    val m = mx as P0OptimisticMasterState; 
                    m.lock();
                    for (entry in m.sent.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:P0ResolveRequest = resolveReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new P0ResolveRequest();
                                resolveReqs.put(backup, rreq);
                            }
                            rreq.countBackups.put(BackupQueryId(m.id , dead), -1n);
                        } else if (dead == edge.src) {
                            var rreq:P0ResolveRequest = resolveReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new P0ResolveRequest();
                                resolveReqs.put(edge.dst, rreq);
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
                            var rreq:P0ResolveRequest = resolveReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new P0ResolveRequest();
                                resolveReqs.put(backup, rreq);
                            }
                            rreq.countBackups.put(BackupQueryId(b.id, dead), -1n);
                        } else if (dead == edge.src) {
                            var rreq:P0ResolveRequest = resolveReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new P0ResolveRequest();
                                resolveReqs.put(edge.dst, rreq);
                            }
                            rreq.countReceived.put(ReceivedQueryId(b.id, dead, edge.dst, edge.kind), -1n);
                        }
                    }
                    b.unlock();
                }
            }
        }
      if (verbose >=1 ) {
          val s = new x10.util.StringBuilder();
          if (resolveReqs.size() > 0) {
              for (e in resolveReqs.entries()) {
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
        return resolveReqs;
    }
    
    static def processCountingRequests(resolveReqs:HashMap[Int,P0ResolveRequest]) {
        if (verbose>=1) debug(">>>> processCountingRequests(size="+resolveReqs.size()+") called");
        if (resolveReqs.size() == 0) {
            if (verbose>=1) debug("<<<< processCountingRequests(size="+resolveReqs.size()+") returning, zero size");
            return;
        }
        val places = new Rail[Int](resolveReqs.size());
        val iter = resolveReqs.keySet().iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val pl = iter.next();
            places(i++) = pl;
        }
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val outputGr = GlobalRef[HashMap[Int,P0ResolveRequest]](resolveReqs);
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                val requests = resolveReqs.getOrThrow(p);
                if (verbose>=1) debug("==== processCountingRequests  moving from " + here + " to " + Place(p));
                if (Place(p).isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("counting_request") async {
                        if (verbose>=1) debug("==== processCountingRequests  reached from " + gr.home + " to " + here);
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
                                val count = countReceived(key.id, key.src, key.kind);
                                countReceived.put(key, count);
                            }
                        }
                        
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== processCountingRequests  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("counting_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,P0ResolveRequest]]{self.home == here})().getOrThrow(me);
                            
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
        
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
        if (fin.failed())
            throw new Exception("FATAL ERROR: another place failed during recovery ...");
    }
    
    static def countLocalChildren(id:Id, backups:HashSet[FinishBackupState]) {
        if (verbose>=1) debug(">>>> countLocalChildren(id="+id+") called");
        var count:Int = 0n;
        for (b in backups) {
            if (b.getParentId() == id) 
                count++;
        }
        if (verbose>=1) debug("<<<< countLocalChildren(id="+id+") returning, count = " + count);
        return count;
    }
    
    //FIXME: nominate another master if the nominated one is dead
    static def createMasters(backups:HashSet[FinishBackupState]) {
        if (verbose>=1) debug(">>>> createMasters(size="+backups.size()+") called");
        if (backups.size() == 0) {
            if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+" returning, zero size");
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
                //val _transit = b.transit;
                val _sent = b.sent;
                val _excs = b.excs;
                val _optSrc = b.optSrc;
                val _optKind = b.optKind;
                
                val master = Place(places(i));
                if (master.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (master) @Immediate("create_masters") async {
                        val newM = new P0OptimisticMasterState(_id, _parentId, _numActive, _sent,
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
        
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        
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
        
        if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+") returning");
    }
    
    //FIXME: nominate another backup if the nominated one is dead
    static def createOrSyncBackups(newDead:HashSet[Int], masters:HashSet[FinishMasterState]) {
        if (verbose>=1) debug(">>>> createOrSyncBackups(size="+masters.size()+") called");
        if (masters.size() == 0) {
            FinishReplicator.nominateBackupPlaceIfDead(here.id as Int);
            if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning, zero size");
            return;
        }
        
        val places = new Rail[Int](masters.size());
        val iter = masters.iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val m = iter.next() as P0OptimisticMasterState;
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
                val m = mx as P0OptimisticMasterState;
                val backup = Place(m.backupPlaceId);
                val id = m.id;
                val parentId = m.parentId;
                val optSrc = m.optSrc;
                val optKind = m.optKind;
                val numActive = m.numActive;
                //val transit = m.transit;
                val sent = m.sent;
                val excs = m.excs;
                if (backup.isDead()) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (backup) @Immediate("create_or_sync_backup") async {
                        FinishReplicator.createOptimisticBackupOrSync(id, parentId, optSrc, optKind, numActive, //transit, 
                                sent, excs, placeOfMaster);
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
            throw new Exception("FATAL ERROR: another place failed during recovery ...");
        
        for (mx in masters) {
            val m = mx as P0OptimisticMasterState;
            m.lock();
            m.migrating = false;
            m.unlock();
        }
        if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning");
    }
*/    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (here != place0) {
            //NOLOG if (verbose>=2) debug(">>>> notifyPlaceDeath returning, not place0");
            return;
        }
        if (Runtime.activity() == null) {
            if (verbose>=1) debug(">>>> notifyPlaceDeath returning, IGNORED REQUEST FROM IMMEDIATE THREAD");
            return; 
        }
        /*
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
        
        val resolveReqs = getCountingRequests(newDead, masters, backups); // combine all requests targetted to a specific place
        processCountingRequests(resolveReqs); //obtain the counts
        
        //merge all results
        val countBackups = new HashMap[FinishResilient.Id, Int]();
        val countReceived = new HashMap[FinishResilient.ReceivedQueryId, Int]();
        for (e in resolveReqs.entries()) {
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
            val master = m as P0OptimisticMasterState;
            
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
            if (verbose>=1) debug("==== createMasters bypassed ====");
            return;
        }
        */
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}

class P0ResolveRequest {
    val countReceived = new HashMap[FinishResilient.ReceivedQueryId, Int]();
}