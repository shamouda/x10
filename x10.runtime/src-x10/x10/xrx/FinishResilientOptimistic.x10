/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
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

//TODO: clean remote finishes
//TODO: bulk globalInit for a chain of finishes.
//TODO: createBackup: repeat if backup place is dead. block until another place is found!!
//TODO: postpone backup creation to be with transit
//TODO: toString
//TODO: strictFinish
/**
 * Distributed Resilient Finish (records transit tasks only)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 */
class FinishResilientOptimistic extends FinishResilient implements CustomSerialization {
	protected transient var me:FinishResilient; // local finish object
	val id:Id;

	public def toString():String { 
		return me.toString();
	}
	
	//a cache for remote finish objects
	private static val remotes = new HashMap[Id, OptimisticRemoteState]() ;
	
	def notifySubActivitySpawn(place:Place):void { me.notifySubActivitySpawn(place); }
	def notifyShiftedActivitySpawn(place:Place):void { me.notifyShiftedActivitySpawn(place); }
	def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean { return me.notifyActivityCreation(srcPlace, activity); }
	def notifyShiftedActivityCreation(srcPlace:Place):Boolean { return notifyShiftedActivityCreation(srcPlace); }
	def notifyRemoteContinuationCreated():void { me.notifyRemoteContinuationCreated(); }
	def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { me.notifyActivityCreationFailed(srcPlace, t); }
	def notifyActivityCreatedAndTerminated(srcPlace:Place):void { me.notifyActivityCreatedAndTerminated(srcPlace); }
	def pushException(t:CheckedThrowable):void { me.pushException(t); }
	def notifyActivityTermination(srcPlace:Place):void { me.notifyActivityTermination(srcPlace); }
	def notifyShiftedActivityCompletion(srcPlace:Place):void { me.notifyShiftedActivityCompletion(srcPlace); }
	def waitForFinish():void { me.waitForFinish(); }
    
    //create root finish
    public def this (parent:FinishState, src:Place) {
    	id = Id(here.id as Int, nextId.getAndIncrement());
    	me = new OptimisticMasterState(id, parent, src);
    	if (verbose>=1) debug("<<<< RootFinish(id="+id+", src="+src+") created");
    }
    
    private static def getRemote(id:Id) {
        try {
            FinishResilient.glock.lock();
            var remoteState:OptimisticRemoteState = remotes.getOrElse(id, null);
            if (remoteState == null) {
                remoteState = new OptimisticRemoteState(id);
                remotes.put(id, remoteState);
            }
            else if (verbose>=1) debug("<<<< RemoteFinish(id="+id+") found");
            return remoteState;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
    	id = deser.readAny() as Id;
    	me = getRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState, src:Place) {
        val fs = new FinishResilientOptimistic(parent, src);
        FinishReplicator.addMaster(fs.id, fs.me as OptimisticMasterState);
        return fs;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
                           // false means don't create a backup
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    //makeBackup is true only when a parent local finish if forced to be global
	//by a child
    private def globalInit(makeBackup:Boolean) {
    	if (me instanceof OptimisticMasterState) {
    		val rootState = me as OptimisticMasterState;
    		rootState.latch.lock();
	        if (!rootState.isGlobal) {
	            if (verbose>=1) debug(">>>> globalInit(id="+id+") called");
	            if (id != TOP_FINISH) {
    	        	val parent = rootState.parent;
    	            if (parent instanceof FinishResilientOptimistic) {
    	                val frParent = parent as FinishResilientOptimistic;
    	                if (frParent.me instanceof OptimisticMasterState) (frParent as FinishResilientOptimistic).globalInit(true);
    	            }
    	            if (makeBackup)
    	                createBackup();
	            } else {
	                if (verbose>=1) debug("=== globalInit(id="+id+") replication not required for top finish");    
	            }
	            rootState.isGlobal = true;
	            if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
	        }
	        rootState.latch.unlock();
        }
    }
    
    private def createBackup() {
    	val rootState = me as OptimisticMasterState;
    	//TODO: redo if backup dies
    	if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
     	val backup = Place(rootState.getBackupPlaceId());
     	if (backup.isDead()) {
     		if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
     		return false;
     	}
     	val myId = id; //don't copy this
     	val home = here;
     	val parentId = rootState.parentId;
     	val backRes = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_create") async {
                val bFin = FinishReplicator.findBackupOrCreate(myId, parentId, false);
                val success = bFin != null;
                at (condGR) @Immediate("backup_create_response") async {
                	val bRes = (backRes as GlobalRef[Cell[Boolean]]{self.home == here})();
                	bRes.value = success;
                    condGR().release();
                }
            }; 
        };
        rCond.run(closure);
        //TODO: complete the below lines
        if (rCond.failed()) {
            val excp = new DeadPlaceException(backup);
        }
        rCond.forget();
        return true;
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) {
            debug(">>>> notifyPlaceDeath called");
            var str:String = "";
            for (p in Place.places()) {
                str += "place(" + p.id + ")=" + p.isDead() + " "; 
            }
            debug("<<<< notifyPlaceDeath returning [isDead: "+str+"]");
        }
    }
    
    /* Because we record only the transit tasks with src-dst, we must record the
     * number of received tasks from each place, so that we can properly report termination at the end.
     * 
     * For adoption, we rely on getting information about remote tasks from remote finish objects.
     * Therefore, we must store the remote objects until root finish is released.
     * 
     * To save memory, we will not create a remote object for each src place,
     * but one remote object per finish that has a HashSet recording the counts of received tasks from each place. 
     */
    public static final class OptimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
    	val id:Id; //parent root finish

        //instance level lock
        val ilock = new Lock();
        
        //root of root (potential adopter) 
        var adopterId:Id = UNASSIGNED;

        var isAdopted:Boolean = false;
        
        val received = new HashMap[Task,Int]();
        
        val reported = new HashMap[Task,Int]();
        
        var denySet:HashSet[Id] = null; // lazily allocated
        
        var lc:Int = 0n; //whenever lc reaches zero, report reported-received to root finish and set reported=received
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Remote dump:\n");
            s.add("           here:" + here.id); s.add('\n');
            s.add("             id:" + id); s.add('\n');            
            s.add("     localCount:"); s.add(lc); s.add('\n');
            s.add("      adopterId: " + adopterId); s.add('\n');
            if (received.size() > 0) {
                s.add("          received:\n");
                for (e in received.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (received.size() > 0) {
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
            if (verbose>=1) { 
                debug(">>>> Remote(id="+id+").getReportMap called");
                dump();
            }
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
            return map;
        }
        
        public def notifyReceived(t:Task) {
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyReceived called");
            try {
                ilock.lock();
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
                        assert false:"FATAL ERROR: notifyTerminationAndGetMap(id="+id+") reached a negative local count";
                    }
                }
                
                if (lc == 0n) {
                    map = getReportMap();
                }
            } finally {
                ilock.unlock();
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
            
            return map;
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
                val parentId = UNASSIGNED;
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
            	val req = new FinishRequest(FinishRequest.TRANSIT, id, parentId, srcId, dstId, kind);
            	FinishReplicator.exec(req);
            	if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }
        }
        
        /*
         * This method can't block because it may run on an @Immediate worker.  
         */
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val lc = notifyReceived(Task(srcId, ASYNC));
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + lc);
            return true;    
        }
        
        /*
         * See similar method: notifyActivityCreation
         */
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") called");
            val lc = notifyReceived(Task(srcId, AT));
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId +",dstId="+dstId+",kind="+AT+") returning, localCount = " + lc);
            return true;
        }
        
        def notifyRemoteContinuationCreated():void {
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyRemoteContinuationCreated(id="+id+") called");
            }
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyActivityCreationFailed(id="+id+") called");
            }
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyActivityCreatedAndTerminated(id="+id+") called");
            }
        }
        
        def pushException(t:CheckedThrowable):void {
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = new FinishRequest(FinishRequest.EXCP, id, parentId, t);
            val resp = FinishReplicator.exec(req);
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
            val parentId = UNASSIGNED;
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
            val req = new FinishRequest(FinishRequest.TERM_MUL, id, parentId, dstId, map);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
        	assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
    }

    public static final class OptimisticMasterState extends FinishMasterState implements x10.io.Unserializable {
    	val id:Id;

        //the direct parent of this finish (rootFin/remoteFin, here/remote)
        val parentId:Id; 
        
        //the direct parent finish object (used in globalInit for recursive initializing)
        val parent:FinishState;

        //latch for blocking and releasing the host activity
        val latch:SimpleLatch = new SimpleLatch();

        //resilient finish counter set
        var numActive:Long;
        val transit = new HashMap[Edge,Int]();
        
        //the nested finishes within this finish scope
        var adoptedChildren:HashSet[Id] = null; // lazily allocated
        
        //flag to indicate whether finish has been resiliently replicated or not
        var isGlobal:Boolean = false;
        
        //will be updated in notifyPlaceDeath
        var backupPlaceId:Int = FinishReplicator.nextPlaceId.get();
        
        var excs:GrowableRail[CheckedThrowable]; 
        
        val src:Place; //the source place that initiated the task that created this finish

        var lc:Int = 1n;
        
        def this(id:Id, parent:FinishState, src:Place) {
            this.id = id;
            this.numActive = 1;
            this.parent = parent;
            this.src = src;
            increment(transit, Edge(id.home, id.home, FinishResilient.ASYNC));
            if (parent instanceof FinishResilientOptimistic) {
                parentId = (parent as FinishResilientOptimistic).id;
            }
            else {
            	if (verbose>=1) debug("FinishResilientOptimistic(id="+id+") foreign parent found of type " + parent);
            	parentId = UNASSIGNED;
            }
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
            
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Root dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("      numActive:"); s.add(numActive); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (adoptedChildren != null) {
            	s.add("       adoptedChildren: {"); 
            	for (e in adoptedChildren) {
            		s.add( e + " ");
            	}
            	s.add("}\n");
            }
            //s.add("      adopterId: " + adopterId); s.add('\n');
            if (transit.size() > 0) {
                s.add("        transit:\n"); 
                for (e in transit.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        

        def addExceptionUnsafe(t:CheckedThrowable) {
    		if (excs == null) excs = new GrowableRail[CheckedThrowable]();
    		excs.add(t);
        }
        
        def addDeadPlaceException(placeId:Long) {
            try {
                latch.lock();
                val e = new DeadPlaceException(Place(placeId));
                addExceptionUnsafe(e);
                return backupPlaceId;
            } finally {
                latch.unlock();
            }
        }
        
        def addException(t:CheckedThrowable) {
            try {
                latch.lock();
                addExceptionUnsafe(t);
                return backupPlaceId;
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
        
        def getBackupPlaceId() {
        	try {
                latch.lock();
                return backupPlaceId;
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
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
        	if (verbose>=1) debug(">>>> Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " called");
        	try {
                latch.lock();
                val e = Edge(srcId, dstId, kind);
                increment(transit, e);
                numActive++;
                if (verbose>=3) {
                    debug("==== Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
                    dump();
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                return backupPlaceId;
        	} finally {
        		 latch.unlock();
        	}
        }
        
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String) {
            try {
                latch.lock();
                if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
                val e = Edge(srcId, dstId, kind);
                decrement(transit, e);
                numActive--;
                if (quiescent()) {
                    releaseLatch();
                    //removeFromStates();
                }
                if (verbose>=3) {
                    debug("==== Master(id="+id+").inTransit srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
                    dump();
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                return backupPlaceId;
            } finally {
                latch.unlock();
            }
        }
        
        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, tag:String) {
            if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " called");
            val e = Edge(srcId, dstId, kind);
            deduct(transit, e, cnt);
            numActive-=cnt;
            if (quiescent()) {
                releaseLatch();
                //removeFromStates();
            }
            if (verbose>=3) {
                debug("==== Master(id="+id+").transitToCompletedMul srcId=" + srcId + ", dstId=" + dstId + " dumping after update");
                dump();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
            return backupPlaceId;
        }
        
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Master(id="+id+").quiescent called");
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=2) debug("quiescent(id="+id+") returning false, already adopted by adopterId=="+adopterId);
                return false;
            }
            */
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
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=1) debug("releaseLatch(id="+id+") called on adopted finish; not releasing latch");
            } else */
            {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                if (verbose>=2) debug("Master(id="+id+") releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));
                if (exceptions != null) {
                    if (excs == null) excs = new GrowableRail[CheckedThrowable](exceptions.size);
                    excs.addAll(exceptions);
                }
                latch.release();
            }
            if (verbose>=2) debug("Master(id="+id+").releaseLatch returning");
        }
        
        public def exec(req:FinishRequest) {
        	val id = req.id;
            val resp = new MasterResponse();
            if (req.reqType == FinishRequest.TRANSIT) {
            	val srcId = req.srcId;
            	val dstId = req.dstId;
            	val kind = req.kind;
            	resp.transit_ok = false;
            	if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + " ] called");
            	try{
    	        	if (Place(srcId).isDead()) {
                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
                    } else if (Place(dstId).isDead()) {
                        if (kind == FinishResilient.ASYNC) {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
                            resp.backupPlaceId = addDeadPlaceException(dstId);
                        } else {
                            if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
                        }
                    } else {
                    	resp.backupPlaceId = inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                    	resp.transit_ok = true;
                    }
            	} catch (t:Exception) { //fatal
            	    t.printStackTrace();
            		resp.backupPlaceId = -1n;
                	resp.excp = t;
            	}
            	if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + ", transit_ok="+resp.transit_ok+" ] returning");
            } else if (req.reqType == FinishRequest.TERM) {
            	val srcId = req.srcId;
            	val dstId = req.dstId;
            	val kind = req.kind;
            	if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + " ] called");
            	try{
            		if (Place(dstId).isDead()) {
                        // NOTE: no state updates or DPE processing here.
                        //       Must happen exactly once and is done
                        //       when Place0 is notified of a dead place.
                        if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
                    } else {
            			resp.backupPlaceId = transitToCompleted(srcId, dstId, kind, "notifyActivityTermination");
                    }
            	} catch (t:Exception) { //fatal
            	    t.printStackTrace();
            		resp.backupPlaceId = -1n;
                	resp.excp = t;
            	}
            	if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    resp.backupPlaceId = addException(ex);
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
                    if (Place(dstId).isDead()) {
                        // NOTE: no state updates or DPE processing here.
                        //       Must happen exactly once and is done
                        //       when Place0 is notified of a dead place.
                        if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId);
                    } else {
                        try {
                            latch.lock();
                            resp.backupPlaceId = -1n;
                            for (e in map.entries()) {
                                val srcId = e.getKey().place;
                                val kind = e.getKey().kind;
                                val cnt = e.getValue();
                                resp.backupPlaceId = transitToCompletedUnsafe(srcId, dstId, kind, cnt, "notifyActivityTermination");
                            }
                        } finally {
                            latch.unlock();
                        }
                    }
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
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
                if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called locally, localCount now "+lc);
            } else {
            	if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            	val req = new FinishRequest(FinishRequest.TRANSIT, id, parentId, srcId, dstId, kind);
            	FinishReplicator.exec(req);
            	if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            }
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
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyRemoteContinuationCreated(id="+id+") called");
            }
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyActivityCreationFailed(id="+id+") called");
            }
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
            for (var i:Long = 0; i < 100; i++) {
                debug(">>>> FATAL notifyActivityCreatedAndTerminated(id="+id+") called");
            }
        }
        
        def pushException(t:CheckedThrowable):void {
            if (localFinishExceptionPushed(t)) {
                if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
                return;
            }
            if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = new FinishRequest(FinishRequest.EXCP, id, parentId, t);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
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
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") called, decremented localCount to "+lc);
            if (lc > 0) {
                return;
            }
            if (lc < 0) {
                for (var i:Long = 0 ; i < 100; i++) {
                    debug("FATAL ERROR: Root(id="+id+").notifyActivityTermination reached a negative local count");
                    assert false: "FATAL ERROR: Root(id="+id+").notifyActivityTermination reached a negative local count";
                }
            }
            if (localFinishReleased()) {
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind+") returning");
                return;
            }
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = new FinishRequest(FinishRequest.TERM, id, parentId, srcId, dstId, kind);
            val resp = FinishReplicator.exec(req);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }

        def waitForFinish():void {
    		if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called, lc = " + lc_Get() );

            // terminate myself
            notifyActivityTermination(here, ASYNC);

            // If we haven't gone remote with this finish yet, see if this worker
            // can execute other asyncs that are governed by the finish before waiting on the latch.
            /*TODO: revise this*/ 
            if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS /*|| !strictFinish*/)) {
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
            if (excs != null) throw new MultipleExceptions(excs);
            
            if (id == TOP_FINISH) {
                //blocks until final replication messages sent from place 0
                //are responded to.
                FinishReplicator.finalizeReplication();
            }
        }
    }

    public static final class OptimisticBackupState extends FinishBackupState implements x10.io.Unserializable {
        private static val verbose = FinishResilient.verbose;
        static def debug(msg:String) {
            val nsec = System.nanoTime();
            val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
            Console.OUT.println(output); Console.OUT.flush();
        }
        
        //instance level lock
        val ilock = new Lock();

        //finish id 
        val id:FinishResilient.Id;
        
        //the root parent id (potential adopter if on a different place that master)
        val parentId:FinishResilient.Id;
        
        //resilient finish counter set
        var numActive:Long;
        val transit = new HashMap[FinishResilient.Edge,Int]();
        
        //the nested finishes within this finish scope
        var adoptedChildren:HashSet[FinishResilient.Id] = null; // lazily allocated
        
        var isAdopted:Boolean = false;
        
        var adopterId:FinishResilient.Id;
        
        var excs:GrowableRail[CheckedThrowable]; 
        
        var isReleased:Boolean = false;
        
        //var masterPlaceId:Int = FinishResilientOptimistic.prevPlaceId.get();
        
        def this(id:FinishResilient.Id, parentId:FinishResilient.Id) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            FinishResilient.increment(transit, FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC));
        }
        
        def markAsAdopted() {
            try {
                ilock.lock();
                isAdopted = true;
            } finally {
                ilock.unlock();
            }
        }
        
        def addExceptionUnsafe(t:CheckedThrowable) {
    		if (excs == null) excs = new GrowableRail[CheckedThrowable]();
    		excs.add(t);
        }
        
        def addDeadPlaceException(placeId:Long) {
            try {
                ilock.lock();
                val e = new DeadPlaceException(Place(placeId));
                addExceptionUnsafe(e);
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
        
        //waits until backup is adopted
        public def getAdopter() {
        	Runtime.increaseParallelism();
        	ilock.lock();
            while (!isAdopted) {
            	ilock.unlock();
                System.threadSleep(0); // release the CPU to more productive pursuits
                ilock.lock();
            }
            ilock.unlock();
            Runtime.decreaseParallelism(1n);
            return adopterId;
        }
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
        	try {
        		ilock.lock();
        		if (verbose>=1) debug(">>>> Backup(id="+id+").inTransit called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                FinishResilient.increment(transit, e);
                numActive++;
                if (verbose>=1) debug("<<<< Backup(id="+id+").inTransit returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
        	} finally {
        		ilock.unlock();
        	}
        }
        
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                val t = FinishResilient.Task(dstId, kind);
                FinishResilient.decrement(transit, e);
                numActive--;
                if (quiescent()) {
                    isReleased = true;
                    //removeFromStates();
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, tag:String) {
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompletedMul called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                val t = FinishResilient.Task(dstId, kind);
                FinishResilient.deduct(transit, e, cnt);
                numActive-=cnt;
                if (quiescent()) {
                    isReleased = true;
                    //removeFromStates();
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompletedMul returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Backup(id="+id+").quiescent called");
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=2) debug("quiescent(id="+id+") returning false, already adopted by adopterId=="+adopterId);
                return false;
            }
            */
            if (numActive < 0) {
                debug("COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!");
                assert false : "COUNTING ERROR: Backup(id="+id+").quiescent negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Backup(id="+id+").quiescent returning " + quiet);
            return quiet;
        }
        
        public def exec(req:FinishRequest) {
            val resp = new BackupResponse();
            if (req.reqType == FinishRequest.TRANSIT) {
            	val srcId = req.srcId;
            	val dstId = req.dstId;
            	val kind = req.kind;
            	if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
            	                    + dstId + ",kind=" + kind + " ] called");
            	try{
            	    inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
            	} catch (t:Exception) {
                	resp.excp = t;
                }
            	if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
                        + dstId + ",kind=" + kind + " ] returning");
            } else if (req.reqType == FinishRequest.TERM) {
            	val srcId = req.srcId;
            	val dstId = req.dstId;
            	val kind = req.kind;
            	if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + " ] called");
            	try{
            		transitToCompleted(srcId, dstId, kind, "notifyActivityTermination");
            	} catch (t:Exception) {
                	resp.excp = t;
                }
            	if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + " ] returning");
            } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    addException(ex);
                } catch (t:Exception) {
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TERM_MUL) {
                val map = req.map;
                val dstId = req.dstId;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] called");
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
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+" ] returning");
            }
            return resp;
        }
    }
}