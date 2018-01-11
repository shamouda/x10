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

//TODO: reuse remote finish
//TODO: do we need to memorize all remote objects and delete them explicitly?
//TODO: bulk globalInit for a chain of finishes.
//TODO: createBackup: repeat if backup place is dead. block until another place is found!!
//TODO: CHECK in ResilientFinishP0 -> line 174: decrement(adopterState.live, t);  should be adopterState.liveAdopted
//TODO: does the backup need to keep the exceptions list???
//TODO: implement adoption 
//TODO: test notifyActivityCreationFailed()
//TODO: handle removeFromStates()
//FIXME: main termination is buggy. use LULESH to reproduce the bug. how to wait until all replication work has finished.
/**
 * Distributed Resilient Finish (records transit and live tasks)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 */
class FinishResilientPessimistic extends FinishResilient implements CustomSerialization {
    private static val DUMMY_INT = -1n;
    
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
    def waitForFinish():void { me.waitForFinish(); }

    //create root finish
    public def this (parent:FinishState) {
    	id = Id(here.id as Int, nextId.getAndIncrement());
    	val grlc = GlobalRef[AtomicInteger](new AtomicInteger(1n));
    	me = new PessimisticMasterState(id, parent, grlc);
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
    	id = deser.readAny() as Id;
    	val lc = deser.readAny() as GlobalRef[AtomicInteger];
        if (lc.home == here) {
            me = new PessimisticRemoteState(id, lc);
            if (verbose>=1) debug("<<<< RemoteFinish(id="+id+",lcHome="+lc.home+") createdA");    
        }
        else {
            val grlc = GlobalRef[AtomicInteger](new AtomicInteger(1n));
            me = new PessimisticRemoteState(id, grlc);
            if (verbose>=1) debug("<<<< RemoteFinish(id="+id+",lcHome="+lc.home+") createdB with new lc=1");
        }
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        val fs = new FinishResilientPessimistic(parent);
        FinishReplicator.addMaster(fs.id, fs.me as PessimisticMasterState);
        return fs;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        globalInit(false); // Once we have more than 1 copy of the finish state, we must go global
                           // false means don't create a backup
        ser.writeAny(id);
        val grlc = me instanceof PessimisticMasterState ?
        				(me as PessimisticMasterState).grlc :
        				(me as PessimisticRemoteState).grlc ;
        ser.writeAny(grlc);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    private def globalInit(makeBackup:Boolean) {
    	if (me instanceof PessimisticMasterState) {
    		val rootState = me as PessimisticMasterState;
    		rootState.latch.lock();
	        if (!rootState.isGlobal) {
	            if (verbose>=1) debug(">>>> globalInit(id="+id+") called");
	            if (id != TOP_FINISH) {
    	        	val parent = rootState.parent;
    	            
    	            if (parent instanceof FinishResilientPessimistic) {
    	                val frParent = parent as FinishResilientPessimistic;
    	                if (frParent.me instanceof PessimisticMasterState) (frParent as FinishResilientPessimistic).globalInit(true);
    	            }
    	            
    	            if (rootState.parentId != UNASSIGNED) {
    	            	val req = FinishRequest.makeAddChildRequest(rootState.parentId /*id*/, id /*child_id*/);
    	            	FinishReplicator.exec(req);
    	            }
    	            if (makeBackup)
    	                createBackup();
	            } else {
	                if (verbose>=1) debug("=== globalInit(id="+id+") replication not required for top finish");    
	            }
	            
	            rootState.isGlobal = true;
	            rootState.strictFinish = true;
	            if (verbose>=1) debug("<<<< globalInit(id="+id+") returning");
	        }
	        rootState.latch.unlock();
        }
    }
    
    private def createBackup() {
    	val rootState = me as PessimisticMasterState;
    	//TODO: redo if backup dies
    	if (verbose>=1) debug(">>>> createBackup(id="+id+") called fs="+this);
     	val backup = Place(rootState.getBackupPlaceId());
     	if (backup.isDead()) {
     		if (verbose>=1) debug("<<<< createBackup(id="+id+") returning fs="+this + " dead backup");
     		return false;
     	}
     	val myId = id; //don't copy this
     	val parentId = rootState.parentId;
     	val backRes = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_create") async {
                val bFin = FinishReplicator.findBackupOrCreate(myId, parentId, false, Place(-1));
                val success = bFin != null;
                at (condGR) @Immediate("backup_create_response") async {
                	val bRes = (backRes as GlobalRef[Cell[Boolean]]{self.home == here})();
                	bRes.value = success;
                    condGR().release();
                }
            }; 
        };
        rCond.run(closure);
        //TODO: revise the below        
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
    
    //REMOTE
	public static final class PessimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
		val id:Id; //parent root finish
	
	    //instance level lock
	    val ilock = new Lock();
	    
	    //root of root (potential adopter) 
	    var adopterId:Id = UNASSIGNED;
	
	    var isAdopted:Boolean = false;
	    
	    private val grlc:GlobalRef[AtomicInteger];
	    
	    private def localCount():AtomicInteger = (grlc as GlobalRef[AtomicInteger]{self.home == here})();
	    
	    public def this (val id:Id, grlc:GlobalRef[AtomicInteger]) {
            this.id = id;
            this.grlc = grlc;
        }
	    
	    private def forgetGlobalRefs():void {
	        (grlc as GlobalRef[AtomicInteger]{self.home==here}).forget();
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
	        	if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+here.id + " dstId="+dstId+" kind="+kind+") called ");
	        	val req = FinishRequest.makeTransitRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	        	FinishReplicator.exec(req);
	        	if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+here.id + " dstId="+dstId+" kind="+kind+") returning");
	        }
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
	        
	        Runtime.submitUncounted( ()=>{
	        	if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreation(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") called");
	        	val parentId = UNASSIGNED;
	        	val req = FinishRequest.makeLiveRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	        	val submit = FinishReplicator.exec(req);
	        	if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreation(isrcId=" + srcId + ",dstId="+dstId+",kind="+kind+") returning (submit="+submit+")");
	            if (submit) {
	            	Runtime.worker().push(activity);
	            }
	        });
	        
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
	    	if (verbose>=1) debug(">>>> Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") called");
	    	val parentId = UNASSIGNED;
            val req = FinishRequest.makeLiveRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	    	val submit = FinishReplicator.exec(req);
	    	if (verbose>=1) debug("<<<< Remote(id="+id+").notifyShiftedActivityCreation(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+") returning (submit="+submit+")");
	        return submit;
	    }
	    
	    def notifyRemoteContinuationCreated():void { /*noop for remote finish*/ }

	    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
	        notifyActivityCreationFailed(srcPlace, t, ASYNC);
	    }
	    
	    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
	        val srcId = srcPlace.id as Int;
	        val dstId = here.id as Int;
            //we cannot block in this method, because it can be called from an immediate thread
	        Runtime.submitUncounted( ()=>{
    	        if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
    	        val parentId = UNASSIGNED;
    	        val req = FinishRequest.makeTransitTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind, t);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
	        });
	    }

	    def notifyActivityCreatedAndTerminated(srcPlace:Place) {
	        notifyActivityCreatedAndTerminated(srcPlace, ASYNC);
	    }
	    
	    def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int) {
	        val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            val parentId = UNASSIGNED;
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
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(parentId="+parentId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") called");
                val req = FinishRequest.makeTransitTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind, null);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(parentId="+parentId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") returning");
            });
	    }
	    
	    def pushException(t:CheckedThrowable):void {
	        val parentId = UNASSIGNED;
	        if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
	        val req = FinishRequest.makeExcpRequest(id, parentId, DUMMY_INT, t);
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
	        val lc = localCount().decrementAndGet();

	        if (lc > 0) {
	            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination called, decremented localCount to "+lc);
	            return;
	        }
	        
	        if (lc < 0) {
	            for (var i:Long = 0 ; i < 100; i++) {
	                debug("FATAL ERROR: Remote(id="+id+").notifyActivityTerminationreached a negative local count");
	                assert false: "FATAL ERROR: Remote(id="+id+").notifyActivityTermination reached a negative local count";
	            }
	        }
	        
	        // If this is not the root finish, we are done with the finish state.
	        // If this is the root finish, it will be kept alive because waitForFinish
	        // is an instance method and it is on the stack of some activity.
	        forgetGlobalRefs();
	        
	        val parentId = UNASSIGNED;
	        val srcId = srcPlace.id as Int; 
	        val dstId = here.id as Int;
	    	if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") called");
	    	val req = FinishRequest.makeTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	    	val resp = FinishReplicator.exec(req);
	    	if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + " dstId="+dstId+" kind="+kind+") returning");
	    }

	    def waitForFinish():void {
	    	assert false : "fatal, waitForFinish must not be called from a remote finish" ;
	    }
	}
	
	//ROOT
	public static final class PessimisticMasterState extends FinishMasterState implements x10.io.Unserializable {
		val id:Id;
	
	    //the direct parent of this finish (rootFin/remoteFin, here/remote)
	    val parentId:Id; 
	    
	    //the direct parent finish object (used in globalInit for recursive initializing)
	    val parent:FinishState;

	    //latch for blocking and releasing the host activity
	    val latch:SimpleLatch = new SimpleLatch();

	    //resilient finish counter set
	    var numActive:Long;
	    val live = new HashMap[Task,Int](); 
	    var transit:HashMap[Edge,Int] = null; // lazily allocated
	    var _liveAdopted:HashMap[Task,Int] = null; // lazily allocated 
	    var _transitAdopted:HashMap[Edge,Int] = null; // lazily allocated
	    
	    //the nested finishes within this finish scope
	    var children:HashSet[Id] = null; // lazily allocated
	    
	    //flag to indicate whether finish has been resiliently replicated or not
	    var isGlobal:Boolean = false;
	    
	    var strictFinish:Boolean = false;
	    
	    //will be updated in notifyPlaceDeath
	    var backupPlaceId:Int = FinishReplicator.nextPlaceId.get();
	    
        var excs:GrowableRail[CheckedThrowable]; 
	    
	    val grlc:GlobalRef[AtomicInteger];
	
	    private def localCount():AtomicInteger = (grlc as GlobalRef[AtomicInteger]{self.home == here})();
	    
	    private def forgetGlobalRefs():void {
	        (grlc as GlobalRef[AtomicInteger]{self.home==here}).forget();
	    }
	    
	    def this(id:Id, parent:FinishState, grlc:GlobalRef[AtomicInteger]) {
	        this.id = id;
	        this.numActive = 1;
	        this.parent = parent;
	        this.grlc = grlc;
	        increment(live, Task(id.home, FinishResilient.ASYNC));
	        if (parent instanceof FinishResilientPessimistic) {
	            parentId = (parent as FinishResilientPessimistic).id;
	        }
	        else {
	        	if (verbose>=1) debug("FinishResilientPessimistic(id="+id+") foreign parent found of type " + parent);
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
        
        public def isImpactedByDeadPlaces(newDead:HashSet[Int]):Boolean {
            return false;
        }
        
        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("State dump:\n");
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
            if (_liveAdopted != null && _liveAdopted.size() > 0) {
                s.add("    liveAdopted:\n"); 
                for (e in _liveAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (transit != null && transit.size() > 0) {
                s.add("        transit:\n"); 
                for (e in transit.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            if (_transitAdopted != null && _transitAdopted.size() > 0) {
                s.add(" transitAdopted:\n"); 
                for (e in _transitAdopted.entries()) {
                    s.add("\t\t"+e.getKey()+" = "+e.getValue()+"\n");
                }
            }
            debug(s.toString());
        }
        
	    def liveAdopted() {
	        if (_liveAdopted == null) _liveAdopted = new HashMap[Task,Int]();
	        return _liveAdopted;
	    }

	    def transit() {
	        if (transit == null) transit = new HashMap[Edge,Int]();
	        return transit;
	    }

	    def transitAdopted() {
	        if (_transitAdopted == null) _transitAdopted = new HashMap[Edge,Int]();
	        return _transitAdopted;
	    }
	    
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def isGlobal() {
            try {
                latch.lock();
                return isGlobal;
            } finally {
                latch.unlock();
            }
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
       
	    def addChild(child:Id) {
	    	if (verbose>=1) debug(">>>> Master(id="+id+").addChild(child=" + child + ") called");
	        try {
	            latch.lock();
	            if (children == null) {
	                children = new HashSet[Id]();
	            }
	            children.add(child);
	            if (verbose>=1) debug("<<<< Master(id="+id+").addChild(child=" + child + ") returning");
	            return backupPlaceId;
	        } finally {
	            latch.unlock();
	        }
	    }
	    
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	if (verbose>=1) debug(">>>> Master(id="+id+").inTransit(srcId=" + srcId + ",dstId=" + dstId + ") called");
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
	            if (verbose>=3) {
	                debug("==== Master(id="+id+").inTransit "+tag+" after update for: "+srcId + " ==> "+dstId+" kind="+kind);
	                dump();
	            }
	            if (verbose>=1) debug("<<<< Master(id="+id+").inTransit(srcId=" + srcId + ",dstId=" + dstId + ") returning" );
	            return backupPlaceId;
        	} finally {
        		 latch.unlock();
        	}
        }
        
        def transitToLive(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
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
	            if (verbose>=3) {
	                debug("==== Master(id="+id+").transitToLive "+tag+" after update for: "+srcId + " ==> "+dstId+" kind="+kind);
	                dump();
	            }
	            if (verbose>=1) debug("<<<< Master(id="+id+").transitToLive returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
	            return backupPlaceId;
        	} finally {
        		latch.unlock();
        	}
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, t:CheckedThrowable, toAdopter:Boolean) {
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
                        //removeFromStates();
                    }
                } else {
                    decrement(transitAdopted(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        releaseLatch();
                        //removeFromStates();
                    }
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") returning");
                return backupPlaceId;
            } finally {
                latch.unlock();
            }
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	if (verbose>=1) debug(">>>> Master(id="+id+").liveToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") called");
        	try {
	            latch.lock();
	            val t = Task(dstId, kind);
	            if (!toAdopter) {
	                decrement(live, t);
	                numActive--;
	                if (quiescent()) {
	                    releaseLatch();
	                    //removeFromStates();
	                }
	            } else {
	            	decrement(liveAdopted(), t);
	                numActive--;
	                if (quiescent()) {
	                    releaseLatch();
	                    //removeFromStates();
	                }
	            }
	            if (verbose>=1) debug("<<<< Master(id="+id+").liveToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") returning" );
	            return backupPlaceId;
        	} finally {
        		latch.unlock();
        	}
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
	        if (req.reqType == FinishRequest.ADD_CHILD) {
	            val childId = req.childId;
	            if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=ADD_CHILD, masterId="+id+", childId="+childId+"] called");
	            try{                        
	            	resp.backupPlaceId = addChild(childId);
	            	resp.submit = true;
	            } catch (t:Exception) { //fatal
	                t.printStackTrace();
	            	resp.backupPlaceId = -1n;
	            	resp.excp = t;
	            }
	            if (verbose>=1) debug("<<<< Master(id="+id+").exec returning [req=ADD_CHILD, masterId="+id+", childId="+childId+"]");
	        } else if (req.reqType == FinishRequest.TRANSIT) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
		        	if (Place(srcId).isDead()) {
	                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
	                } else if (Place(dstId).isDead()) {
	                    if (kind == FinishResilient.ASYNC) {
	                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
	                        resp.backupPlaceId = addDeadPlaceException(dstId);
	                        resp.transitSubmitDPE = true;
	                    } else {
	                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
	                    }
	                } else {
	                	resp.backupPlaceId = inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter);
	                	resp.submit = true;
	                }
	        	} catch (t:Exception) { //fatal
	        	    t.printStackTrace();
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", submit="+resp.submit+" ] returning");
	        } else if (req.reqType == FinishRequest.LIVE) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	resp.submit = false;
	        	if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
	        		var msg:String = kind == FinishResilient.ASYNC ? "notifyActivityCreation":"notifyShiftedActivityCreation";
	        		if (Place(srcId).isDead() || Place(dstId).isDead()) {
		                // NOTE: no state updates or DPE processing here.
		                //       Must happen exactly once and is done
		                //       when Place0 is notified of a dead place.
	        			if (verbose>=1) debug("==== Master(id="+id+").exec "+msg+" suppressed: "+srcId + " ==> "+dstId+" kind="+kind);
	        		} else {
	        			resp.backupPlaceId = transitToLive(srcId, dstId, kind, msg, toAdopter);
	        			resp.submit = true;
	        		}
	        	} catch (t:Exception) { //fatal
	        	    t.printStackTrace();
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", submit=" + resp.submit + " ] returning");
	        } else if (req.reqType == FinishRequest.TERM) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
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
	        			resp.backupPlaceId = liveToCompleted(srcId, dstId, kind, "notifyActivityTermination", toAdopter);
	        			resp.submit = true;
	                }
	        	} catch (t:Exception) { //fatal
	        	    t.printStackTrace();
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", submit=" + resp.submit + " ] returning");
	        } else if (req.reqType == FinishRequest.EXCP) {
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                try{
                    resp.backupPlaceId = addException(ex);
                    resp.submit = true;
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TRANSIT_TERM) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + ", ex="+ex+" ] called");
                try{
                    if (Place(srcId).isDead() || Place(dstId).isDead()) {
                        // NOTE: no state updates or DPE processing here.
                        //       Must happen exactly once and is done
                        //       when Place0 is notified of a dead place.
                        if (verbose>=1) debug("==== notifyActivityCreationFailed(id="+id+") suppressed: "+srcId + " ==> "+dstId+" kind="+kind);
                    } else {
                        resp.backupPlaceId = transitToCompleted(srcId, dstId, kind, ex, toAdopter);
                        resp.submit = true;
                    }
                } catch (t:Exception) { //fatal
                    t.printStackTrace();
                    resp.backupPlaceId = -1n;
                    resp.excp = t;
                }
                if (verbose>=1) debug(">>>> Master(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + ", ex="+ex+" ] returning");
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
	        	val req = FinishRequest.makeTransitRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	        	FinishReplicator.exec(req);
	        	if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
	        }
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
	            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called locally. no action required");
	            return true;
	        }
	        
	        Runtime.submitUncounted( ()=>{
	        	if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
	        	val req = FinishRequest.makeLiveRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	        	val submit = FinishReplicator.exec(req);
	        	if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning (submit="+submit+")");
	            if (submit) {
	            	Runtime.worker().push(activity);
	            }
	        });
	        
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
	    	if (verbose>=1) debug(">>>> Root(id="+id+").notifyShiftedActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
	    	val req = FinishRequest.makeLiveRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	    	val submit = FinishReplicator.exec(req);
	    	if (verbose>=1) debug("<<<< Root(id="+id+").notifyShiftedActivityCreation(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning (submit="+submit+")");
	        return submit;
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
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
                val req = FinishRequest.makeTransitTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind, t);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
            });
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
                    for (var i:Long = 0 ; i < 100; i++) {
                        debug("FATAL ERROR: Root(id="+id+").notifyActivityCreatedAndTerminated() reached a negative local count");
                        assert false: "FATAL ERROR: Root(id="+id+").notifyActivityCreatedAndTerminated() reached a negative local count";
                    }
                }
                
                // If this is not the root finish, we are done with the finish state.
                // If this is the root finish, it will be kept alive because waitForFinish
                // is an instance method and it is on the stack of some activity.
                forgetGlobalRefs();
                
                if (localFinishReleased()) {
                    if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated() returning");
                    return;
                }
            }
            //we cannot block in this method, because it can be called from an immediate thread
            Runtime.submitUncounted( ()=>{
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeTransitTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind, null);
                val resp = FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            });
        }
	    
	    def pushException(t:CheckedThrowable):void {
	        if (localFinishExceptionPushed(t)) {
	            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
	            return;
	        }
	        
	        if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
	        val req = FinishRequest.makeExcpRequest(id, parentId, DUMMY_INT, t);
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
	        val lc = localCount().decrementAndGet();

	        if (lc > 0) {
	            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination() called, decremented localCount to "+lc);
	            return;
	        }
	        
	        if (lc < 0) {
	            for (var i:Long = 0 ; i < 100; i++) {
	                debug("FATAL ERROR: Root(id="+id+").notifyActivityTermination() reached a negative local count");
	                assert false: "FATAL ERROR: Root(id="+id+").notifyActivityTermination() reached a negative local count";
	            }
	        }
	        
	        // If this is not the root finish, we are done with the finish state.
	        // If this is the root finish, it will be kept alive because waitForFinish
	        // is an instance method and it is on the stack of some activity.
	        forgetGlobalRefs();
	        
	        if (localFinishReleased()) {
	        	if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination() returning");
	        	return;
	        }
	        val srcId = srcPlace.id as Int;
	        val dstId = here.id as Int;
	    	if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
	        val req = FinishRequest.makeTermRequest(id, parentId, DUMMY_INT, srcId, dstId, kind);
	    	val resp = FinishReplicator.exec(req);
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
	        if (excs != null) throw new MultipleExceptions(excs);
	        
	        if (id == TOP_FINISH) {
	            //blocks until final replication messages sent from place 0
	            //are responded to.
	            FinishReplicator.finalizeReplication();
	        }
	    }
	}
	
    //BACKUP
	public static final class PessimisticBackupState extends FinishBackupState implements x10.io.Unserializable {
	    //instance level lock
	    val ilock = new Lock();

	    //finish id 
	    val id:Id;
	    
	    //the root parent id (potential adopter if on a different place that master)
	    val parentId:Id;
	    
	    //resilient finish counter set
	    var numActive:Long;
	    val live = new HashMap[FinishResilient.Task,Int] (); // lazily allocated
	    var transit:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
	    var _liveAdopted:HashMap[FinishResilient.Task,Int] = null; // lazily allocated 
	    var _transitAdopted:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
	    
	    //the nested finishes within this finish scope
	    var children:HashSet[Id] = null; // lazily allocated
	    
	    var isAdopted:Boolean = false;
	    
	    var adopterId:Id;
	    
	    var excs:GrowableRail[CheckedThrowable]; 
	    
	    var isReleased:Boolean = false;
	    
	    //var masterPlaceId:Int = FinishResilientPessimistic.prevPlaceId.get();
	    
	    def this(id:Id, parentId:Id) {
	        this.id = id;
	        this.numActive = 1;
	        this.parentId = parentId;
	        increment(live, Task(id.home, FinishResilient.ASYNC));
	    }
	    
	    def sync(_numActive:Long, _transit:HashMap[FinishResilient.Edge,Int],
	            _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int):void {
	        //FIXME: implement this
	    }
	    
	    public def markAsImpactedByPlaceDeath() {
	        //FIXME: implement this
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
            assert false: "must not be used in pessimistic protocol";
            return -1n;
        }
        public def markAsAdopted() {
            try {
                ilock.lock();
                isAdopted = true;
            } finally {
                ilock.unlock();
            }
        }
	       
	    def liveAdopted() {
	        if (_liveAdopted == null) _liveAdopted = new HashMap[Task,Int]();
	        return _liveAdopted;
	    }

	    def transit() {
	        if (transit == null) transit = new HashMap[Edge,Int]();
	        return transit;
	    }

	    def transitAdopted() {
	        if (_transitAdopted == null) _transitAdopted = new HashMap[Edge,Int]();
	        return _transitAdopted;
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
	    public def getNewMasterBlocking() {
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
                        //removeFromStates();
                    }
                } else {
                    decrement(transitAdopted(), e);
                    numActive--;
                    if (t != null) addExceptionUnsafe(t);
                    if (quiescent()) {
                        isReleased = true;
                        //removeFromStates();
                    }
                }
            } finally {
                ilock.unlock();
            }
            if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted(srcId=" + srcId + ",dstId=" + dstId + ") returning");
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	try {
        		ilock.lock();
        		if (verbose>=1) debug(">>>> Backup(id="+id+").liveToCompleted called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
	            val t = Task(dstId, kind);
	            if (!toAdopter) {
	                decrement(live, t);
	                numActive--;
	                if (quiescent()) {
	                	isReleased = true;
	                    //removeFromStates();
	                }
	            } else {
	            	decrement(liveAdopted(), t);
	                numActive--;
	                if (quiescent()) {
	                	isReleased = true;
	                    //removeFromStates();
	                }
	            }
	            if (verbose>=1) debug("<<<< Backup(id="+id+").liveToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
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
	        if (req.reqType == FinishRequest.ADD_CHILD) {
	            val childId = req.childId;
	            if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=ADD_CHILD, childId="+childId+"] called");
	            try{                        
	            	addChild(childId);
	            } catch (t:Exception) {
	                t.printStackTrace();
	            	resp.excp = t;
	            }
	            if (verbose>=1) debug("<<<< Backup(id="+id+").exec returning [req=ADD_CHILD, childId="+childId+"]");
	        } else if (req.reqType == FinishRequest.TRANSIT) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
	        	                    + dstId + ",kind=" + kind + ",subDPE="+req.transitSubmitDPE+" ] called");
	        	try{
	        	    if (req.transitSubmitDPE)
	        	        addDeadPlaceException(dstId);
	        	    else
	        	        inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter);
	        	} catch (t:Exception) {
	        	    t.printStackTrace();
	            	resp.excp = t;
	            }
	        	if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] returning");
	        } else if (req.reqType == FinishRequest.LIVE) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] called");
	        	try{
	        		var msg:String = kind == FinishResilient.ASYNC ? "notifyActivityCreation" : "notifyShiftedActivityCreation";
	        		transitToLive(srcId, dstId, kind, msg , toAdopter);
	        	} catch (t:Exception) {
	        	    t.printStackTrace();
	            	resp.excp = t;
	            }
	        	if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=LIVE, srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] returning");
	        } else if (req.reqType == FinishRequest.TERM) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
	        		liveToCompleted(srcId, dstId, kind, "notifyActivityTermination", toAdopter);
	        	} catch (t:Exception) {
	        	    t.printStackTrace();
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
                    t.printStackTrace();
                    resp.excp = t;
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (req.reqType == FinishRequest.TRANSIT_TERM) {
                val srcId = req.srcId;
                val dstId = req.dstId;
                val toAdopter = req.toAdopter;
                val kind = req.kind;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + ", ex="+ex+" ] called");
                try{
                    transitToCompleted(srcId, dstId, kind, ex, toAdopter);
                } catch (t:Exception) {
                    t.printStackTrace();
                    resp.excp = t;
                }
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT_TERM, srcId=" + srcId + ", dstId="
                        + dstId + ", kind=" + kind + ", ex="+ex+" ] returning");
            }
	        return resp;
	    }
	}
}