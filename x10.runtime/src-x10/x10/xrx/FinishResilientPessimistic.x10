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
//TODO: bulk globalInit for chain of finishes.
//TODO: createBackup: repeat if backup place is dead. block until another place is found!!
//TODO: CHECK in ResilientFinishP0 -> line 174: decrement(adopterState.live, t);  should be adopterState.liveAdopted
//TODO: there is a comment in ResilientFinishP0 says that notifyActivityCreation must not block!!!
/**
 * Distributed Resilient Finish (records transit and live tasks)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 */
class FinishResilientPessimistic extends FinishResilient implements CustomSerialization {
    //a global class-level lock
    private static glock = new Lock();

    //the set of all masters
    private static fmasters = new HashMap[Id, RootMasterState]();
    
    //the set of all backups
    private static fbackups = new HashMap[Id, RootBackupState]();
    
    //backup mapping
    private static backupMap = new HashMap[Int, Int]();
    
    //default next place - may be updated in notifyPlaceDeath
    static val nextPlaceId = new AtomicInteger(((here.id +1)%Place.numPlaces()) as Int);

    //default previous place - may be updated in notifyPlaceDeath
    static val prevPlaceId = new AtomicInteger(((here.id -1 + Place.numPlaces())%Place.numPlaces()) as Int);
    
    private val isRoot:Boolean;
    private var remoteState:RemoteState;
    private var rootState:RootMasterState;
    
    private val id:Id;
    
    private val grlc:GlobalRef[AtomicInteger];
    
    private def localCount():AtomicInteger = (grlc as GlobalRef[AtomicInteger]{self.home == here})();
    
    public def toString():String { 
    	return ( isRoot? "FinishResilientPessimisticRoot(id="+id+", parentId="+rootState.parentId+")" : 
               "FinishResilientPessimisticRemote(parentId="+id+")");
    }

    static @Inline def increment[K](map:HashMap[K,Int], k:K) {
        map.put(k, map.getOrElse(k, 0n)+1n);
    }

    static @Inline def decrement[K](map:HashMap[K,Int], k:K) {
        val oldCount = map(k);
        if (oldCount == 1n) {
             map.remove(k);
        } else {
             map(k) = oldCount-1n;
        }
    }

    //create root finish
    public def this (parent:FinishState) {
    	isRoot = true;
    	id = Id(here.id as Int, nextId.getAndIncrement());
    	grlc = GlobalRef[AtomicInteger](new AtomicInteger(1n));
    	rootState = new RootMasterState(id, parent);
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
    	isRoot = false;
    	id = deser.readAny() as Id;
    	remoteState = new RemoteState(id);
    	val lc = deser.readAny() as GlobalRef[AtomicInteger];
    	grlc = (lc.home == here) ? lc : GlobalRef[AtomicInteger](new AtomicInteger(1n));
    	if (verbose>=1) debug("<<<< RemoteFinish(id="+id+") created");
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        val fs = new FinishResilientPessimistic(parent);
        try {
        	glock.lock();
            fmasters.put(fs.id, fs.rootState);
        } finally {
        	glock.unlock();	
        }
        return fs;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
    	assert isRoot : "only a root finish should be serialized" ;
        if (verbose>=1) debug(">>>> serialize(id="+rootState.id+") called ");
        globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(id);
        ser.writeAny(grlc);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    private def forgetGlobalRefs():void {
        (grlc as GlobalRef[AtomicInteger]{self.home==here}).forget();
    }
    
	public static final class RemoteState implements x10.io.Unserializable {
		val id:Id; //parent root finish
	
	    //instance level lock
	    val ilock = new Lock();
	    
	    //root of root (potential adopter) 
	    var adopterId:Id = UNASSIGNED;
	
	    var isAdopted:Boolean = false;
	    
	    public def this (val id:Id) {
            this.id = id;
        }
	}
	
	public static final class RootMasterState implements x10.io.Unserializable {
		val id:Id;
	
	    //the direct parent of this finish (rootFin/remoteFin, here/remote)
	    val parentId:Id; 
	    
	    //the direct parent finish object (used in globalInit for recursive initializing)
	    val parent:FinishState;

	    //latch for blocking and releasing the host activity
	    val latch:SimpleLatch = new SimpleLatch();

	    //resilient finish counter set
	    var numActive:Long;
	    val live = new HashMap[Task,Int](); // lazily allocated
	    var transit:HashMap[Edge,Int] = null; // lazily allocated
	    var _liveAdopted:HashMap[Task,Int] = null; // lazily allocated 
	    var _transitAdopted:HashMap[Edge,Int] = null; // lazily allocated
	    
	    //the nested finishes within this finish scope
	    var children:HashSet[Id] = null; // lazily allocated
	    
	    //flag to indicate whether finish has been resiliently replicated or not
	    var isGlobal:Boolean = false;
	    
	    //will be updated in notifyPlaceDeath
	    var backupPlaceId:Int = FinishResilientPessimistic.nextPlaceId.get();
	    
        var excs:GrowableRail[CheckedThrowable]; 
	
	    def this(id:Id, parent:FinishState) {
	        this.id = id;
	        this.numActive = 1;
	        this.parent = parent;
	        increment(live, Task(id.home, FinishResilient.ASYNC));
	        if (parent instanceof FinishResilientPessimistic) {
	            parentId = (parent as FinishResilientPessimistic).id;
	        }
	        else {
	        	if (verbose>=1) debug("FinishResilientPessimistic(id="+id+") foreign parent found of type " + parent);
	        	parentId = UNASSIGNED;
	        }
	    }
	    
        def dump() {
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
	    
	    def addException(t:CheckedThrowable) {
	    	try {
        		latch.lock();
        		if (excs == null) excs = new GrowableRail[CheckedThrowable]();
        		excs.add(t);
	    	} finally {
        		latch.unlock();
        	}
	    }
	    
        def addDeadPlaceException(placeId:Long) {
            val e = new DeadPlaceException(Place(placeId));
            addException(e);
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
	    
	    def addChild(child:Id) {
	    	if (verbose>=1) debug(">>>> Master.addChild id="+id + ", child=" + child + " called");
	        try {
	            latch.lock();
	            if (children == null) {
	                children = new HashSet[Id]();
	            }
	            children.add(child);
	            if (verbose>=1) debug("<<<< Master.addChild returning id="+ id + ", child=" + child + 
	            		              ", backup=" + backupPlaceId);
	            return backupPlaceId;
	        } finally {
	            latch.unlock();
	        }
	    }
	    
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	if (verbose>=1) debug(">>>> Master.inTransit id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " called");
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
	                debug("==== Master.inTransit "+tag+"(id="+id+") after update for: "+srcId + " ==> "+dstId+" kind="+kind);
	                dump();
	            }
	            if (verbose>=1) debug("<<<< Master.inTransit returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
	            return backupPlaceId;
        	} finally {
        		 latch.unlock();
        	}
        }
        
        def transitToLive(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	if (verbose>=1) debug(">>>> Master.transitToLive id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " called");
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
	                debug("==== Master.transitToLive "+tag+"(id="+id+") after update for: "+srcId + " ==> "+dstId+" kind="+kind);
	                dump();
	            }
	            if (verbose>=1) debug("<<<< Master.transitToLive returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
	            return backupPlaceId;
        	} finally {
        		latch.unlock();
        	}
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	if (verbose>=1) debug(">>>> Master.liveToCompleted id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " called");
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
	            if (verbose>=1) debug("<<<< Master.liveToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
	            return backupPlaceId;
        	} finally {
        		latch.unlock();
        	}
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Master.quiescent(id="+id+") called");
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=2) debug("quiescent(id="+id+") returning false, already adopted by adopterId=="+adopterId);
                return false;
            }
            */
            if (numActive < 0) {
                debug("COUNTING ERROR: quiescent(id="+id+") negative numActive!!!");
                dump();
                assert false : "COUNTING ERROR: quiescent(id="+id+") negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            if (verbose>=3) dump();
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Master.quiescent(id="+id+") returning " + quiet);
            return quiet;
        }
        
        def releaseLatch() {
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=1) debug("releaseLatch(id="+id+") called on adopted finish; not releasing latch");
            } else */
            {
                val exceptions = (excs == null || excs.isEmpty()) ?  null : excs.toRail();
                if (verbose>=2) debug("releasing latch id="+id+(exceptions == null ? " no exceptions" : " with exceptions"));
                if (exceptions != null) {
                    if (excs == null) excs = new GrowableRail[CheckedThrowable](exceptions.size);
                    excs.addAll(exceptions);
                }
                latch.release();
            }
            if (verbose>=2) debug("releaseLatch(id="+id+") returning");
        }
        
	    public def exec(req:FinishRequest) {
	    	val id = req.id;
	        val resp = new MasterResponse();
	        if (req.reqType == FinishRequest.ADD_CHILD) {
	            val childId = req.childId;
	            if (verbose>=1) debug(">>>> Master.exec [req=ADD_CHILD, masterId="+id+", childId="+childId+"] called");
	            try{                        
	            	resp.backupPlaceId = addChild(childId);
	            } catch (t:Exception) {
	            	resp.backupPlaceId = -1n;
	            	resp.excp = t;
	            }
	            if (verbose>=1) debug("<<<< Master.exec returning [req=ADD_CHILD, masterId="+id+", childId="+childId+"]");
	        } else if (req.reqType == FinishRequest.TRANSIT) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	resp.transit_ok = false;
	        	if (verbose>=1) debug(">>>> Master.exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
		        	if (Place(srcId).isDead()) {
	                    if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") src "+srcId + "is dead; dropping async");
	                } else if (Place(dstId).isDead()) {
	                    if (kind == FinishResilient.ASYNC) {
	                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; pushed DPE for async");
	                        addDeadPlaceException(dstId);
	                    } else {
	                        if (verbose>=1) debug("==== notifySubActivitySpawn(id="+id+") destination "+dstId + "is dead; dropped at");
	                    }
	                } else {
	                	resp.backupPlaceId = inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter);
	                	resp.transit_ok = true;
	                }
	        	} catch (t:Exception) {
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master.exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", transit_ok="+resp.transit_ok+" ] returning");
	        } else if (req.reqType == FinishRequest.LIVE) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	resp.live_ok = false;
	        	if (verbose>=1) debug(">>>> Master.exec [req=LIVE, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
	        		var msg:String = kind == FinishResilient.ASYNC ? "notifyActivityCreation":"notifyShiftedActivityCreation";
	        		if (Place(srcId).isDead() || Place(dstId).isDead()) {
		                // NOTE: no state updates or DPE processing here.
		                //       Must happen exactly once and is done
		                //       when Place0 is notified of a dead place.
	        			if (verbose>=1) debug("==== "+msg+"(id="+id+") suppressed: "+srcId + " ==> "+dstId+" kind="+kind);
	        		} else {
	        			resp.backupPlaceId = transitToLive(srcId, dstId, kind, msg, toAdopter);
	        			resp.live_ok = true;
	        		}
	        	} catch (t:Exception) {
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master.exec [req=LIVE, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", live_ok=" + resp.live_ok + " ] returning");
	        } else if (req.reqType == FinishRequest.TERM) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Master.exec [req=TERM, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
	        		if (Place(dstId).isDead()) {
	                    // NOTE: no state updates or DPE processing here.
	                    //       Must happen exactly once and is done
	                    //       when Place0 is notified of a dead place.
	                    if (verbose>=1) debug("==== notifyActivityTermination(id="+id+") suppressed: "+dstId+" kind="+kind);
	                } else {
	        			resp.backupPlaceId = liveToCompleted(srcId, dstId, kind, "notifyActivityTermination", toAdopter);
	                }
	        	} catch (t:Exception) {
	        		resp.backupPlaceId = -1n;
	            	resp.excp = t;
	        	}
	        	if (verbose>=1) debug("<<<< Master.exec [req=TERM, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + ", live_ok=" + resp.live_ok + " ] returning");
	        }
	        return resp;
	    }
	}
	
	public static final class RootBackupState implements x10.io.Unserializable {
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
	    
	    def addException(t:CheckedThrowable) {
	    	try {
	    		ilock.lock();
        		if (excs == null) excs = new GrowableRail[CheckedThrowable]();
        		excs.add(t);
	    	} finally {
	    		ilock.unlock();
        	}
	    }
	    
        def addDeadPlaceException(placeId:Long) {
            val e = new DeadPlaceException(Place(placeId));
            addException(e);
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
	    
	    def addChild(child:Id) {
	    	if (verbose>=1) debug(">>>> Backup.addChild called (id="+id + ", child=" + child + ")");
	        try {
	        	ilock.lock();
	            if (children == null) {
	                children = new HashSet[Id]();
	            }
	            children.add(child);
	            if (verbose>=1) debug("<<<< Backup.addChild returning (id="+ id + ", child=" + child +")");
	        } finally {
	        	ilock.unlock();
	        }
	    }
	    
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	try {
        		ilock.lock();
        		if (verbose>=1) debug(">>>> Backup.inTransit called (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
	            val e = Edge(srcId, dstId, kind);
	            if (!toAdopter) {
	                increment(transit(), e);
	                numActive++;
	            } else {
	                increment(transitAdopted(), e);
	                numActive++;
	            }
	            if (verbose>=1) debug("<<<< Backup.inTransit returning (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
        	} finally {
        		ilock.unlock();
        	}
        }
        
        def transitToLive(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	try {
        		ilock.lock();
        		if (verbose>=1) debug(">>>> Backup.transitToLive called (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
	            val e = Edge(srcId, dstId, kind);
	            val t = Task(dstId, kind);
	            if (!toAdopter) {
	                increment(live,t);
	                decrement(transit(), e);
	            } else {
	                increment(liveAdopted(), t);
	                decrement(transitAdopted(), e);
	            }
	            if (verbose>=1) debug("<<<< Backup.transitToLive returning (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
        	} finally {
        		ilock.unlock();
        	}
        }
        
        def liveToCompleted(srcId:Long, dstId:Long, kind:Int, tag:String, toAdopter:Boolean) {
        	try {
        		ilock.lock();
        		if (verbose>=1) debug(">>>> Backup.liveToCompleted called (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
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
	            if (verbose>=1) debug("<<<< Backup.liveToCompleted returning (id="+id + ", numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
        	} finally {
        		ilock.unlock();
        	}
        }
        
        def quiescent():Boolean {
            if (verbose>=2) debug(">>>> Backup.quiescent(id="+id+") called");
            /* TODO: revise this
            if (isAdopted()) {
                if (verbose>=2) debug("quiescent(id="+id+") returning false, already adopted by adopterId=="+adopterId);
                return false;
            }
            */
            if (numActive < 0) {
                debug("COUNTING ERROR: quiescent(id="+id+") negative numActive!!!");
                assert false : "COUNTING ERROR: quiescent(id="+id+") negative numActive!!!";
                return true; // TODO: This really should be converted to a fatal error....
            }
        
            val quiet = numActive == 0;
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Backup.quiescent(id="+id+") returning " + quiet);
            return quiet;
        }
	    
	    public def exec(req:FinishRequest) {
	        val resp = new BackupResponse();
	        if (req.reqType == FinishRequest.ADD_CHILD) {
	            val childId = req.childId;
	            if (verbose>=1) debug(">>>> Backup.exec [req=ADD_CHILD, id="+id+", childId="+childId+"] called");
	            try{                        
	            	addChild(childId);
	            } catch (t:Exception) {
	            	resp.excp = t;
	            }
	            if (verbose>=1) debug("<<<< Backup.exec returning [req=ADD_CHILD, id="+id+", childId="+childId+"]");
	        } else if (req.reqType == FinishRequest.TRANSIT) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup.exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
	        	                    + dstId + ",kind=" + kind + " ] called");
	        	try{
	        	    inTransit(srcId, dstId, kind, "notifySubActivitySpawn", toAdopter);
	        	} catch (t:Exception) {
	            	resp.excp = t;
	            }
	        	if (verbose>=1) debug("<<<< Backup.exec [req=TRANSIT, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] returning");
	        } else if (req.reqType == FinishRequest.LIVE) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup.exec [req=LIVE, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] called");
	        	try{
	        		var msg:String = kind == FinishResilient.ASYNC ? "notifyActivityCreation" : "notifyShiftedActivityCreation";
	        		transitToLive(srcId, dstId, kind, msg , toAdopter);
	        	} catch (t:Exception) {
	            	resp.excp = t;
	            }
	        	if (verbose>=1) debug("<<<< Backup.exec [req=LIVE, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ",kind=" + kind + " ] returning");
	        } else if (req.reqType == FinishRequest.TERM) {
	        	val srcId = req.srcId;
	        	val dstId = req.dstId;
	        	val toAdopter = req.toAdopter;
	        	val kind = req.kind;
	        	if (verbose>=1) debug(">>>> Backup.exec [req=TERM, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] called");
	        	try{
	        		liveToCompleted(srcId, dstId, kind, "notifyActivityTermination", toAdopter);
	        	} catch (t:Exception) {
	            	resp.excp = t;
	            }
	        	if (verbose>=1) debug("<<<< Backup.exec [req=TERM, id=" + id + ", srcId=" + srcId + ", dstId="
	                    + dstId + ", kind=" + kind + " ] returning");
	        }
	        return resp;
	    }
	}

    static def findMaster(id:Id):RootMasterState {
    	if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            glock.lock();
            val fs = fmasters.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return fs;
        } finally {
            glock.unlock();
        }
    }
    
    static def findBackupOrThrow(id:Id):RootBackupState {
    	if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            glock.lock();
            val bs = fbackups.getOrThrow(id);
            if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning");
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    /*static def releaseBackup(id:Id) {
    	if (verbose>=1) debug(">>>> releaseBackup(id="+id+") called ");
        try {
            glock.lock();
            val bs = fbackups.getOrThrow(id);
            bs.release();
            if (verbose>=1) debug("<<<< releaseBackup(id="+id+") returning bs="+bs);
        } finally {
            glock.unlock();
        }
    }*/
    
    static def findOrCreateBackup(id:Id, parentId:Id):RootBackupState {
    	if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+") called ");
        try {
            glock.lock();
            var bs:RootBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
            	bs = new RootBackupState(id, parentId);
            	fbackups.put(id, bs);
            }
            if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+") returning bs="+bs);
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    //used only when master replica is found dead
    public static def getBackupPlace(masterHome:Int) {
    	try {
    		glock.lock();
    		val backup = backupMap.getOrElse (masterHome, -1n);
    		if (backup == -1n) {
    			return FinishResilientPessimistic.nextPlaceId.get();
    		} 
    		else {
    			return backup;
    		}
    	} finally {
    		glock.unlock();
    	}
    }
    
    private def globalInit() {
    	if (isRoot) {
    		rootState.latch.lock();
	        if (!rootState.isGlobal) {
	        	val parent = rootState.parent;
	            if (verbose>=1) debug(">>>> doing globalInit for id="+id);
	            
	            if (parent instanceof FinishResilientPessimistic) {
	                val frParent = parent as FinishResilientPessimistic;
	                if (frParent.isRoot) frParent.globalInit();
	            }
	            
	            if (rootState.parentId != UNASSIGNED) {
	            	val req = new FinishRequest(FinishRequest.ADD_CHILD, rootState.parentId, id);
	            	Replicator.exec(req);
	            }
	            
	            createBackup();
	            
	            rootState.isGlobal = true;
	            if (verbose>=1) debug("<<<< globalInit returning fs="+this);
	        }
	        rootState.latch.unlock();
        }
    }
    
    private def createBackup() {
    	//TODO: redo if backup dies
    	if (verbose>=1) debug(">>>> createBackup called fs="+this);
     	val backup = Place(rootState.getBackupPlaceId());
     	if (backup.isDead()) {
     		if (verbose>=1) debug("<<<< createBackup returning fs="+this + " dead backup");
     		return false;
     	}
     	val myId = id; //don't copy this
     	val parentId = rootState.parentId;
     	val backRes = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_create") async {
                val backFin = FinishResilientPessimistic.findOrCreateBackup(myId, parentId);
                val success = backFin != null;
                at (condGR) @Immediate("backup_create_response") async {
                	val bRes = (backRes as GlobalRef[Cell[Boolean]]{self.home == here})();
                	bRes.value = success;
                    condGR().release();
                }
            }; 
        };
        rCond.run(closure);
        /*TODO: complete the below lines*/
        if (rCond.failed()) {
            val excp = new DeadPlaceException(backup);
        }
        rCond.forget();
        return true;
    }
    
    def notifySubActivitySpawn(place:Place):void {
        notifySubActivitySpawn(place, ASYNC);
    }
    
    def notifyShiftedActivitySpawn(place:Place):void {
        notifySubActivitySpawn(place, AT);
    }
    
    def notifySubActivitySpawn(dstPlace:Place, kind:Int):void {
    	val myId = id;
    	val srcId = here.id as Int;
        val dstId = dstPlace.id as Int;
        if (dstId == here.id as Int) {
        	val lc = localCount().incrementAndGet();
            if (verbose>=1) debug(">>>> notifySubActivitySpawn(id="+myId+",isRoot="+isRoot+") called locally, localCount now "+lc);
        } else {
        	val parentId = isRoot? myId: remoteState.id;
        	if (verbose>=1) debug(">>>> notifySubActivitySpawn(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") called, srcId="+here.id + " dstId="+dstId+" kind="+kind);
        	val req = new FinishRequest(FinishRequest.TRANSIT, parentId, srcId, dstId, kind);
        	Replicator.exec(req);
        	if (verbose>=1) debug("<<<< notifySubActivitySpawn(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") returning, srcId="+here.id + " dstId="+dstId+" kind="+kind);
        }
    }
    
    /*
     * This method can't block because it may run on an @Immediate worker.  
     * Instead we create an uncounted activity to update the counter set at master and backup replicas
     */
    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
        val srcId = srcPlace.id as Int; 
        val dstId = here.id as Int;
        val myId = id;
        if (srcId == dstId) {
            if (verbose>=1) debug(">>>> notifyActivityCreation(id="+myId+") called locally. no action required");
            return true;
        }
        
        Runtime.submitUncounted( ()=>{
        	val parentId = isRoot? myId: remoteState.id;
        	val kind = FinishResilient.ASYNC;
        	if (verbose>=1) debug(">>>> notifyActivityCreation(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") called, srcId=" + srcId + " dstId="+dstId+" kind="+kind);
        	val req = new FinishRequest(FinishRequest.LIVE, parentId, srcId, dstId, kind);
        	val resp = Replicator.exec(req);
        	if (verbose>=1) debug("<<<< notifyActivityCreation(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") returning (submit="+resp.live_ok+"), srcId=" + srcId + " dstId="+dstId+" kind="+kind);
            if (resp.live_ok) {
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
        val myId = id;
    	val parentId = isRoot? myId: remoteState.id;
    	val kind = FinishResilient.AT;
    	if (verbose>=1) debug(">>>> notifyActivityCreation(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") called, srcId=" + srcId + " dstId="+dstId+" kind="+kind);
    	val req = new FinishRequest(FinishRequest.LIVE, parentId, srcId, dstId, kind);
    	val resp = Replicator.exec(req);
    	if (verbose>=1) debug("<<<< notifyActivityCreation(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") returning (submit="+resp.live_ok+"), srcId=" + srcId + " dstId="+dstId+" kind="+kind);
        return resp.live_ok;
    }
    
    def notifyRemoteContinuationCreated():void {
        
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
        
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        
    }

    def notifyActivityTermination(srcPlace:Place):void {
        notifyActivityTermination(srcPlace, ASYNC);
    }
    def notifyShiftedActivityCompletion(srcPlace:Place):void {
        notifyActivityTermination(srcPlace, AT);
    }
    def notifyActivityTermination(srcPlace:Place, kind:Int):void {
        val lc = localCount().decrementAndGet();
        val myId = this.id;

        if (lc > 0) {
            if (verbose>=1) debug(">>>> notifyActivityTermination(id="+myId+") called, decremented localCount to "+lc);
            return;
        }

        // If this is not the root finish, we are done with the finish state.
        // If this is the root finish, it will be kept alive because waitForFinish
        // is an instance method and it is on the stack of some activity.
        forgetGlobalRefs();
        
        if (isRoot && rootState.localFinishReleased()) {
        	if (verbose>=1) debug("<<<< notifyActivityTermination(id="+myId+",isRoot="+isRoot+") returning");
        	return;
        }
        
        val srcId = srcPlace.id as Int; 
        val dstId = here.id as Int;
    	val parentId = isRoot? myId: remoteState.id;
    	if (verbose>=1) debug(">>>> notifyActivityTermination(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") called, srcId="+here.id + " dstId="+dstId+" kind="+kind);
    	val req = new FinishRequest(FinishRequest.TERM, parentId, srcId, dstId, kind);
    	val resp = Replicator.exec(req);
    	if (verbose>=1) debug("<<<< notifyActivityTermination(id="+myId+",isRoot="+isRoot+", parentId="+parentId+") returning, srcId="+here.id + " dstId="+dstId+" kind="+kind);
    }

    def pushException(t:CheckedThrowable):void {
        
    }

    def waitForFinish():void {
    	assert isRoot : "fatal, waitForFinish must not be called from a remote finish" ;
		if (verbose>=1) debug(">>>> waitForFinish(id="+id+") called, lc = " + localCount().get() );

        // terminate myself
        notifyActivityTermination(here);

        // If we haven't gone remote with this finish yet, see if this worker
        // can execute other asyncs that are governed by the finish before waiting on the latch.
        /*TODO: revise this 
        if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS || !strictFinish)) {
            if (verbose>=2) debug("calling worker.join for id="+id);
            Runtime.worker().join(this.latch);
        }*/

        // wait for the latch release
        if (verbose>=2) debug("calling latch.await for id="+id);
        rootState.latch.await(); // wait for the termination (latch may already be released)
        if (verbose>=2) debug("returned from latch.await for id="+id);

        // no more messages will come back to this finish state 
        forgetGlobalRefs();
        
        // get exceptions and throw wrapped in a ME if there are any
        if (rootState.excs != null) throw new MultipleExceptions(rootState.excs);
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        var str:String = "";
        for (p in Place.places()) {
            str += "place(" + p.id + ")=" + p.isDead() + " "; 
        }
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning [isDead: "+str+"]");
    }
}

class FinishRequest {
    static val ADD_CHILD = 0;
    static val TRANSIT = 1;
    static val LIVE = 2;
    static val TERM = 3;
    
    val reqType:Long;
    val id:FinishResilient.Id;
    
    //add child request
    var childId:FinishResilient.Id;
    
    //transit/live/term request
    var srcId:Int;
    var dstId:Int;
    var kind:Int;
    
    //redirect to adopter
    var toAdopter:Boolean = false;
    var adopterId:FinishResilient.Id;
    
    var backupPlaceId:Int = -1n;
    
    public def toString() {
    	var typeDesc:String = "";
        if (reqType == ADD_CHILD)
        	typeDesc = "ADD_CHILD";
        else if (reqType == TRANSIT)
        	typeDesc = "TRANSIT";
        else if (reqType == LIVE)
        	typeDesc = "LIVE" ;
        else if (reqType == TERM)
        	typeDesc = "TERM" ; 
        return "type=" + typeDesc + ",id="+id+",childId="+childId+",srcId="+srcId+",dstId="+dstId;
    }
    
    //ADD_CHILD
    public def this(reqType:Long, id:FinishResilient.Id,
    		childId:FinishResilient.Id) {
    	this.reqType = reqType;
    	this.id = id;
    	this.childId = childId;
    	this.toAdopter = false;
    }
    
    //TRANSIT/LIVE/TERM
    public def this(reqType:Long, id:FinishResilient.Id,
    		srcId:Int, dstId:Int, kind:Int) {
    	this.reqType = reqType;
    	this.id = id;
    	this.toAdopter = false;
    	this.srcId = srcId;
    	this.dstId = dstId;
    	this.kind = kind;
    }
    
    public def isLocal() = (id.home == here.id as Int);
    
}

class MasterResponse {
    var backupPlaceId:Int;
    var excp:Exception;
    var transit_ok:Boolean = false;
    var live_ok:Boolean = false;
}

class BackupResponse {
    var isAdopted:Boolean;
    var adopterId:FinishResilient.Id;
    var excp:Exception;
}

class ReplicatorResponse {
	var transit_ok:Boolean = false;
	var live_ok:Boolean = false;
}

class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterAndBackupDied extends Exception {}

final class Replicator {
	//a global class-level lock
    private static glock = new Lock();
    
	static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    
    public static def exec(req:FinishRequest):ReplicatorResponse {
    	val repResp = new ReplicatorResponse();
    	var toAdopter:Boolean = false;
        var adopterId:FinishResilient.Id = FinishResilient.UNASSIGNED;
    	while (true) {
	    	try {
	    		val mresp:MasterResponse;
	    	    if (!toAdopter) {
	    		    mresp = masterExec(req);
	    	    } else {
	    	    	req.toAdopter = true;
	    			req.adopterId = adopterId;
	    			mresp = masterExec(req);
	    	    }
	    	    repResp.transit_ok = mresp.transit_ok;
	    	    repResp.live_ok = mresp.live_ok;
	    	    if (verbose>=1) debug(">>>> Replicator.exec() masterDone =>"
	    	    		+ " transit_ok = " + repResp.transit_ok 
	    	    		+ " live_ok = " + repResp.live_ok );
	    	    if (mresp.backupPlaceId != -1n) {
		    	    req.backupPlaceId = mresp.backupPlaceId;
		    		val bresp = backupExec(req);
		    		if (bresp.isAdopted) {
		    			toAdopter = true;
		    			adopterId = bresp.adopterId;
		    		}
	    	    }
	    	    break;
	    	} catch (ex:MasterDied) {
	    		val backupPlaceId = FinishResilientPessimistic.getBackupPlace(req.id.home);
	    		adopterId = backupGetAdopter(backupPlaceId, req.id);
	    		toAdopter = true;
	    	} catch (ex:BackupDied) {
	    		debug("ignoring backup failure exception");
	    		break; //master should re-replicate
	    	}
	    	
    	}
    	return repResp;
    }
    
    public static def masterExec(req:FinishRequest) {
    	if (verbose>=1) debug(">>>> Replicator.masterExec called [" + req + "]" );
        if (req.isLocal()) {
        	val parent = FinishResilientPessimistic.findMaster(req.id);
            assert (parent != null) : "fatal error, parent is null";
            val resp = parent.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator.masterExec returning [" + req + "]" );
            return resp;
        }
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.id.home);
        val rCond = ResilientCondition.make(master);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val parent = FinishResilientPessimistic.findMaster(req.id);
                assert (parent != null) : "fatal error, parent is null";
                val resp = parent.exec(req);
                val r_back = resp.backupPlaceId;
                val r_liveok = resp.live_ok;
                val r_transitok = resp.transit_ok;
                val r_exp = resp.excp;
                at (condGR) @Immediate("master_exec_response") async {
                	val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                	mRes.backupPlaceId = r_back;
                	mRes.live_ok = r_liveok;
                	mRes.transit_ok = r_transitok;
                	mRes.excp = r_exp;
                    condGR().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (rCond.failed()) {
            masterRes().excp = new DeadPlaceException(master);
        }
        rCond.forget();
        val resp = masterRes();
        if (resp.excp != null) { 
        	if (resp.excp instanceof DeadPlaceException) {
        	    throw new MasterDied();
        	}
        	else {
        		throw resp.excp;
        	}
        }
        if (verbose>=1) debug("<<<< Replicator.masterExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupExec(req:FinishRequest) {
    	if (verbose>=1) debug(">>>> Replicator.backupExec called [" + req + "]" );
        if (req.backupPlaceId == here.id as Int) {
        	val bFin = FinishResilientPessimistic.findBackupOrThrow(req.id);
            val resp = bFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator.backupExec returning [" + req + "]" );
            return resp;
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(req.backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
                val parentBackup = FinishResilientPessimistic.findBackupOrThrow(req.id);
                val resp = parentBackup.exec(req);
                val r_isAdopt = resp.isAdopted;
                val r_adoptId = resp.adopterId;
                val r_excp = resp.excp;
                at (condGR) @Immediate("backup_exec_response") async {
                	val bRes = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                	bRes.isAdopted = r_isAdopt;
                	bRes.adopterId = r_adoptId;
                	bRes.excp = r_excp;
                    condGR().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (rCond.failed()) {
        	backupRes().excp = new DeadPlaceException(backup);
        }
        
        rCond.forget();
        val resp = backupRes();
        if (resp.excp != null) { 
        	if (resp.excp instanceof DeadPlaceException)
        	    throw new BackupDied();
        	else 
        		throw resp.excp;
        }
        if (verbose>=1) debug("<<<< Replicator.backupExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupGetAdopter(backupPlaceId:Int, id:FinishResilient.Id):FinishResilient.Id {
    	if (backupPlaceId == here.id as Int) { //
    		 val bFin = FinishResilientPessimistic.findBackupOrThrow(id);
    		 return bFin.getAdopter();
    	}
    	else {
    		val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
    		val backup = Place(backupPlaceId);
    		//we cannot use Immediate activities, because this function is blocking
            val rCond = ResilientCondition.make(backup);
            val condGR = rCond.gr;
            val closure = (gr:GlobalRef[Condition]) => {
                at (backup) @Uncounted async {
                	var adopterIdVar:FinishResilient.Id = FinishResilient.UNASSIGNED;
                    var exVar:Exception = null;
                    val bFin = FinishResilientPessimistic.findBackupOrThrow(id);
                    try {
                    	adopterIdVar = bFin.getAdopter();
                    } catch (t:Exception) {
                    	exVar = t;
                    }
                    val ex = exVar;
                    val adopterId = adopterIdVar;
                    at (condGR) @Immediate("backup_get_adopter_response") async {
                    	val resp = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                        resp.isAdopted = true;
                        resp.adopterId = adopterId;
                        resp.excp = ex;
                        condGR().release();
                    }
                }
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
            	backupRes().excp = new MasterAndBackupDied();
            }
            rCond.forget();
            return backupRes().adopterId;
    	}
    }
} 
