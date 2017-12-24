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

//TODO: do we need to memorize all remote objects and delete them explicitly?

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
    
    //default next place - may be updated in notifyPlaceDeath
    static val nextPlaceId = new AtomicInteger(((here.id +1)%Place.numPlaces()) as Int);

    //default previous place - may be updated in notifyPlaceDeath
    static val prevPlaceId = new AtomicInteger(((here.id -1 + Place.numPlaces())%Place.numPlaces()) as Int);
    
    private val isRoot:Boolean;
    private var remoteState:RemoteState;
    private var rootState:RootMasterState;
    
    public def toString():String { 
    	return ( isRoot? "FinishResilientPessimisticRoot(id="+rootState.id+", parentId="+rootState.parentId+")" : 
    		             "FinishResilientPessimisticRemote(parentId="+remoteState.parentId+")");
    }

    //create root finish
    public def this (parent:FinishState) {
    	isRoot = true;
    	rootState = new RootMasterState(parent);
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
    	isRoot = false;
    	val parentId = deser.readAny() as Id;
    	remoteState = new RemoteState(parentId);
    	if (verbose>=1) debug("<<<< RemoteFinish(parentId="+parentId+") created");
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        val fs = new FinishResilientPessimistic(parent);
        try {
        	glock.lock();
            fmasters.put(fs.rootState.id, fs.rootState);
        } finally {
        	glock.unlock();	
        }
        return fs;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
    	assert isRoot : "only a root finish should be serialized" ;
        if (verbose>=1) debug(">>>> serialize(id="+rootState.id+"0 called ");
        if (!rootState.isGlobal) globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(rootState.id);
        if (verbose>=1) debug("<<<< serialize(id="+rootState.id+") returning ");
    }
    
    static def findMaster(id:Id):RootMasterState {
    	if (verbose>=1) debug(">>>> findMaster(id="+id+") called ");
        try {
            glock.lock();
            val fs = fmasters.getOrElse(id, null);
            if (verbose>=1) debug(">>>> findMaster(id="+id+") returning fs="+fs);
            return fs;
        } finally {
            glock.unlock();
        }
    }
    
    static def findBackupOrThrow(id:Id):RootBackupState {
    	if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called ");
        try {
            glock.lock();
            val bs = fbackups.getOrThrow(id);
            if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") returning bs="+bs);
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    static def findOrCreateBackup(id:Id, parentId:Id):RootBackupState {
    	if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+") called ");
        try {
            glock.lock();
            var bs:RootBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
            	bs = new RootBackupState(id, parentId);
            	fbackups.put(id, bs);
            }
            if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+") returning bs="+bs);
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
	public static final class RemoteState implements x10.io.Unserializable {
	    //the direct parent of this finish (can be at same or different place)
	    val parentId:Id; //for a remote finish, it's the root finish.

	    //instance level lock
	    val ilock = new Lock();
	    
	    //root of root (potential adopter) 
	    var adopterId:Id = UNASSIGNED;
	
	    var isAdopted:Boolean = false;
	    //local tasks count
	    var count:Long; 
	    
	    public def this (parentId:Id) {
            this.count = 1;
            this.parentId = parentId;
        }
	}
	
	public static final class RootMasterState implements x10.io.Unserializable {
	    //finish id (this is a root finish)
	    val id:Id;
	    
	    //the direct parent of this finish can be (rootFin/remoteFin, here/remote)
	    val parentId:Id; 
	    
	    //the direct parent finish object (used for recursive initializing)
	    val parent:FinishState;

	    //latch for blocking and releasing the host activity
	    val latch:SimpleLatch = new SimpleLatch();

	    //resilient finish counter set
	    var numActive:Long;
	    var live:HashMap[Task,Int] = null; // lazily allocated
	    var transit:HashMap[Edge,Int] = null; // lazily allocated
	    var _liveAdopted:HashMap[Task,Int] = null; // lazily allocated 
	    var _transitAdopted:HashMap[Edge,Int] = null; // lazily allocated
	    
	    //the nested finishes within this finish scope
	    var children:HashSet[Id] = null; // lazily allocated
	    
	    //flag to indicate whether finish has been resiliently replicated or not
	    var isGlobal:Boolean = false;
	    
	    var backupPlaceId:Int = FinishResilientPessimistic.nextPlaceId.get();
	    
	    def this(parent:FinishState) {
	    	val tmpId = Id(here.id as Int, nextId.getAndIncrement());
	        this.id = tmpId;
	        this.numActive = 1;
	        this.parent = parent;
	        if (parent instanceof FinishResilientPessimistic) {
	        	if ((parent as FinishResilientPessimistic).isRoot)
	        	    parentId = (parent as FinishResilientPessimistic).rootState.id;
	        	else
	        		parentId = (parent as FinishResilientPessimistic).remoteState.parentId;
	        }
	        else {
	        	if (verbose>=1) debug("FinishResilientPessimistic(id="+tmpId+") foreign parent found of type " + parent);
	        	parentId = UNASSIGNED;
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
	    
	    def addChild(child:Id) {
	    	if (verbose>=1) debug(">>>> addChild id="+id + ", child=" + child);
	        try {
	            latch.lock();
	            if (children == null) {
	                children = new HashSet[Id]();
	            }
	            children.add(child);
	            if (verbose>=1) debug("<<<< addChild id="+ id + ", child=" + child + 
	            		              ", backup=" + backupPlaceId);
	            return backupPlaceId;
	        } finally {
	            latch.unlock();
	        }
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
	    var live:HashMap[FinishResilient.Task,Int] = null; // lazily allocated
	    var transit:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
	    var _liveAdopted:HashMap[FinishResilient.Task,Int] = null; // lazily allocated 
	    var _transitAdopted:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
	    
	    //the nested finishes within this finish scope
	    var children:HashSet[Id] = null; // lazily allocated
	    
	    var isAdopted:Boolean = false;
	    
	    var adopterId:Id;
	    
	    var masterPlaceId:Int = FinishResilientPessimistic.prevPlaceId.get();
	    
	    def this(id:Id, parentId:Id) {
	        this.id = id;
	        this.numActive = 1;
	        this.parentId = parentId;
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
	}

    private def globalInit() {
    	if (isRoot) {
    		rootState.latch.lock();
	        if (!rootState.isGlobal) {
	        	val id = rootState.id;
	        	val parent = rootState.parent;
	            if (verbose>=1) debug(">>>> doing globalInit for id="+id);
	            
	            if (parent instanceof FinishResilientPessimistic) {
	                val frParent = parent as FinishResilientPessimistic;
	                if (frParent.isRoot) frParent.globalInit();
	            }
	            
	            if (rootState.parentId != UNASSIGNED) {
	            	val req = new FinishRequest(FinishRequest.ADD_CHILD, rootState.parentId, id);
	            	val masterRes = Replicator.masterDo(req);
	            }
	            
	            rootState.isGlobal = true;
	            if (verbose>=1) debug("<<<< globalInit returning fs="+this);
	        }
	        rootState.latch.unlock();
        }
    }
    
    
    /* forward finish actions to the specialized implementation */
    def notifySubActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyRemoteContinuationCreated():void {
        
    }

    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
        return true;
    }

    def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
        return true;
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
        
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        
    }

    def notifyActivityTermination(srcPlace:Place):void {
        
    }

    def notifyShiftedActivityCompletion(srcPlace:Place):void {
        notifyActivityTermination(srcPlace);
    }

    def pushException(t:CheckedThrowable):void {
        
    }

    def waitForFinish():void {
    	assert isRoot : "fatal, waitForFinish must not be called from a remote finish" ;
        // wait for the latch release
        if (verbose>=2) debug("calling latch.await for id="+rootState.id);
        rootState.latch.await(); // wait for the termination (latch may already be released)
        if (verbose>=2) debug("returned from latch.await for id="+rootState.id);
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}

class FinishRequest {
    static val ADD_CHILD = 0;
    static val NOTIFY_ACT_TERM = 1;
    val reqType:Long;
    var childId:FinishResilient.Id;
    val id:FinishResilient.Id;
    
    var toAdopter:Boolean;
    var adopterId:FinishResilient.Id;
    
    var backupPlaceId:Int;
    
    public def this(reqType:Long, id:FinishResilient.Id,
    		childId:FinishResilient.Id) {
    	this.reqType = reqType;
    	this.id = id;
    	this.childId = childId;
    	this.toAdopter = false;
    }
    
    public def isLocal() = (id.home == here.id as Int);
    
}

class MasterResponse {
    var backupPlaceId:Int;
    var excp:Exception;
}

class BackupResponse {
    var isAdopted:Boolean;
    var adopterId:FinishResilient.Id;
    var excp:Exception;
}

class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterAndBackupDied extends Exception {}

final class Replicator {
	//a global class-level lock
    private static glock = new Lock();

    //backup mapping
    private static backupMap = new HashMap[Int, Int]();
    
    private static def getBackupPlace(masterHome:Int) {
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
    
	static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    
    //findOrCreateBackup
    public static def exec(req:FinishRequest) {
    	var toAdopter:Boolean = false;
        var adopterId:FinishResilient.Id = FinishResilient.UNASSIGNED;
    	while (true) {
	    	try {
	    		val mresp:MasterResponse;
	    	    if (!toAdopter) {
	    		    mresp = masterDo(req);
	    	    }
	    	    else {
	    	    	req.toAdopter = true;
	    			req.adopterId = adopterId;
	    			mresp = masterDo(req);
	    	    }
	    	    req.backupPlaceId = mresp.backupPlaceId;
	    	    /*
	    		val bresp = backupDo(req);
	    		if (bresp.isAdopted) {
	    			toAdopter = true;
	    			adopterId = bresp.adopterId;
	    		}*/
	    	} catch (ex:MasterDied) {
	    		val backupPlaceId = getBackupPlace(req.id.home);
	    		//go to backup and get adopter
	    	} catch (ex:BackupDied) {
	    		debug("ignoring backup failure exception");
	    	}
	    	
    	}
    }
    
    public static def masterDo(req:FinishRequest) {
        if (req.isLocal())
            return masterDoLocal(req);
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        if (req.reqType == FinishRequest.ADD_CHILD) {
            val masterId = req.id;
            val childId = req.childId;
            val master = Place(masterId.home);
            if (verbose>=1) debug(">>>> Replicator.masterRemote [req=ADD_CHILD, masterId="+masterId+", childId="+childId+"] called");
            val rCond = ResilientCondition.make(master);
            val condGR = rCond.gr;
            val closure = (gr:GlobalRef[Condition]) => {
                at (master) @Immediate("master_add_child") async {
                	var backVar:Int = -1n;
                    var exVar:Exception = null;
                    try {
                        val parent = FinishResilientPessimistic.findMaster(masterId);
                        assert (parent != null) : "fatal error, parent is null";
                        backVar = parent.addChild(childId);
                    } catch (t:Exception) {
                    	exVar = t;
                    }
                    val backup = backVar;
                    val ex = exVar;
                    at (condGR) @Immediate("master_add_child_response") async {
                        (masterRes as GlobalRef[MasterResponse]{self.home == here})().backupPlaceId = backup;
                        (masterRes as GlobalRef[MasterResponse]{self.home == here})().excp = ex;
                        condGR().release();
                    }
                }; 
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
                masterRes().excp = new DeadPlaceException(master);
            }
            rCond.forget();
            if (verbose>=1) debug("<<<< Replicator.masterRemote returning [req=ADD_CHILD, masterId="+masterId+", childId="+childId+"]");
        }
        val resp = masterRes();
        if (resp.excp != null) { 
        	if (resp.excp instanceof DeadPlaceException)
        	    throw new MasterDied();
        	else 
        		throw resp.excp;
        }
        return resp;
    }
    
    public static def masterDoLocal(req:FinishRequest) {
        val resp = new MasterResponse();
        if (req.reqType == FinishRequest.ADD_CHILD) {
            val masterId = req.id;
            val childId = req.childId;
            if (verbose>=1) debug(">>>> Replicator.masterLocal [req=ADD_CHILD, masterId="+masterId+", childId="+childId+"] called");
            val parent = FinishResilientPessimistic.findMaster(masterId);
            assert (parent != null) : "fatal error, parent is null";
            if (verbose>=1) debug(">>>> Replicator.masterLocal [req=ADD_CHILD, masterId="+masterId+", childId="+childId+"] parent=>" + parent);

            val backup = parent.addChild(childId);
            try{                        
            	resp.backupPlaceId = backup;
            } catch (t:Exception) {
            	resp.excp = t;
            }
            if (verbose>=1) debug("<<<< Replicator.masterLocal [req=ADD_CHILD, masterId="+masterId+", childId="+childId+"] returning");
            
        }
        if (resp.excp != null) { 
            throw resp.excp;
        }
        return resp;
    }
    
    public static def backupGetAdopter(backupPlaceId:Int, id:FinishResilient.Id):FinishResilient.Id {
    	if (backupPlaceId == here.id as Int) { //
    		 val bFinish = FinishResilientPessimistic.findBackupOrThrow(id);
    		 return bFinish.getAdopter();
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
                    val bFinish = FinishResilientPessimistic.findBackupOrThrow(id);
                    try {
                    	adopterIdVar = bFinish.getAdopter();
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
                }; 
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
