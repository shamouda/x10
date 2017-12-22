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

/**
 * Distributed Resilient Finish (records transit and live tasks)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 */
class FinishResilientPessimistic extends FinishResilient implements CustomSerialization {    
    // local finish object
    protected transient var me:FinishResilient; 

    //create root finish
    public def this (me:PessimisticRootFinishMaster) {
        this.me = me;
    }
    
    public def toString():String = me == null ? "NULL(FinishResilientPessimistic)" : me.toString();
    
    //make root finish    
    static def make(parent:FinishState) {
        val me = new PessimisticRootFinishMaster(parent);
        val fs = new FinishResilientPessimistic(me);
        return fs;
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        val rootId = deser.readAny() as Id;
        me = new PessimisticFinishRemote(rootId);
    }
    
    //serialize root finish
    public def serialize(ser:Serializer) {
        if (me instanceof PessimisticRootFinishMaster)
            (me as PessimisticRootFinishMaster).serialize(ser);
        else assert false;
    }
    
    /* forward finish actions to the specialized implementation */
    def notifySubActivitySpawn(dstPlace:Place) { me.notifySubActivitySpawn(dstPlace); }
    def notifyShiftedActivitySpawn(dstPlace:Place) { me.notifyShiftedActivitySpawn(dstPlace); }
    def notifyRemoteContinuationCreated() { me.notifyRemoteContinuationCreated(); }
    def notifyActivityCreation(srcPlace:Place, activity:Activity) { return me.notifyActivityCreation(srcPlace, activity); }
    def notifyShiftedActivityCreation(srcPlace:Place) { return me.notifyShiftedActivityCreation(srcPlace); }
    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable) { me.notifyActivityCreationFailed(srcPlace, t); }
    def notifyActivityCreatedAndTerminated(srcPlace:Place) { me.notifyActivityCreatedAndTerminated(srcPlace); }
    def notifyActivityTermination(srcPlace:Place) { me.notifyActivityTermination(srcPlace); }
    def notifyShiftedActivityCompletion(srcPlace:Place) { me.notifyShiftedActivityCompletion(srcPlace); }
    def pushException(t:CheckedThrowable) { me.pushException(t); }
    def waitForFinish() { me.waitForFinish(); }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}
//TODO: do we need to memorize all remote objects and delete them explicitly?
final class PessimisticFinishRemote extends FinishResilient {
    //root finish
    public transient val rootId:Id;

    //root of root (potential adopter) 
    private transient var adopterId:Id = UNASSIGNED;

    private transient var isAdopted:Boolean = false;

    //local tasks count
    private transient var count:Long; 
    
    //instance level lock
    private val ilock = new Lock();

    public def toString():String = "PessimisticFinishRemote(rootId="+rootId+")";
    
    public def this (rootId:Id) {
        this.rootId = rootId;
        this.count = 1;
        if (verbose>=1) debug("<<<< PessimisticFinishRemote(rootId="+rootId+") created");
    }
    
    def notifySubActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyRemoteContinuationCreated():void {
        // no-op for remote finish
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
        assert false;
    }
}

class FinishRequest {
    public static val ADD_CHILD = 0;
    public static val NOTIFY_ACT_TERM = 1;
    
    private val type:Long;
    private var childId:Id;
    private val masterId:Id;
    
    public def isLocal() = (masterId.home = here.id);
    
}

class MasterResponse {
    var backupPlaceId:Int;
    var excp:Exception;
}

final class Replicator {
    
    public static def masterDo(req:FinishRequest) {
        if (req.isLocal())
            return masterDoLocal(req);
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        if (req.type == FinishRequest.ADD_CHILD) {
            val masterId = req.masterId;
            val childId = req.childId;
            val master = Place(masterId.home);
            val condGR = ResilientCondition.makeGR(master);
            val closure = (gr:GlobalRef[Condition]) => { 
                at (master) @Immediate("master_add_child") async {
                    try {
                        val parent = find(parentId);
                        assert (parent != null) : "fatal error, parent is null";
                        val backup = parent.addChild(childId);
                        
                        at (condGR) @Immediate("master_add_child_success") async {
                            masterRes().backupPlaceId = backup;
                            condGR().release();
                        }
                    } catch (t:Exception) {
                        at (condGR) @Immediate("master_add_child_failure") async {
                            masterRes().excp = t;
                            condGR().release();
                        };
                    }
                }; 
            };
            
            cond.run(closure);
            
            if (cond().failed()) {
                masterRes().excp = new DeadPlaceException(Place(-1n));
            }
            cond.forget();
        }
        return masterRes();
    }
    
    public static def masterDoLocal(req:FinishRequest) {
        val masterRes = new MasterResponse();
        if (req.type == FinishRequest.ADD_CHILD) {
            val parentId = req.parentId;
            val childId = req.childId;
            val parent = find(parentId);
            assert (parent != null) : "fatal error, parent is null";
            
            val backup = parent.addChild(childId);
            try{                        
                masterRes.backupPlaceId = backup;
            } catch (t:Exception) {
                masterRes.excp = t;
            }

            if (masterRes.excp == null) {
                
            }
        }
    }
} 
final class PessimisticRootFinishMaster extends FinishResilient {
    //a global class-level lock
    private static glock = new Lock();

    //the set of all masters
    private static fmasters = new HashMap[Id, PessimisticRootFinishMaster]();
    
    //backup place is next place by default
    private static val backupPlaceId = new AtomicInteger(((here.id +1)%Place.numPlaces()) as Int);

    //finish id (this is a root finish)
    private val id:Id;
    
    //the direct parent of this finish (can be at same or different place)
    private val parentId:Id;
    
    //the direct parent finish object (used for recursive initializing)
    private val parent:FinishState;

    //latch for blocking and releasing the host activity
    private transient val latch:SimpleLatch = new SimpleLatch();

    //resilient finish counter set
    private transient var numActive:Long;
    private transient var live:HashMap[Task,Int] = null; // lazily allocated
    private transient var transit:HashMap[Edge,Int] = null; // lazily allocated
    private transient var _liveAdopted:HashMap[Task,Int] = null; // lazily allocated 
    private transient var _transitAdopted:HashMap[Edge,Int] = null; // lazily allocated
    
    //the nested finishes within this finish scope
    private transient var children:HashSet[Id] = null; // lazily allocated
    
    //flag to indicate whether finish has been resiliently replicated or not
    private transient var isGlobal:Boolean = false;

    public def toString():String = "PessimisticMaster(id="+id+", parentId="+parentId+")";
    
    public def this (parent:FinishState) {
        this.id = Id(here.id as Int, nextId.getAndIncrement());
        this.numActive = 1;
        this.parent = parent;
        if (parent instanceof PessimisticRootFinishMaster)
            parentId = (parent as PessimisticRootFinishMaster).id;
        else if (parent instanceof PessimisticFinishRemote)
            parentId = (parent as PessimisticFinishRemote).rootId;
        else 
            parentId = UNASSIGNED;
    }
    
    public def serialize(ser:Serializer) {
        if (!isGlobal)
            globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(id);
    }
    
    public def find(id:Id) {
        try {
            glock.lock();
            return fmasters.getOrElse(id, null);
        }finally {
            glock.unlock();
        }
    }
    
    public def addChild(child:Id) {
        try {
            latch.lock();
            if (children == null) {
                children = new HashSet[Id]();
            }
            children.add(child);
        } finally {
            latch.unlock();
        }
    }
    
    private def globalInit() {
        latch.lock();
        if (!isGlobal) {
            if (verbose>=1) debug(">>>> doing globalInit for id="+id);
            
            
            isGlobal = true;
            if (verbose>=1) debug("<<<< globalInit returning fs="+this);
        }
        latch.unlock();
    }
    
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
        // wait for the latch release
        if (verbose>=2) debug("calling latch.await for id="+id);
        latch.await(); // wait for the termination (latch may already be released)
        if (verbose>=2) debug("returned from latch.await for id="+id);
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> PessimisticMaster.notifyPlaceDeath called");
        if (Place(backupPlaceId.get()).isDead()) {
            // TODO: create a new backup
            // backupPlace = NEW_BACKUP_PLACE
        }
        if (verbose>=1) debug("<<<< PessimisticMaster.notifyPlaceDeath returning");
    }
}

final class PessimisticRootFinishBackup implements x10.io.Unserializable {
    //a global class-level lock
    private static glock = new Lock();

    //the set of all masters
    private static fbackups = new HashMap[FinishResilient.Id, PessimisticRootFinishBackup]();
    
    //master place is previous place by default
    private static val masterPlaceId = new AtomicInteger(((here.id -1 + Place.numPlaces())%Place.numPlaces()) as Int);
    
    //instance level lock
    private val ilock = new Lock();

    //finish id (this is a root finish)
    private val id:FinishResilient.Id;
    
    //resilient finish counter set
    private transient var numActive:Long;
    private transient var live:HashMap[FinishResilient.Task,Int] = null; // lazily allocated
    private transient var transit:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
    private transient var _liveAdopted:HashMap[FinishResilient.Task,Int] = null; // lazily allocated 
    private transient var _transitAdopted:HashMap[FinishResilient.Edge,Int] = null; // lazily allocated
    
    //the nested finishes within this finish scope
    private var children:GrowableRail[FinishResilient.Id] = null; // lazily allocated
    
    //the potential adopter of this finish (the parent finish)
    private val parentId:FinishResilient.Id;

    private var isAdopted:Boolean = false;
    
    private def this(id:FinishResilient.Id, parentId:FinishResilient.Id) {
        this.id = id;
        this.numActive = 1;
        this.parentId = parentId;
    }
    
    public static def make(id:FinishResilient.Id, parentId:FinishResilient.Id) {
        return new PessimisticRootFinishBackup(id, parentId);
    }
    
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
}
