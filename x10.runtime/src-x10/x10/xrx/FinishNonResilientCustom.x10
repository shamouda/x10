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
import x10.util.Pair;
import x10.xrx.txstore.TxConfig;

class FinishNonResilientCustom extends FinishState implements CustomSerialization {
    protected transient var me:FinishState; // local finish object
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
        NonResilientCustomMasterState.addRoot(id, me as NonResilientCustomMasterState);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        if (id.home as Long == here.id)
            me = NonResilientCustomMasterState.getRoot(id);
        else
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
        private static val remotes = new HashMap[Id, NonResilientCustomRemoteState]() ; //a cache for remote finish objects

        var exceptions:GrowableRail[CheckedThrowable];
        val lock = new Lock();
        var count:Int = 0n;
        var remoteActivities:HashMap[Long,Int]; // key is place id, value is count for that place.
        val local = new AtomicInteger(0n); // local count
        
        private var txFlag:Boolean = false;
        private var txReadOnlyFlag:Boolean = true;
        
        public def toString() {
            return "Remote(id="+id+", localCount="+local.get()+", count="+count+")";
        }
        
        public def this (val id:Id) {
            this.id = id;
        }
        
        def setTxFlags(isTx:Boolean, isTxRO:Boolean) {
        	lock.lock();
            txFlag = txFlag | isTx;
            txReadOnlyFlag = txReadOnlyFlag & isTxRO;
            lock.unlock();
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
        
        def ensureRemoteActivities() {
            if (remoteActivities == null) {
                remoteActivities = new HashMap[Long,Int]();
            }
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Remote dump:\n");
            s.add("           here:" + here.id); s.add('\n');
            s.add("             id:" + id); s.add('\n');            
            s.add("     localCount:"); s.add(local); s.add('\n');
            if (remoteActivities != null) {
                s.add("     remote acts:"); s.add('\n');
            	for (e in remoteActivities.entries()) {
            	    s.add("         place "); s.add(e.getKey()); s.add(":"); s.add(e.getValue());s.add('\n');	
            	}
            }
            debug(s.toString());
        }
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:NonResilientCustomRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
                    remoteState = new NonResilientCustomRemoteState(id);
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
            if (dstPlace.id == here.id) {
                count++;
                lock.unlock();
                return;
            }
            ensureRemoteActivities();
            val old = remoteActivities.getOrElse(dstPlace.id, 0n);
            remoteActivities.put(dstPlace.id, old+1n);
            lock.unlock();
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { //done
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").spawnRemoteActivity(srcId=" + srcId +",dstId="+dstId+",kind="+kind+") called");
            notifySubActivitySpawn(dstPlace, kind);
            val preSendAction = ()=>{ };
            val fs = Runtime.activity().finishState(); //the outer finish
            x10.xrx.Runtime.x10rtSendAsync(dstPlace.id, body, fs, prof, preSendAction);
            if (verbose>=1) debug("<<<< Remote(id="+id+").spawnRemoteActivity(srcId=" + srcId +",dstId="+dstId+",kind="+kind+") returning");
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
        
        def setTxFlagsUnsafe(isTx:Boolean, isTxRO:Boolean) {
            txFlag = txFlag | isTx;
            txReadOnlyFlag = txReadOnlyFlag & isTxRO;
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, t:CheckedThrowable, isTx:Boolean, readOnly:Boolean):void {
            val id = this.id; //don't copy this
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+readOnly+") called ");
            lock.lock();
            if (t != null) {
                if (null == exceptions) exceptions = new GrowableRail[CheckedThrowable]();
                exceptions.add(t);
            }
            if (isTx)
                setTxFlagsUnsafe(isTx, readOnly);
            count--;
            if (local.decrementAndGet() > 0) {
                lock.unlock();
                return;
            }
            val cTx = txFlag;
            val cTxReadyOnly = txReadOnlyFlag;
            
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
                    	val fin = NonResilientCustomMasterState.getRoot(id);
                    	fin.notify(serializedTable, excs, cTx, cTxReadyOnly);
                    }
                } else {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_2") async {
                    	val fin = NonResilientCustomMasterState.getRoot(id);
                    	fin.notify(serializedTable, cTx, cTxReadyOnly);
                    }
                }
            } else {
                val message = new Pair[Long, Int](here.id, count);
                count = 0n;
                lock.unlock();
                if (null != excs) {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_3") async {
                    	val fin = NonResilientCustomMasterState.getRoot(id);
                        fin.notify(message, excs, cTx, cTxReadyOnly);
                    }
                } else {
                    at(Place(home)) @Immediate("nonResCustom_notifyActivityTermination_4") async {
                    	val fin = NonResilientCustomMasterState.getRoot(id);
                        fin.notify(message, cTx, cTxReadyOnly);
                    }
                }
            }
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+readOnly+") returning");
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
    public static final class NonResilientCustomMasterState extends FinishState implements x10.io.Unserializable, Releasable {
    	private static val rootLock = new Lock();
        private static val roots = new HashMap[Id, NonResilientCustomMasterState]() ; //a cache for remote finish objects
        public static def addRoot(id:Id, obj:NonResilientCustomMasterState) {
        	rootLock.lock();
        	roots.put(id, obj);
        	rootLock.unlock();
        }
        public static def getRoot(id:Id) {
        	rootLock.lock();
        	val obj = roots.getOrElse(id, null);
        	rootLock.unlock();
        	return obj;
        }
        
        public static def removeRoot(id:Id) {
        	rootLock.lock();
        	roots.remove(id);
        	rootLock.unlock();
        }
        
        val id:Id;
        val parentId:Id; 
        val parent:FinishState;
        
        val latch:SimpleLatch;
        var count:Int = 1n; // locally created activities
        var exceptions:GrowableRail[CheckedThrowable]; // captured remote exceptions.  lazily initialized
        // remotely spawned activities (created RemoteFinishes). lazily initialized
        var remoteActivities:HashMap[Long,Int]; // key is place id, value is count for that place.

        def ensureRemoteActivities():void {
            if (remoteActivities == null) {
                remoteActivities = new HashMap[Long,Int]();
            }
        }
        
        public def registerFinishTx(old:Tx, rootTx:Boolean):void {
            latch.lock();
            this.isRootTx = rootTx;
            if (rootTx)
                this.tx = old;
            else
                this.tx = Tx.clone(old);
            this.tx.initializeCustom(id);
            latch.unlock();
        }
    
        def this(id:Id, parent:FinishState) {
            this.latch = new SimpleLatch();
            this.id = id;
            this.parent = parent;
            if (parent instanceof FinishNonResilientCustom) {
                parentId = (parent as FinishNonResilientCustom).id;
            }
            else {
                parentId = UNASSIGNED;
            }
        }
        
        public def toString() {
            return "Root(id="+id+", parentId="+parentId+", count="+count+")";
        }
        
        public def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Root dump:\n");
            s.add("             id:" + id); s.add('\n');
            s.add("     localCount:"); s.add(count); s.add('\n');
            s.add("       parentId: " + parentId); s.add('\n');
            if (remoteActivities != null) {
                s.add("    remote acts:"); s.add('\n');
            	for (e in remoteActivities.entries()) {
            	    s.add("       place "); s.add(e.getKey()); s.add(":"); s.add(e.getValue());s.add('\n');	
            	}
            }
            debug(s.toString());
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
            if (verbose>=1) debug(">>>> Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            latch.lock();
            if (here.id == dstPlace.id) {
                count++;
                if (verbose>=4) dump();
                if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") local activity added, count=" + count);
                latch.unlock();
                return;
            }
            ensureRemoteActivities();
            remoteActivities.put(dstPlace.id, remoteActivities.getOrElse(dstPlace.id, 0n)+1n);
            if (verbose>=4) dump();
            if (verbose>=1) debug("<<<< Root(id="+id+").notifySubActivitySpawn(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
            latch.unlock();
        }
        
        def spawnRemoteActivity(dstPlace:Place, body:()=>void, prof:x10.xrx.Runtime.Profile):void { //done
            val kind = ASYNC;
            val srcId = here.id as Int;
            val dstId = dstPlace.id as Int;
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Root(id="+id+").spawnRemoteActivity(srcId=" + srcId +",dstId="+dstId+",kind="+kind+") called");
            notifySubActivitySpawn(dstPlace, kind);
            val preSendAction = ()=>{ };
            val fs = Runtime.activity().finishState(); //the outer finish
            x10.xrx.Runtime.x10rtSendAsync(dstPlace.id, body, fs, prof, preSendAction);
            if (verbose>=1) debug("<<<< Root(id="+id+").spawnRemoteActivity(srcId=" + srcId +",dstId="+dstId+",kind="+kind+") returning");
        }
        
        def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
            return true;
        }
        
        def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
            return true;
        }
        
        public def notifyRemoteContinuationCreated():void {
            latch.lock();
            ensureRemoteActivities();
            latch.unlock();
        }

        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void { 
            notifyActivityCreationFailed(srcPlace, t, ASYNC);
        }
        
        def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable, kind:Int):void { 
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") called");
            pushException(t);
            notifyActivityTermination(srcPlace);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        	notifyActivityTermination(srcPlace);
        }
        
        protected def process(t:CheckedThrowable):void {
            if (null == exceptions) exceptions = new GrowableRail[CheckedThrowable]();
            exceptions.add(t);
        }
        protected def process(excs:Rail[CheckedThrowable]):void {
            for (e in excs) {
                process(e);
            }
        }
        public def pushException(t:CheckedThrowable):void {
        	if (verbose>=1) debug(">>>> Root(id="+id+").pushException(t="+t.getMessage()+") called");
            latch.lock();
            process(t);
            latch.unlock();
            if (verbose>=1) debug("<<<< Root(id="+id+").pushException(t="+t.getMessage()+") returning");
        }
        
        def notifyActivityTermination(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, ASYNC, false, false, null);
        }
        def notifyShiftedActivityCompletion(srcPlace:Place):void {
            notifyActivityTermination(srcPlace, AT, false, false, null);
        }
        
        def notifyTxActivityTermination(srcPlace:Place, readOnly:Boolean, t:CheckedThrowable) {
            notifyActivityTermination(srcPlace, ASYNC, true, readOnly, t);
        }
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, isTx:Boolean, readOnly:Boolean, t:CheckedThrowable):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Root(id="+id+").notifyActivityTermination(srcId="+srcId + " dstId="+dstId+",kind="+kind
                    +",isTx="+isTx+",isTxRO="+readOnly+") called");
            latch.lock();
            if (t != null)
                process(t);
            
            if (tx != null && isTx) {
            	tx.addMember(here.id as Int, readOnly, 20n);
            }
            
            if (--count != 0n) {
                if (verbose>=4) dump();
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, count="+count);
                latch.unlock();
                return;
            }
            
            if (remoteActivities != null && remoteActivities.size() != 0) {
                for (entry in remoteActivities.entries()) {
                    if (entry.getValue() != 0n) {
                        if (verbose>=4) dump();
                        if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, pending remote activities, count="+count);
                        latch.unlock();
                        return;
                    }
                }
            }
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning, call tryRelease()");
            latch.unlock();
            tryRelease();
        }
        
        def waitForFinish():void {
            if (verbose>=1) debug(">>>> Root(id="+id+").waitForFinish called");
            // remove our own activity from count
            if (Runtime.activity().tx)
                notifyTxActivityTermination(here, Runtime.activity().txReadOnly, null);
            else
                notifyActivityTermination(here);
            
            if ((!Runtime.STRICT_FINISH) && (Runtime.STATIC_THREADS || remoteActivities == null)) {
                Runtime.worker().join(latch);
            }
            if (verbose>=2) debug("calling latch.await for id="+id);
            latch.await(); // sit here, waiting for all child activities to complete
            if (verbose>=2) debug("returned from latch.await for id="+id);
            //local finish -> for cases activities other than the finish main activity have created Tx work 
            if (tx != null && !tx.isEmpty() && !isRootTx) {
            	val subMembers = tx.getMembers();
            	val ro = tx.isReadOnly();
                //notify parent of transaction status
                if (parent instanceof FinishNonResilientCustom) {
                	val parentId = this.parentId;
                    val frParent = parent as FinishNonResilientCustom;
                    if (frParent.me instanceof NonResilientCustomMasterState) {
                    	(frParent.me as NonResilientCustomMasterState).tx.addSubMembers(subMembers, ro, 6666n);
                    } else if (frParent.me instanceof NonResilientCustomRemoteState && parentId.home as Long == here.id) {
                        NonResilientCustomMasterState.getRoot(parentId).tx.addSubMembers(subMembers, ro, 7777n);
                    } else if (frParent.me instanceof NonResilientCustomRemoteState) {
                        at(Place(parentId.home as Long)) @Immediate("nonResCustom_mergeSubMembers") async {
                        	NonResilientCustomMasterState.getRoot(parentId).tx.addSubMembers(subMembers, ro, 8888n);
                        }
                    }
                }
            }
            
            gc();
            
            // throw exceptions here if any were collected via the execution of child activities
            val t = MultipleExceptions.make(exceptions);
            if (null != t) throw t;
            
            // if no exceptions, simply return
        }
        
        
        def gc() {
        	val id = this.id;
            // if there were remote activities spawned, clean up the RemoteFinish objects which tracked them
            if (remoteActivities != null && remoteActivities.size() != 0) {
                remoteActivities.remove(here.id);
                if (tx == null || tx.isEmpty()) {
                    for (placeId in remoteActivities.keySet()) {
                        at(Place(placeId)) @Immediate("nonResCustom_remoteFinishCleanup1") async {
                        	NonResilientCustomRemoteState.deleteObject(id);
                        }
                    }
                } else {
                    for (placeId in remoteActivities.keySet()) {
                        if (tx.contains(placeId as Int))
                            continue;
                        at(Place(placeId)) @Immediate("nonResCustom_remoteFinishCleanup2") async {
                        	NonResilientCustomRemoteState.deleteObject(id);
                        }
                    }
                }
            }
        }

        protected def process(remoteMap:HashMap[Long, Int], isTx:Boolean, txRO:Boolean):void {
            if (verbose>=1) debug(">>>> Root(id="+id+").process(map) called");
            ensureRemoteActivities();
            var src:Int = -1n;
            // add the remote set of records to the local set
            for (remoteEntry in remoteMap.entries()) {
                remoteActivities.put(remoteEntry.getKey(), remoteActivities.getOrElse(remoteEntry.getKey(), 0n)+remoteEntry.getValue());
                if (remoteEntry.getValue() < 0) {
                    src = remoteEntry.getKey() as Int;
                }
            }
            if (tx != null && isTx) {
            	tx.addMember(src, txRO, 21n);
            }
        
            // add anything in the remote set which ran here to my local count, and remove from the remote set
            count += remoteActivities.getOrElse(here.id, 0n);
            remoteActivities.remove(here.id);
            
            // check if anything is pending locally
            if (count != 0n) {
                if (verbose>=4) dump();
                if (verbose>=1) debug("<<<< Root(id="+id+").process(map) returning");
                return;
            }
            
            // check to see if anything is still pending remotely
            for (entry in remoteActivities.entries()) {
                if (entry.getValue() != 0n) {
                    if (verbose>=4) dump();
                    if (verbose>=1) debug("<<<< Root(id="+id+").process(map) returning");
                    return;
                }
            }
            if (verbose>=4) dump();
            if (verbose>=1) debug("<<<< Root(id="+id+").process(map) returning, call tryRelease");
            // nothing is pending.  Release the latch
            tryRelease();
        }

        def notify(remoteMapBytes:Rail[Byte]):void {
            notify(remoteMapBytes, false, false);
        }
        
        def notify(remoteMapBytes:Rail[Byte], isTx:Boolean, txRO:Boolean):void {
            remoteMap:HashMap[Long, Int] = new x10.io.Deserializer(remoteMapBytes).readAny() as HashMap[Long, Int]; 
            latch.lock();
            process(remoteMap, isTx, txRO);
            latch.unlock();
        }

        def notify(remoteMapBytes:Rail[Byte], excs:Rail[CheckedThrowable]):void {
            notify(remoteMapBytes, excs, false, false);
        }
        
        def notify(remoteMapBytes:Rail[Byte], excs:Rail[CheckedThrowable], isTx:Boolean, txRO:Boolean):void {
            remoteMap:HashMap[Long, Int] = new x10.io.Deserializer(remoteMapBytes).readAny() as HashMap[Long, Int];
            latch.lock();
            process(excs);
            process(remoteMap, isTx, txRO);
            latch.unlock();
        }
        
        protected def process(remoteEntry:Pair[Long, Int], isTx:Boolean, txRO:Boolean):void {
            if (verbose>=1) debug(">>>> Root(id="+id+").process(pair) called");
            ensureRemoteActivities();
            // add the remote record to the local set
            remoteActivities.put(remoteEntry.first, remoteActivities.getOrElse(remoteEntry.first, 0n)+remoteEntry.second);
        
            if (tx != null && isTx) {
            	tx.addMember(remoteEntry.first as Int, txRO, 22n);
            }
            
            // check if anything is pending locally
            if (count != 0n) {
                if (verbose>=4) dump();
                if (verbose>=1) debug("<<<< Root(id="+id+").process(pair) returning");
                return;
            }
        
            // check to see if anything is still pending remotely
            for (entry in remoteActivities.entries()) {
                if (entry.getValue() != 0n) {
                    if (verbose>=4) dump();
                    if (verbose>=1) debug("<<<< Root(id="+id+").process(pair) returning");
                    return;
                }
            }

            if (verbose>=4) dump();
            if (verbose>=1) debug("<<<< Root(id="+id+").process(pair) returning, call tryRelease");
            // nothing is pending.  Release the latch
            tryRelease();
        }

        def notify(remoteEntry:Pair[Long, Int]) {
            notify(remoteEntry, false, false);
        }
        
        def notify(remoteEntry:Pair[Long, Int], isTx:Boolean, txRO:Boolean):void {
            latch.lock();
            process(remoteEntry, isTx, txRO);
            latch.unlock();
        }
        def notify(remoteEntry:Pair[Long, Int], excs:Rail[CheckedThrowable]) {
            notify(remoteEntry, excs, false, false);
        }
        
        def notify(remoteEntry:Pair[Long, Int], excs:Rail[CheckedThrowable], isTx:Boolean, txRO:Boolean):void {
            latch.lock();
            process(excs);
            process(remoteEntry, isTx, txRO);
            latch.unlock();
        }
        
        public def releaseFinish(excs:GrowableRail[CheckedThrowable]) {
            if (excs != null) {
                for (t in excs.toRail())
                    process(t);
            }
            latch.release();
        }
        
        def tryRelease() {
            if (tx == null || tx.isEmpty() || !isRootTx) {
                releaseFinish(null);
            } else {
                var abort:Boolean = false;
                if (exceptions != null && exceptions.size() > 0) {
                    abort = true;
                    if (TxConfig.TM_DEBUG) {
                        var str:String = "";
                        for (var m:Long = 0; m < exceptions.size(); m++)
                            str += exceptions(m).getMessage() + " , ";
                        Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"] finalize with abort because ["+str+"] ");
                    }
                }
                tx.finalize(this, abort);
            }
        }
    }
}