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

/**
 * Distributed Resilient Finish (records transit tasks only)
 * Implementation notes: remote objects are shared and are persisted in a static hashmap (special GC needed)
 */
class FinishResilientOptimistic extends FinishResilient implements CustomSerialization {
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
        if (me instanceof OptimisticRemoteState) {
            if (t == null) me.notifyActivityTermination(srcPlace);
            else me.notifyActivityTermination(srcPlace, t);
        }
        else
            super.notifyActivityTermination(srcPlace, t);
    }
    
    def notifyActivityCreatedAndTerminated(srcPlace:Place, t:CheckedThrowable):void { 
        if (me instanceof OptimisticRemoteState) {
            if (t == null) me.notifyActivityCreatedAndTerminated(srcPlace);
            else me.notifyActivityCreatedAndTerminated(srcPlace, t);
        }
        else
            super.notifyActivityCreatedAndTerminated(srcPlace, t);
    }
    
    def notifyShiftedActivityCompletion(srcPlace:Place, t:CheckedThrowable):void {
        if (me instanceof OptimisticRemoteState) {
            if (t == null) me.notifyShiftedActivityCompletion(srcPlace);
            else me.notifyShiftedActivityCompletion(srcPlace, t);
        }
        else
            super.notifyShiftedActivityCompletion(srcPlace, t);
    }
    
    //create root finish
    public def this (parent:FinishState) {
        id = Id(here.id as Int, nextId.getAndIncrement());
        me = new OptimisticMasterState(id, parent);
        FinishReplicator.addMaster(id, me as FinishMasterState);
        if (verbose>=1) debug("<<<< RootFinish(id="+id+") created");
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = OptimisticRemoteState.getOrCreateRemote(id);
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        return new FinishResilientOptimistic(parent);
    }
    
    def getSource():Place {
        if (me instanceof OptimisticMasterState)
            return here;
        else
            return Runtime.activity().srcPlace;
    }
    
    //serialize a root finish
    public def serialize(ser:Serializer) {
        if (verbose>=1) debug(">>>> serialize(id="+id+") called ");
        if (me instanceof OptimisticMasterState) {
            val me2 = (me as OptimisticMasterState); 
            if (!me2.isGlobal)
                me2.globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        }
        ser.writeAny(id);
        if (verbose>=1) debug("<<<< serialize(id="+id+") returning ");
    }
    
    def registerFinishTx(tx:Tx, rootTx:Boolean):void {
        me.registerFinishTx(tx, rootTx);
    }
    
    /* Because we record only the transit tasks with src-dst, we must record the
     * number of received tasks from each place, to be able to identify which transit tasks terminated.
     * 
     * For adoption, we rely on getting information about remote tasks from remote finish objects.
     * Therefore, we must store the remote objects until root finish is released.
     * 
     * To save memory, we don't create a remote object for each src place for each finish,
     * rather we create one remote object per finish with hash maps recording received and reported tasks per place.
     */
    //REMOTE
    public static final class OptimisticRemoteState extends FinishResilient implements x10.io.Unserializable {
        val id:Id; //parent root finish
        val ilock = new Lock(); //instance level lock
        val received = new HashMap[Task,Int](); //increasing counts
        val reported = new HashMap[Task,Int](); //increasing counts
        var taskDeny:HashSet[Int] = null; // lazily allocated
        val lc = new AtomicInteger(0n);//each time lc reaches zero, we report reported-received to root finish and set reported=received


        
        private static val remoteLock = new Lock();
        private static val remotes = new HashMap[Id, OptimisticRemoteState]() ; //a cache for remote finish objects
        
        private var txFlag:Boolean = false;
        private var txReadOnlyFlag:Boolean = true;
        private var ex:CheckedThrowable = null; //fixme: make it a list of exceptions        
        
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
                    if (verbose>=1) debug(">>>> deleting object(id="+idx+")");
                    remotes.delete(idx);
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
        
        public static def getOrCreateRemote(id:Id) {
            try {
                remoteLock.lock();
                var remoteState:OptimisticRemoteState = remotes.getOrElse(id, null);
                if (remoteState == null) {
                    remoteState = new OptimisticRemoteState(id);
                    remotes.put(id, remoteState);
                }
                return remoteState;
            } finally {
                remoteLock.unlock();
            }
        }

        public static def countDropped(id:Id, src:Int, kind:Int, sent:Int) {
            if (verbose>=1) debug(">>>> countDropped(id="+id+", src="+src+", kind="+kind+", sent="+sent+") called");
            var dropped:Int = 0n;
            var createdNow:Boolean = false;
            var allRemotes:String = "";
            try {
                remoteLock.lock();
                for (r in remotes.entries()) {
                    allRemotes += r.getKey() + " , ";
                }
                val remote = remotes.getOrElse(id, null);
                if (remote != null) {
                    val received = remote.receivedFrom(src, kind);
                    dropped = sent - received;
                } else {
                    createdNow = true;
                    //prepare remote to not accept future tasks from the src
                    val newRemote = new OptimisticRemoteState(id);
                    remotes.put(id, newRemote);
                    newRemote.taskDeny = new HashSet[Int]();
                    newRemote.taskDeny.add(src);
                    dropped = sent;
                }
            } finally {
                remoteLock.unlock();
            }
            if (verbose>=1) debug("<<<< countDropped(id="+id+", src="+src+", kind="+kind+", sent="+sent+") returning, createdNow="+createdNow+" allRemotes={"+allRemotes+"} dropped="+dropped);
            return dropped;
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
        
        private def getReportMapUnsafe() {
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
            
        }
        //Calculates the delta between received and reported
        private def getReportMap(exp:CheckedThrowable) {
            try {
                ilock.lock();
                if (exp != null)
                    ex = exp;
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
        
        def recordExp(exp:CheckedThrowable) {
            ilock.lock();
            if (exp != null)
                ex = exp;
            ilock.unlock();
        }
        
        public def notifyTerminationAndGetMap(t:Task, exp:CheckedThrowable) {
            var map:HashMap[Task,Int] = null;
            val count = lc.decrementAndGet();
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyTerminationAndGetMap called, taskFrom["+t.place+"] decremented localCount to "+count);
            if (count == 0n) {
                map = getReportMap(exp);
            }
            else {
                recordExp(exp);
            }
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
                val parentId = UNASSIGNED;
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") called");
                val req = FinishRequest.makeOptTransitRequest(id, parentId, srcId, dstId, kind, null, false);
                FinishReplicator.exec(req);
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifySubActivitySpawn(srcId="+srcId+",dstId="+dstId+",kind="+kind+") returning");
            }
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
            
            if (verbose>=1) debug(">>>> Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+",kind="+kind+",bytes="+bytes.size+") called");
            val req = FinishRequest.makeOptTransitRequest(id, parentId, srcId, dstId, kind, null, false);
            val num = req.num;
            
            val fs = Runtime.activity().finishState(); //the outer finish
            val preSendAction = ()=>{FinishReplicator.addPendingAct(id, num, dstId, fs, bytes, prof);};
            val postSendAction = (submit:Boolean, adopterId:Id)=>{
                if (submit) {
                    FinishReplicator.sendPendingAct(id, num);
                }
                fs.notifyActivityTermination(Place(srcId)); // terminate synthetic activity
            };
            val count = notifyReceived(Task(srcId, ASYNC)); // synthetic activity to keep finish locally live during async replication
            if (verbose>=1) debug("<<<< Remote(id="+id+").spawnRemoteActivity(srcId="+srcId+",dstId="+dstId+",kind="+kind+") incremented localCount to " + count);
            FinishReplicator.asyncExec(req, null, preSendAction, postSendAction);
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
            val parentId = UNASSIGNED;
            val req = FinishRequest.makeOptTermRequest(id, parentId, srcId, dstId, kind, t, null, false, false, false);
            FinishReplicator.asyncExec(req, null);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreationFailed(srcId=" + srcId + ",dstId="+dstId+",kind="+kind+",t="+t.getMessage()+") returning");
        }

        def notifyActivityCreatedAndTerminated(srcPlace:Place) {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, null);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, kind:Int, t:CheckedThrowable) {
            val srcId = srcPlace.id as Int;
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") called");
            val count = notifyReceived(Task(srcId, ASYNC));
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId +",dstId="+dstId+",kind="+ASYNC+") returning, localCount = " + count);
            
            val map = notifyTerminationAndGetMap(Task(srcId, kind), t);
            if (map == null) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning, map is null");
                return;
            }

            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId="+srcId+",dstId="+dstId+",kind="+kind+") reporting to root");
            val req = FinishRequest.makeOptTermMulRequest(id, dstId, map, ex, false, false);
            FinishReplicator.asyncExec(req, null);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityCreatedAndTerminated(srcId=" + srcId + ",dstId="+dstId+",kind="+ASYNC+") returning");
        }
        
        def pushException(t:CheckedThrowable):void {
            assert (Runtime.activity() != null) : here + " >>>> Remote(id="+id+").pushException(t="+t.getMessage()+") blocking method called within an immediate thread";
            val parentId = UNASSIGNED;
            if (verbose>=1) debug(">>>> Remote(id="+id+").pushException(t="+t.getMessage()+") called");
            val req = FinishRequest.makeOptExcpRequest(id, parentId, t);
            FinishReplicator.exec(req, null);
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
        
        def notifyActivityTermination(srcPlace:Place, kind:Int, t:CheckedThrowable, isTx:Boolean, isTxRO:Boolean):void {
            val srcId = srcPlace.id as Int; 
            val dstId = here.id as Int;
            if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") called ");
            var map:HashMap[Task,Int] = null;
            var tmpTx:Boolean = false;
            var tmpTxRO:Boolean = false;
            try {
                ilock.lock();
                val count = lc.decrementAndGet();
                if (verbose>=1) debug(">>>> Remote(id="+id+").notifyActivityTermination called, taskFrom["+srcId+"] decremented localCount to "+count);
                if (count == 0n) {
                    map = getReportMapUnsafe();
                    if (isTx)
                        setTxFlagsUnsafe(isTx, isTxRO);
                    tmpTx = txFlag;
                    tmpTxRO = txReadOnlyFlag;
                    if (t != null)
                        ex = t;
                } else  {
                    if (isTx)
                        setTxFlagsUnsafe(isTx, isTxRO);
                    if (t != null)
                        ex = t;
                }
            } finally {
                ilock.unlock();
            }
            if (map == null) {
                if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") returning without communication - map is NULL");
                return;
            }
            if (verbose>=1) debug("==== Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",tmpTx="+tmpTx+",tmpTxRO="+tmpTxRO+") reporting to root");
            val req = FinishRequest.makeOptTermMulRequest(id, dstId, map, ex, tmpTx, tmpTxRO);
            FinishReplicator.asyncExec(req, null);
            if (verbose>=1) debug("<<<< Remote(id="+id+").notifyActivityTermination(srcId="+srcId+",dstId="+dstId+",kind="+kind+",isTx="+isTx+",isTxRO="+isTxRO+") returning");
        }

        def waitForFinish():void {
            assert false : "fatal, waitForFinish must not be called from a remote finish" ;
        }
        
        def notifyActivityTermination(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, ASYNC, t, false, false);
        }
        
        def notifyActivityCreatedAndTerminated(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityCreatedAndTerminated(srcPlace, ASYNC, t);
        }
        
        def notifyShiftedActivityCompletion(srcPlace:Place, t:CheckedThrowable):void {
            notifyActivityTermination(srcPlace, AT, t, false, false);
        }
    }
    
    //ROOT
    public static final class OptimisticMasterState extends FinishMasterState implements x10.io.Unserializable, Releasable {
    
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
            if (parent instanceof FinishResilientOptimistic) {
                parentId = (parent as FinishResilientOptimistic).id;
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
                
                if (parent instanceof FinishResilientOptimistic) {
                    val frParent = parent as FinishResilientOptimistic;
                    if (frParent.me instanceof OptimisticMasterState) {
                        val par = (frParent.me as OptimisticMasterState);
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
            rCond.run(closure);
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
            try {
                latch.lock();
                
                if (isTx && tx != null) {
                    tx.addMember(dstId as Int, isTxRO);
                }
                
                if (verbose>=1) debug(">>>> Master(id="+id+").transitToCompleted srcId=" + srcId + ", dstId=" + dstId + " called");
                val e = Edge(srcId, dstId, kind);
                decrement(transit, e);
                //don't decrement 'sent' 
                
                numActive--;
                assert numActive>=0 : here + " FATAL error, Master(id="+id+").numActive reached -ve value";
                if (t != null) addExceptionUnsafe(t);
                if (quiescent()) {
                    tryRelease();
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompleted returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId );
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def removeGhostChild(childId:Id, t:CheckedThrowable, subMembers:Set[Int], subReadOnly:Boolean, resp:MasterResponse) {
            try {
                latch.lock();
                if (tx != null && subMembers != null){
                    tx.addSubMembers(subMembers, subReadOnly);
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
                        tryRelease();
                    }
                }
                if (verbose>=1) debug("<<<< Master(id="+id+").removeGhostChild childId=" + childId + ", t=" + t + " returning");
                resp.backupPlaceId = backupPlaceId;
                resp.backupChanged = backupChanged;
            } finally {
                latch.unlock();
            }
        }
        
        def addSubTxMembers(subMembers:Set[Int], subReadOnly:Boolean, resp:MasterResponse) {
            try {
                latch.lock();
                if (tx != null) {
                    tx.addSubMembers(subMembers, subReadOnly);
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
            if (quiescent()) {
                tryRelease();
            }
            resp.backupPlaceId = backupPlaceId;
            resp.backupChanged = backupChanged;
            if (verbose>=1) debug("<<<< Master(id="+id+").transitToCompletedMul returning id="+id + ", srcId=" + srcId + ", dstId=" + dstId + " set backup="+resp.backupPlaceId+ " set backupChanged="+resp.backupChanged);
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
                tx.finalizeWithBackup(this, abort, backupPlaceId); //this also performs gc
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
                    try {
                        latch.lock();
                        
                        if (isTx && tx != null) {
                            tx.addMember(dstId as Int, isTxRO);
                        }
                        
                        resp.backupPlaceId = -1n;
                        for (e in map.entries()) {
                            val srcId = e.getKey().place;
                            val kind = e.getKey().kind;
                            val cnt = e.getValue();
                            transitToCompletedUnsafe(srcId, dstId, kind, cnt, ex, "notifyActivityTermination", resp);
                        }
                    } finally {
                        latch.unlock();
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
                if (parent instanceof FinishResilientOptimistic) {
                    val frParent = parent as FinishResilientOptimistic;
                    if (frParent.me instanceof OptimisticMasterState) {
                        val par = (frParent.me as OptimisticMasterState);
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
            if (parent instanceof FinishResilientOptimistic) {
                val frParent = parent as FinishResilientOptimistic;
                if (frParent.me instanceof OptimisticMasterState) {
                    val par = (frParent.me as OptimisticMasterState);
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
            if (parent instanceof FinishResilientOptimistic) {
                val frParent = parent as FinishResilientOptimistic;
                if (frParent.me instanceof OptimisticMasterState) {
                    val par = (frParent.me as OptimisticMasterState);
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
            if (parent instanceof FinishResilientOptimistic) {
                val frParent = parent as FinishResilientOptimistic;
                if (frParent.me instanceof OptimisticMasterState) {
                    val par = (frParent.me as OptimisticMasterState);
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
            if (verbose>=1) debug("==== Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") called");
            val req = FinishRequest.makeOptTermRequest(id, parentId, srcId, dstId, kind, null, tx, isRootTx, txFlag, txReadOnlyFlag);
            FinishReplicator.asyncExec(req, this);
            if (verbose>=1) debug("<<<< Root(id="+id+").notifyActivityTermination(parentId="+parentId+",srcId="+srcId + ",dstId="+dstId+",kind="+kind+") returning");
        }
        
        
        private def tryReleaseLocal() {
            if (verbose>=1) debug(">>>> Root(id="+id+").tryReleaseLocal called ");
            if (tx == null || tx.isEmpty() || !isRootTx) {
                latch.release();
            } else {
                tx.addMember(here.id as Int, txReadOnlyFlag);
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
            
            // no more messages will come back to this finish state 
            if (REMOTE_GC) gc();
            
            // get exceptions and throw wrapped in a ME if there are any
            if (excs != null) {
                if (verbose>=1) debug("<<<< Root(id="+id+").waitForFinish returning with exceptions, size=" + excs.size() );
                throw new MultipleExceptions(excs);
            }
            
            if (id == TOP_FINISH) {
                //blocks until replication work is done at all places
                FinishReplicator.finalizeReplication();
            }
        }
        
        def notifyRootTx() {
            if (tx != null && !tx.isEmpty() && !isRootTx) {
                if (verbose>=1) debug(">>>> Root(id="+id+").notifyRootTx called");
                val req = FinishRequest.makeOptMergeSubTxRequest(parentId, tx.getMembers(), tx.isReadOnly());
                val myId = id;
                val myBackupId = backupPlaceId;
                val preSendAction = ()=>{};
                val postSendAction = (submit:Boolean, adopterId:Id)=>{
                    at (Place(myBackupId)) @Immediate("optdist_release_backup2") async {
                        if (verbose>=1) debug("==== releaseFinish(id="+myId+") reached backup");
                        FinishReplicator.removeBackupOrMarkToDelete(myId);
                    }
                };
                FinishReplicator.asyncExec(req, this, preSendAction, postSendAction);
                if (verbose>=1) debug("<<<< Root(id="+id+").notifyRootTx returning");
            }
        }
        
        def gc() {
            val id = this.id;
            val set = new HashSet[Int]();
            for (e in sent.entries()) {
                val dst = e.getKey().dst;
                if (dst != here.id as Int && !set.contains(dst)) {
                    set.add(dst);
                    at(Place(dst)) @Immediate("optdist_remoteFinishCleanup") async {
                        FinishResilientOptimistic.OptimisticRemoteState.deleteObject(id);
                    }
                }
            }
        }
        
        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        public def isImpactedByDeadPlaces(newDead:HashSet[Int]) {
            if (tx != null && tx instanceof TxResilient && (tx as TxResilient).isImpactedByDeadPlaces(newDead)) {
                return true;
            }
            if (!isGlobal)
                return false;
            if (newDead.contains(backupPlaceId)) /*needs re-replication*/
                return true;
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.src) || newDead.contains(edge.dst))
                    return true;
            }
            return false;
        }
        
        public def convertToDead(newDead:HashSet[Int], countChildrenBackups:HashMap[FinishResilient.Id, HashSet[FinishResilient.Id]]) {
            if (verbose>=1) debug(">>>> Master(id="+id+").convertToDead called");
            val ghosts = countChildrenBackups.get(id);
            if (ghosts != null && ghosts.size() > 0) {
                if (ghostsNotifiedEarly != null) {
                    ghosts.removeAll(ghostsNotifiedEarly); // these are already notified
                }
                if (ghostChildren == null) {
                    ghostChildren = new HashSet[Id]();
                }
                ghostChildren.addAll(ghosts);
                if (verbose>=1) debug("==== Master(id="+id+").convertToDead adding ["+ghosts.size()+"] ghosts");
                numActive += ghosts.size();
            }
            ghostsNotifiedEarly = null;
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) ) {
                    val count = e.getValue();
                    toRemove.add(edge);
                    numActive -= count;
                    if (numActive < 0)
                        throw new Exception ( here + " Master(id="+id+").convertToDead FATAL error, numActive must not be negative");
                    
                    if (edge.kind == ASYNC) {
                        for (1..count) {
                            if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                            val dpe = new DeadPlaceException(Place(edge.dst));
                            dpe.fillInStackTrace();
                            addExceptionUnsafe(dpe);
                        }
                    }
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                tryRelease();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").convertToDead returning, numActive="+numActive);
        }

        public def convertFromDead(countDropped:HashMap[FinishResilientOptimistic.DroppedQueryId, Int]) {
            if (verbose>=1) debug(">>>> Master(id="+id+").convertFromDead called");
            for (e in countDropped.entries()) {
                val query = e.getKey();
                val dropped = e.getValue();
                if (dropped > 0 && query.id == id) {
                    val edge = Edge(query.src, query.dst, query.kind);
                    val oldTransit = transit.get(edge);
                    val oldActive = numActive;
                    
                    if (oldActive < dropped)
                        throw new Exception(here + " FATAL: dropped tasks counting error id = " + id);
                    
                    numActive -= dropped;
                    if (verbose>=1) debug(">>>> Master(id="+id+").convertFromDead removed "+dropped+" dropped message(s)");
                    if (oldTransit - dropped == 0n) {
                        transit.remove(edge);
                    }
                    else {
                        transit.put(edge, oldTransit - dropped);
                    }
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                }
            }
            
            if (quiescent()) {
                tryRelease();
            }
            if (verbose>=1) debug("<<<< Master(id="+id+").convertFromDead returning, numActive="+numActive);
        }
        
        public def notifyTxPlaceDeath() {
            (tx as TxResilient).notifyPlaceDeath();
        }
    }
    

    //BACKUP
    public static final class OptimisticBackupState extends FinishBackupState implements x10.io.Unserializable, Releasable {
        private static val verbose = FinishResilient.verbose;
        static def debug(msg:String) {
            val nsec = System.nanoTime();
            val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
            Console.OUT.println(output); Console.OUT.flush();
        }

        val id:Id; //finish id 
        val ilock = new Lock(); //instance lock
        val parentId:Id; //the root parent id (potential adopter)
        
        //resilient finish counter set
        var numActive:Long;
        val sent = new HashMap[FinishResilient.Edge,Int](); //always increasing   
        val transit = new HashMap[FinishResilient.Edge,Int]();
        var ghostChildren:HashSet[Id] = null;  //lazily allocated in addException
        var ghostsNotifiedEarly:HashSet[Id] = null;
        var excs:GrowableRail[CheckedThrowable]; 
        var isRecovered:Boolean = false;
        var migrating:Boolean = false;
        var placeOfMaster:Int = -1n; //will be updated during adoption
        var isReleased:Boolean = false;
        var canDelete:Boolean = false;
        val tx:Tx;
        val isRootTx:Boolean;
        var txStarted:Boolean = false;
        def this(id:FinishResilient.Id, parentId:FinishResilient.Id, tx:Tx, rootTx:Boolean) {
            this.id = id;
            this.numActive = 1;
            this.parentId = parentId;
            sent.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            transit.put(FinishResilient.Edge(id.home, id.home, FinishResilient.ASYNC), 1n);
            this.placeOfMaster = id.home;
            this.tx = tx;
            isRootTx = rootTx;
            if (tx != null) {
                tx.initialize(id, -1n);
            }
            if (verbose>=1) debug(">>>> Backup(id="+id+", parentId="+parentId+", tx="+tx+") created by simple constructor");
        }
        
        def this(id:FinishResilient.Id, _parentId:FinishResilient.Id, _numActive:Long, 
                _sent:HashMap[FinishResilient.Edge,Int],
                _transit:HashMap[FinishResilient.Edge,Int],
                _ghostChildren:HashSet[Id],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int,
                _tx:Tx, _rootTx:Boolean) {
            this.id = id;
            this.numActive = _numActive;
            this.parentId = _parentId;
            for (e in _sent.entries()) {
                this.sent.put(e.getKey(), e.getValue());
            }
            for (e in _transit.entries()) {
                this.transit.put(e.getKey(), e.getValue());
            }
            this.ghostChildren = _ghostChildren;
            this.placeOfMaster = _placeOfMaster;
            this.excs = _excs;
            this.tx = _tx;
            this.isRootTx = _rootTx;
            if (this.tx != null) {
                this.tx.initialize(id, -1n);
            }
            if (verbose>=1) debug(">>>> Backup(id="+id+", parentId="+parentId+", tx="+tx+") created by complex constructor");
        }
        
        def getTxStarted() {
            try {
                ilock.lock();
                return txStarted;
            } finally {
                ilock.unlock();
            }
        }

        def removeBackupOrMarkToDelete():void {
            ilock.lock();
            if (verbose>=1) debug(">>>> Backup(id="+id+").removeBackupOrMarkToDelete  isReleased["+isReleased+"] numActive["+numActive+"] ");
            if (isReleased) {
                if (verbose>=1) debug("<<<< Backup(id="+id+").removeBackupOrMarkToDelete  will delete");
                FinishReplicator.removeBackup(id);
            } else {
                if (verbose>=1) debug("<<<< Backup(id="+id+").removeBackupOrMarkToDelete  will mark only for future delete");
                canDelete = true;
            }
            ilock.unlock();
        }
        
        def notifyCommit() {
            if (tx != null) {
                (tx as TxResilient).markAsCommitting();
            }
        }
        
        def dump() {
            val s = new x10.util.StringBuilder();
            s.add("Backup dump:\n");
            s.add("             id:" + id); s.add('\n');
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
        
        def sync(_numActive:Long, _sent:HashMap[FinishResilient.Edge,Int],
        		_transit:HashMap[FinishResilient.Edge,Int],
        		_ghostChildren:HashSet[Id],
                _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int) {
            try {
                lock();
                this.numActive = _numActive;
                this.sent.clear();
                for (e in _sent.entries()) {
                    this.sent.put(e.getKey(), e.getValue());
                }
                this.transit.clear();
                for (e in _transit.entries()) {
                    this.transit.put(e.getKey(), e.getValue());
                }
                this.ghostChildren = _ghostChildren;
                this.placeOfMaster = _placeOfMaster;
                this.excs = _excs;
                if (verbose>=3) dump();
            } finally {
                unlock();
            }
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
            try {
                lock();
                return placeOfMaster;
            } finally {
                unlock();
            }
        }
        
        def addExceptionUnsafe(t:CheckedThrowable) {
            if (excs == null) excs = new GrowableRail[CheckedThrowable]();
            excs.add(t);
        }
        
        def addDeadPlaceException(placeId:Long) {
            try {
                ilock.lock();
                val dpe = new DeadPlaceException(Place(placeId));
                dpe.fillInStackTrace();
                addExceptionUnsafe(dpe);
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
        
        def inTransit(srcId:Long, dstId:Long, kind:Int, tag:String) {
            val lc:Long;
            try {
                ilock.lock();
                if (verbose>=1) debug(">>>> Backup(id="+id+").inTransit called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                FinishResilient.increment(sent, e);
                FinishResilient.increment(transit, e);
                numActive++;
                lc = numActive;
                if (verbose>=3) debug("==== Backup(id="+id+").inTransit (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                if (verbose>=3) dump();
                
                if (verbose>=1) debug("<<<< Backup(id="+id+").inTransit returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
            return lc;
        }
        
        def transitToCompleted(srcId:Long, dstId:Long, kind:Int, ex:CheckedThrowable, isTx:Boolean,
                isTxRO:Boolean, tag:String) {
            try {
                ilock.lock();
                
                if (isTx && tx != null) {
                    tx.addMember(dstId as Int, isTxRO);
                }
                
                if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompleted called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                decrement(transit, e);
                //don't decrement 'sent'
                numActive--;
                if (verbose>=3) debug("==== Backup(id="+id+").transitToCompleted (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                if (verbose>=3) dump();
                if (ex != null) addExceptionUnsafe(ex);
                if (quiescent()) {
                    isReleased = true;
                    if (tx == null || tx instanceof TxResilientNoSlaves || canDelete) 
                        FinishReplicator.removeBackup(id);
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompleted returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def removeGhostChild(childId:Id, ex:CheckedThrowable, subMembers:Set[Int], subReadOnly:Boolean) {
            try {
                ilock.lock();
                if (tx != null && subMembers != null) {
                    tx.addSubMembers(subMembers, subReadOnly);
                }
                if (verbose>=1) debug(">>>> Backup(id="+id+").removeGhostChild called (childId="+childId+", ex=" + ex + ") ");
                if (ghostChildren == null || !ghostChildren.contains(childId)) {
                    if (ghostsNotifiedEarly == null)
                        ghostsNotifiedEarly = new HashSet[Id]();
                    ghostsNotifiedEarly.add(childId);
                    if (verbose>=1) debug("<<<< Backup(id="+id+").removeGhostChild returning - child buffered (childId="+childId+", ex=" + ex + ") ");
                } else {
                    ghostChildren.remove(childId);
                    numActive--;
                    
                    if (verbose>=3) debug("==== Backup(id="+id+").removeGhostChild called (childId="+childId+", ex=" + ex + ") dumping after update");
                    if (verbose>=3) dump();
                    if (ex != null) addExceptionUnsafe(ex);
                    if (quiescent()) {
                        isReleased = true;
                        if (tx == null || tx instanceof TxResilientNoSlaves || canDelete) 
                            FinishReplicator.removeBackup(id);
                    }
                    if (verbose>=1) debug("<<<< Backup(id="+id+").removeGhostChild returning (childId="+childId+", ex=" + ex + ") numActive=" + numActive + " isReleased=" + isReleased);
                }
            } finally {
                ilock.unlock();
            }
        }
        
        def addSubTxMembers(subMembers:Set[Int], subReadOnly:Boolean) {
            if (tx == null)
                return;
            try {
                ilock.lock();
                tx.addSubMembers(subMembers, subReadOnly);
            } finally {
                ilock.unlock();
            }
        }
                
        def transitToCompletedUnsafe(srcId:Long, dstId:Long, kind:Int, cnt:Int, ex:CheckedThrowable, tag:String) {
            try {
                ilock.lock();
                
                if (ex != null) addExceptionUnsafe(ex);
                
                if (verbose>=1) debug(">>>> Backup(id="+id+").transitToCompletedMul called (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
                val e = FinishResilient.Edge(srcId, dstId, kind);
                FinishResilient.deduct(transit, e, cnt);
                //don't decrement 'sent'
                numActive-=cnt;
                if (verbose>=3) debug("==== Backup(id="+id+").transitToCompletedMul (srcId=" + srcId + ", dstId=" + dstId + ") dumping after update");
                if (verbose>=3) dump();
                if (quiescent()) {
                    isReleased = true;
                    if (tx == null || tx instanceof TxResilientNoSlaves || canDelete) 
                        FinishReplicator.removeBackup(id);
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").transitToCompletedMul returning (numActive="+numActive+", srcId=" + srcId + ", dstId=" + dstId + ", cnt="+cnt+") ");
            } finally {
                ilock.unlock();
            }
        }
        
        def quiescent():Boolean {
            val quiet = numActive == 0;
            if (verbose>=2 || (verbose>=1 && quiet)) debug("<<<< Backup(id="+id+").quiescent returning " + quiet + "  tx="+tx);
            return quiet;
        }
        def backupNotifyParent() {
            if (verbose>=1) debug(">>>> Backup(id="+id+").notifyParent(parentId="+parentId+") called");
            if (parentId != FinishResilient.UNASSIGNED) {
                var subMembers:Set[Int] = null;
                var subReadOnly:Boolean = true;
                if (tx != null && !isRootTx) {
                    subMembers = tx.getMembers();
                    subReadOnly = tx.isReadOnly();
                }
                val mem = subMembers;
                val ro = subReadOnly;
                if ( parentId.home == id.home) { 
                    val parBack = FinishReplicator.findBackup(parentId);
                    if (parBack == null) {
                        if (verbose>=1) debug("<<<< Backup(id="+id+").notifyParent(parentId="+parentId+") skip request because backup doesn't exist");
                        return;
                    } else if (this.migrating && (parBack as OptimisticBackupState).isMigrating()) { //child recovery is done before the parent recovery
                        val dpe = new DeadPlaceException(Place(id.home));
                        (parBack as OptimisticBackupState).removeGhostChild(id, dpe, mem, ro);
                        return;
                    }
                }
                Runtime.submitUncounted( ()=>{
                    if (verbose>=1) debug(">>>> Backup(id="+id+").notifyParent(parentId="+parentId+") called");
                    val req = FinishRequest.makeOptRemoveGhostChildRequest(parentId, id, mem, ro);
                    val resp = FinishReplicator.exec(req);
                    if (verbose>=1) debug("<<<< Backup(id="+id+").notifyParent(parentId="+parentId+") returning");
                });
            }
        }

        public def exec(xreq:FinishRequest) {
            val resp = new BackupResponse();
            val reqMaster = xreq.masterPlaceId;
            try {
                lock();
                if (migrating) {
                    resp.errMasterDied = true;
                    return resp;
                }
                if (reqMaster != placeOfMaster) {
                    resp.errMasterChanged = true;
                    resp.newMasterPlace = placeOfMaster;
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
                val transitSubmitDPE = req.transitSubmitDPE;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] called");
                if (req.transitSubmitDPE) {
                    addDeadPlaceException(dstId);
                    if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT-transitSubmitDPE, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] returning");
                } else {
                    val lc = inTransit(srcId, dstId, kind, "notifySubActivitySpawn");
                    if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TRANSIT, srcId=" + srcId + ", dstId="+ dstId + ",kind=" + kind + " ] returning, count="+lc);
                }
            } else if (xreq instanceof TermRequestOpt) {
                val req = xreq as TermRequestOpt;
                val srcId = req.srcId;
                val dstId = req.dstId;
                val kind = req.kind;
                val ex = req.ex;
                val isTx = req.isTx;
                val isTxRO = req.isTxRO;
                
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + ", ex=" + ex + " ] called");
                transitToCompleted(srcId, dstId, kind, ex, isTx, isTxRO, "notifyActivityTermination");
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM, srcId=" + srcId + ", dstId="+ dstId + ", kind=" + kind + ", ex=" + ex + " ] returning");
            } else if (xreq instanceof ExcpRequestOpt) {
                val req = xreq as ExcpRequestOpt;
                val ex = req.ex;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] called");
                addException(ex);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=EXCP, ex="+ex+" ] returning");
            } else if (xreq instanceof TermMulRequestOpt) {
                val req = xreq as TermMulRequestOpt;
                val map = req.map;
                val dstId = req.dstId;
                val ex = req.ex;
                val isTx = req.isTx;
                val isTxRO = req.isTxRO;
                
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+", isTx="+isTx+", isTxRO="+isTxRO+"] called");
                try {
                    ilock.lock();
                    if (isTx && tx != null) {
                        tx.addMember(dstId as Int, isTxRO);
                    }
                    for (e in map.entries()) {
                        val srcId = e.getKey().place;
                        val kind = e.getKey().kind;
                        val cnt = e.getValue();
                        transitToCompletedUnsafe(srcId, dstId, kind, cnt, ex, "notifyActivityTermination");
                    }
                }finally {
                    ilock.unlock();
                }
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=TERM_MUL, dstId=" + dstId +", mapSz="+map.size()+", isTx="+isTx+", isTxRO="+isTxRO+"] returning");
            } else if (xreq instanceof RemoveGhostChildRequestOpt) {
                val req = xreq as RemoveGhostChildRequestOpt;
                val childId = req.childId;
                val mem = req.subMembers;
                val ro = req.subReadOnly;
                val dpe = new DeadPlaceException(Place(childId.home));
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=REM_GHOST, child="+childId+", ex="+dpe+" ] called");
                removeGhostChild(childId, dpe, mem, ro);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=REM_GHOST, child="+childId+", ex="+dpe+" ] returning");
            } else if (xreq instanceof MergeSubTxRequestOpt) {
                val req = xreq as MergeSubTxRequestOpt;
                val mem = req.subMembers;
                val ro = req.subReadOnly;
                if (verbose>=1) debug(">>>> Backup(id="+id+").exec [req=MERGE_SUB_TX] called");
                addSubTxMembers(mem, ro);
                if (verbose>=1) debug("<<<< Backup(id="+id+").exec [req=MERGE_SUB_TX] returning");
            }
            return resp;
        }

        /*********************************************************************/
        /*******************   Failure Recovery Methods   ********************/
        /*********************************************************************/
        //waits until backup is adopted
        public def getNewMasterBlocking() {
            if (verbose>=1) debug(">>>> Backup(id="+id+").getNewMasterBlocking called");
            Runtime.increaseParallelism();
            ilock.lock();
            while (!isRecovered || migrating) {
                ilock.unlock();
                System.threadSleep(0); // release the CPU to more productive pursuits
                ilock.lock();
            }
            ilock.unlock();
            Runtime.decreaseParallelism(1n);
            if (verbose>=1) debug("<<<< Backup(id="+id+").getNewMasterBlocking returning, newMaster=" + id);
            return id;
        }
        
        
        public def isMigrating() {
            val result:Boolean;
            try {
                ilock.lock();
                result = migrating;
                return result;
            } finally {
                ilock.unlock();
            }
        }
        
        public def convertToDead(newDead:HashSet[Int], countChildrenBackups:HashMap[FinishResilient.Id, HashSet[FinishResilient.Id]]) {
            if (verbose>=1) debug(">>>> Backup(id="+id+").convertToDead called");
            val ghosts = countChildrenBackups.get(id);
            
            
            if (ghosts != null && ghosts.size() > 0) {
                if (ghostsNotifiedEarly != null) {
                    ghosts.removeAll(ghostsNotifiedEarly); // these are already notified
                }
                if (ghostChildren == null) {
                    ghostChildren = new HashSet[Id]();
                }
                ghostChildren.addAll(ghosts);
                if (verbose>=1) debug("==== Backup(id="+id+").convertToDead adding ["+ghosts.size()+"] ghosts");
                numActive += ghosts.size();
            }
            ghostsNotifiedEarly = null;
            val toRemove = new HashSet[Edge]();
            for (e in transit.entries()) {
                val edge = e.getKey();
                if (newDead.contains(edge.dst) ) {
                    val count = e.getValue();
                    toRemove.add(edge);
                    numActive -= count;
                    if (numActive < 0)
                        throw new Exception ( here + " Backup(id="+id+").convertToDead FATAL error, numActive must not be negative");
                    
                    if (edge.kind == ASYNC) {
                        for (1..count) {
                            if (verbose>=3) debug("adding DPE to "+id+" for transit task at "+edge.dst);
                            val dpe = new DeadPlaceException(Place(edge.dst));
                            dpe.fillInStackTrace();
                            addExceptionUnsafe(dpe);
                        }
                    }
                }
            }
            
            for (e in toRemove)
                transit.remove(e);
            
            if (quiescent()) {
                isReleased = true;
            }
            if (verbose>=1) debug("<<<< Backup(id="+id+").convertToDead returning, numActive="+numActive);
        }
        
        public def convertFromDead(countDropped:HashMap[FinishResilientOptimistic.DroppedQueryId, Int]) {
            if (verbose>=1) debug(">>>> Backup(id="+id+").convertFromDead called");
            for (e in countDropped.entries()) {
                val query = e.getKey();
                val dropped = e.getValue();
                if (dropped > 0 && query.id == id) {
                    //FATAL IF STATE IS NULL
                    val edge = Edge(query.src, query.dst, query.kind);
                    val oldTransit = transit.get(edge);
                    val oldActive = numActive;
                    
                    if (oldActive < dropped)
                        throw new Exception(here + " FATAL: dropped tasks counting error id = " + id);
                    
                    numActive -= dropped;
                    if (oldTransit - dropped == 0n) {
                        transit.remove(edge);
                    }
                    else {
                        transit.put(edge, oldTransit - dropped);
                    }
                    assert numActive >= 0 : here + " Master(id="+id+").convertFromDead FATAL error, numActive must not be negative";
                }
            }
            
            if (quiescent()) {
                isReleased = true;
            }
            if (verbose>=1) debug("<<<< Backup(id="+id+").convertFromDead returning, numActive="+numActive);
        }
        
        private def backupTryRelease() {
            if (tx != null && !tx.isEmpty() && isRootTx) {
                txStarted = true;
                (tx as TxResilient).backupFinalize(this);
            } else {
                releaseFinish(null);
            }
        }
        
        public def releaseFinish(excs:GrowableRail[CheckedThrowable]) {
            if (verbose>=1) debug(">>>> Backup(id="+id+").releaseFinish called");
            backupNotifyParent();
            FinishReplicator.removeBackup(id);
            if (verbose>=1) debug("<<<< Backup(id="+id+").releaseFinish returning");
        }
        
        public def notifyTxPlaceDeath() {
            (tx as TxResilient).notifyPlaceDeath();
        }
        
    }
    
    /*********************************************************************/
    /*******************   Failure Recovery Methods   ********************/
    /*********************************************************************/
    static def aggregateCountingRequests(newDead:HashSet[Int],
            masters:HashSet[FinishMasterState], backups:ArrayList[FinishBackupState]) {
        val countingReqs = new HashMap[Int,OptResolveRequest]();
        if (!masters.isEmpty()) {
            for (dead in newDead) {
                var backup:Int = FinishReplicator.getBackupPlace(dead);
                if (Place(backup).isDead())
                    backup = FinishReplicator.searchBackup(dead, backup);
                for (mx in masters) {
                    val m = mx as OptimisticMasterState; 
                    m.lock();
                    for (entry in m.transit.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countChildren .put(ChildrenQueryId(m.id /*parent id*/, dead, edge.src ), null);
                        } else if (dead == edge.src) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(edge.dst, rreq);
                            }
                            val sent = m.sent.get(edge);
                            rreq.countDropped.put(DroppedQueryId(m.id, dead, edge.dst, edge.kind, sent), -1n);
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
                    for (entry in b.transit.entries()) {
                        val edge = entry.getKey();
                        if (dead == edge.dst) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(backup, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(backup, rreq);
                            }
                            rreq.countChildren.put(ChildrenQueryId(b.id, dead, edge.src), null);
                        } else if (dead == edge.src) {
                            var rreq:OptResolveRequest = countingReqs.getOrElse(edge.dst, null);
                            if (rreq == null){
                                rreq = new OptResolveRequest();
                                countingReqs.put(edge.dst, rreq);
                            }
                            val sent = b.sent.get(edge);
                            rreq.countDropped.put(DroppedQueryId(b.id, dead, edge.dst, edge.kind, sent), -1n);
                        }
                    }
                    b.unlock();
                }
            }
        }
        if (verbose >=1) printResolveReqs(countingReqs); 
        return countingReqs;
    }
    
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
                        val countChildrenBackups = requests.countChildren ;
                        val countDropped = requests.countDropped;
                        if (countChildrenBackups.size() > 0) {
                            for (b in countChildrenBackups.entries()) {
                                val rec = b.getKey();
                                val set = FinishReplicator.countChildrenBackups(rec.parentId, rec.dead, rec.src);
                                if (verbose>=1) debug("==== processCountingRequests  children of " + rec.parentId + " is the set " + set);
                                countChildrenBackups.put(b.getKey(), set);   
                            }
                        }
                        
                        if (countDropped.size() > 0) {
                            for (r in countDropped.entries()) {
                                val key = r.getKey();
                                val dropped = OptimisticRemoteState.countDropped(key.id, key.src, key.kind, key.sent);
                                if (verbose>=1) debug("==== processCountingRequests  dropped of id " + key.id + " from src " + key.src + " is " + dropped);
                                countDropped.put(key, dropped);
                            }
                        }
                        
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== processCountingRequests  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("counting_response") async {
                            val output = (outputGr as GlobalRef[HashMap[Int,OptResolveRequest]]{self.home == here})().getOrThrow(me);
                            
                            for (ve in countChildrenBackups.entries()) {
                                output.countChildren.put(ve.getKey(), ve.getValue());
                            }
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
        
        if (fin.failed()) {
            var str:String = "";
            for (p in places)
                str += p + "," + Place(p as Long).isDead()+ " , ";
            throw new Exception(here + " FATAL ERROR: another place failed during recovery ["+str+"] ...");
        }
    }
    
    static def createMasters(backups:HashSet[FinishBackupState]) {
        if (verbose>=1) debug(">>>> createMasters(size="+backups.size()+") called");
        if (backups.size() == 0) {
            if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+" returning, zero size");
            return;
        }
        
        val places = new Rail[Int](backups.size());
        val nominations = new HashMap[Int,Int]();//finish home-master place
        var i:Long = 0;
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            if (verbose>=1) debug("===== createMasters nominate for id["+b.id+"] ");
            var pl:Int = nominations.getOrElse(b.placeOfMaster,-1n);
            if (pl == -1n) {
                pl = FinishReplicator.nominateMasterPlaceIfDead(b.placeOfMaster);
                nominations.put(b.placeOfMaster, pl);
            }
            places(i++) = pl;
        }
        
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val _backupPlaceId = here.id as Int;
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            var i:Long = 0;
            for (bx in backups) {
                val b = bx as OptimisticBackupState;
                val _id = b.id;
                val _parentId = b.parentId;
                val _numActive = b.numActive;
                val _sent = b.sent;
                val _transit = b.transit;
                val _ghosts = b.ghostChildren;
                val _excs = b.excs;
                val _tx = b.tx;
                val _rootTx = b.isRootTx;
                
                var mem:Set[Int] = null;
                var excs:GrowableRail[CheckedThrowable] = null;
                var ro:Boolean = false;
                if (_tx != null) {
                    val o = b.tx.getBackupClone();
                    mem = o.members;
                    excs = o.excs;
                    ro = o.readOnly;
                }
                val _tx_mem = mem;
                val _tx_excs = excs;
                val _tx_ro = ro;
                
                val master = Place(places(i));
                if (master.isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    if (verbose>=1) debug("===== createMasters going to master["+master+"] to create finish["+_id+"] ");
                    at (master) @Immediate("create_masters") async {
                        val newM = new OptimisticMasterState(_id, _parentId, _numActive, 
                        		_sent, _transit, _ghosts, _excs, _backupPlaceId, _tx, _rootTx, _tx_mem, _tx_excs, _tx_ro);
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
        
        if (fin.failed()) {
            var str:String = "";
            for (p in places)
                str += p + "," + Place(p as Long).isDead()+ " , ";
            throw new Exception("FATAL ERROR: another place failed during recovery ["+str+"] ...");
        }

        i = 0;
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            b.lock();
            b.placeOfMaster = places(i);
            b.isRecovered = true;
            b.migrating = false;
            b.unlock();
        }
        FinishReplicator.applyNominatedMasterPlaces(nominations);
        if (verbose>=1) debug("<<<< createMasters(size="+backups.size()+") returning");
    }
    
    static def createOrSyncBackups(newDead:HashSet[Int], masters:HashSet[FinishMasterState]) {
        if (verbose>=1) debug(">>>> createOrSyncBackups(size="+masters.size()+") called");
        
        val places = new Rail[Int](masters.size());
        val newBackups = new HashMap[Id,Int](); //id to backup, -1n means 
        val iter = masters.iterator();
        var i:Long = 0;
        while (iter.hasNext()) {
            val m = iter.next() as OptimisticMasterState;
            val newB = FinishReplicator.updateBackupPlaceIfDead(m.id.home);
            places(i) = newB;
            //don't update m.backupPlaceId until the recovered backups are created. Otherwise, pleases that are calling getNewBackup may reach the backup before it is created
            newBackups.put(m.id, newB); 
            i++;
            
            if (verbose>=3) {
                debug(">>>> sync from master to backup ("+m.id+") newB["+newB+"] m.backupPlaceId["+m.backupPlaceId+"] m.backupChanged["+m.backupChanged+"]");
                m.dump();
            }
        }
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val placeOfMaster = here.id as Int;
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (mx in masters) {
                val m = mx as OptimisticMasterState;
                val backup = Place(newBackups.getOrThrow(m.id));
                val id = m.id;
                val parentId = m.parentId;
                val numActive = m.numActive;
                val sent = m.sent;
                val transit = m.transit;
                val ghosts = m.ghostChildren;
                val excs = m.excs;
                val tx = m.tx;
                val rootTx = m.isRootTx;
                if (backup.isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (backup) @Immediate("create_or_sync_backup") async {
                        FinishReplicator.createOptimisticBackupOrSync(id, parentId, numActive, 
                                sent, transit, ghosts, excs, placeOfMaster, tx, rootTx);
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
        
        if (fin.failed()) {
            var str:String = "";
            for (p in places)
                str += p + "," + Place(p as Long).isDead()+ " , ";
            Console.OUT.println("FATAL ERROR: another place failed during recovery ["+str+"] ...");
            System.killHere();
        }
        
        for (mx in masters) {
            val m = mx as OptimisticMasterState;
            m.lock();
            val newB2 = newBackups.getOrThrow(m.id);
            if (newB2 != m.backupPlaceId) {
                m.backupPlaceId = newB2;
                m.backupChanged = true;
                if (m.tx != null){
                    (m.tx as TxResilient).notifyBackupChange(newB2);
                }
            }
            m.migrating = false;
            m.unlock();
        }
        if (verbose>=1) debug("<<<< createOrSyncBackups(size="+masters.size()+") returning");
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (Runtime.activity() == null) {
            if (verbose>=1) debug(">>>> notifyPlaceDeath returning, IGNORED REQUEST FROM IMMEDIATE THREAD");
            return; 
        }
        val newDead = FinishReplicator.getNewDeadPlaces();
        if (newDead == null || newDead.size() == 0) //occurs at program termination
            return;
        
        val hereId = here.id as Int;
        
        val masters = FinishReplicator.getImpactedMasters(newDead); //any master who contacted the dead place or whose backup was lost
        val backups = FinishReplicator.getImpactedBackups(newDead); //any backup who lost its master.
        val myBackupDied = newDead.contains(FinishReplicator.getBackupPlace(hereId));
        
        //prevent updates on backups since they are based on decisions made by a dead master
        for (bx in backups) {
            val b = bx as OptimisticBackupState;
            b.lock();
            b.migrating = true;
            b.unlock();
        }
        
        val countingReqs = aggregateCountingRequests(newDead, masters, backups); // combine all requests targetted to a specific place
        processCountingRequests(countingReqs); //obtain the counts
        
        //merge all results
        val countChildrenBackups = new HashMap[FinishResilient.Id, HashSet[FinishResilient.Id]]();
        val countDropped = new HashMap[FinishResilientOptimistic.DroppedQueryId, Int]();
        for (e in countingReqs.entries()) {
            val v = e.getValue();
            for (ve in v.countChildren .entries()) {
                val set = ve.getValue();
                countChildrenBackups.put(ve.getKey().parentId, set);
                if (verbose>=1) debug(">>>> notifyPlaceDeath countChildrenBackups.put( id="+ve.getKey().parentId+", set="+ setToString(set) +" )");
            }
            for (vr in v.countDropped.entries()) {
                countDropped.put(vr.getKey(), vr.getValue());
                if (verbose>=1) debug(">>>> notifyPlaceDeath countDropped.put( id="+vr.getKey().id+", src="+vr.getKey().src+", set="+ vr.getValue() +" )");
            }
        }
        
        //update counts and check if quiecent reached
        for (m in masters) {
            val master = m as OptimisticMasterState;
            
            master.lock();
            
            if (master.txStarted) {
                master.notifyTxPlaceDeath();
            } else {
                //convert to dead
                master.convertToDead(newDead, countChildrenBackups);
                if (master.numActive > 0) {
                    //convert from dead
                    master.convertFromDead(countDropped);
                }
            }
            
            master.migrating = true; //prevent updates to masters as we are copying the data, 
                                     //and we want to ensure that backup is created before processing new requests
            master.unlock();
        }
        
        if (masters.size() > 0)
            createOrSyncBackups(newDead, masters);
        else {
            if (myBackupDied) {
                val newBackup = Place(FinishReplicator.updateBackupPlaceIfDead(hereId));
                val rCond = ResilientCondition.make(newBackup);
                val closure = (gr:GlobalRef[Condition]) => {
                    at (newBackup) @Immediate("dummy_backup") async {
                        FinishReplicator.createDummyBackup(hereId);
                        at (gr) @Immediate("dummy_backup_response") async {
                            gr().release();
                        }
                    }
                };
                rCond.run(closure);
            }
        }

        val activeBackups = new HashSet[FinishBackupState]();
        //sorting the backups is not needed. notifyParent calls are done using uncounted activities that will retry until the parent is available
        for (b in backups) {
            val backup = b as OptimisticBackupState;
            
            backup.lock();
            
            if (backup.getTxStarted()) {
                backup.notifyTxPlaceDeath();
            } else {
                //convert to dead
                backup.convertToDead(newDead, countChildrenBackups);
                if (!backup.isReleased) {
                    //convert from dead
                    backup.convertFromDead(countDropped);
                }
            }
            backup.unlock(); //as long as we keep migrating = true, no changes will be permitted on backup
        }
        
        for (b in backups) {
            val backup = b as OptimisticBackupState;
            backup.lock();
            if (verbose>=1) debug("==== notifyPlaceDeath isBackupActive => id["+backup.id+"] backup.isReleased["+backup.isReleased+"] txStarted["+backup.getTxStarted()+"]");
            if (!backup.getTxStarted()) { 
                if (backup.isReleased) {
                    if (verbose>=1) debug("==== notifyPlaceDeath isBackupActive => id["+backup.id+"] backup.isReleased["+backup.isReleased+"] txStarted["+backup.getTxStarted()+"]   TRY_RELEASE");
                    backup.backupTryRelease();
                } else {
                    activeBackups.add(backup);
                    if (verbose>=1) debug("==== notifyPlaceDeath isBackupActive => id["+backup.id+"] backup.isReleased["+backup.isReleased+"] txStarted["+backup.getTxStarted()+"]   ADD_TO_ACTIVE");
                }
            }
            backup.unlock(); //as long as we keep migrating = true, no changes will be permitted on backup
        }
        
        if (activeBackups.size() > 0) {
            createMasters(activeBackups);
        } else {
            if (verbose>=1) debug("==== createMasters bypassed ====");
        }
        
        if (verbose>=1) debug("==== handling non-blocking pending requests ====");
        FinishReplicator.submitDeadBackupPendingRequests(newDead);
        FinishReplicator.submitDeadMasterPendingRequests(newDead);
        
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
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