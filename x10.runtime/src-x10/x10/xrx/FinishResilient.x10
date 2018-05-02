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

import x10.compiler.*;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicBoolean;
import x10.util.concurrent.Condition;
import x10.util.concurrent.AtomicInteger;
import x10.util.HashMap;
import x10.util.HashSet;

/*
 * Common abstract class for Resilient Finish
 */
public abstract class FinishResilient extends FinishState {
    /*
     * for debug
     */
    public static val verbose = getEnvLong("X10_RESILIENT_VERBOSE"); // should be copied to subclass
    protected static def getEnvLong(name:String) {
        val env = System.getenv(name);
        val v = (env!=null) ? Long.parseLong(env) : 0;
        if (v>0 && here.id==0) Console.OUT.println(name + "=" + v);
        return v;
    }
    protected static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    protected static def dumpStack(msg:String) {
        try { throw new Exception(msg); } catch (e:Exception) { e.printStackTrace(); }
    }
    
    // TODO: We should empirically tune this size for performance.
    //       Initially I am picking a size that will make it very likely
    //       that we will use a mix of both direct and indirect protocols
    //       for a large number of test cases to shake out mixed-mode problems.
    protected static val ASYNC_SIZE_THRESHOLD = Long.parse(Runtime.env.getOrElse("X10_RESILIENT_FINISH_SMALL_ASYNC_SIZE", "0"));
    
    public static struct Id(home:int,id:int) {
        public def toString() = "<"+home+","+id+">";
    }
    
    public static val UNASSIGNED = Id(-1n,-1n);
    
    /* The implicit top finish is not replicated in the replication-based implementations.
     * Place 0 starts the shutdown procedure when that finish is released, and sometimes, shutting down
     * occurs while places are exchanging final replication messages. 
     * This behaviour caused the programs to hang while shutting down. Therefore special termination logic
     * is added for the TOP_FINISH in replication-based implementations */
    public static val TOP_FINISH = Id(0n,0n);
    
    public static val AT = 0n;
    public static val ASYNC = 1n;

    public static struct Task(place:Int, kind:Int) {
        public def toString() {
            if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC ||
                Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC )
                return "<"+(kind == AT ? "at" : "async")+" from "+place+">";
            else return "<"+(kind == AT ? "at" : "async")+" live @ "+place+">";
        }
        
        def this(place:Long, kind:Int) {
            property(place as Int, kind);
        }
    }

    protected static struct Edge(src:Int, dst:Int, kind:Int) {
        public def toString() = "<"+(kind == AT ? "at" : "async")+" from "+src+" to "+dst+">";
        def this(srcId:Long, dstId:Long, kind:Int) {
            property(srcId as Int, dstId as Int, kind);
        }
    }
    
    protected static val DISABLE_NONBLOCKING_REPLICATION:Boolean = (System.getenv("DISABLE_NONBLOCKING_REPLICATION") != null 
            && System.getenv("DISABLE_NONBLOCKING_REPLICATION").equals("1"));
    
    protected static struct ReplicatorResponse(submit:Boolean, adopterId:Id) {
        public def toString() = "<repResponse submit="+submit+", adopterId="+adopterId+">";
    }
    
    /* Recovery related structs to hold query parameters needed to count the number of live tasks */
    protected static struct ReceivedQueryId(id:Id, src:Int, dst:Int, kind:Int) {
        public def toString() = "<receivedQuery id=" + id + " src=" + src + " dst="+dst+" kind="+kind+">";
    }
    
    protected static struct DroppedQueryId(id:Id, src:Int, dst:Int, kind:Int, sent:Int) {
        public def toString() = "<droppedQueryId id=" + id + " src=" + src + " dst="+dst+" kind="+kind+" sent="+sent+" >";
    }
    
    protected static struct ChildrenQueryId(parentId:Id, dead:Int, src:Int) {
        public def toString() = "<ChildrenQueryId parentId=" + parentId + " dead="+dead+" src=" + src +">";
    }
    
    protected static val nextId = new AtomicInteger(); // per-place portion of unique id
    
    /*
     * Static methods to be implemented in subclasses
     */
    // static def make(parent:FinishState):FinishResilient;
    // static def notifyPlaceDeath():void;
    
    /*
     * Other methods to be implemented in subclasses (declared in FinishState class)
     */
    // def notifySubActivitySpawn(place:Place):void;
    // def notifyShiftedActivitySpawn(place:Place):void;
    // def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean;
    // def notifyShiftedActivityCreation(srcPlace:Place):Boolean;
    // def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void;
    // def notifyActivityCreatedAndTerminated(srcPlace:Place):void;
    // def notifyActivityTermination(srcPlace:Place):void;
    // def notifyShiftedActivityTermination():void;
    // def pushException(t:CheckedThrowable):void;
    // def waitForFinish():void;

    private static def failJavaOnlyMode() {
        throw new UnsupportedOperationException("Java-only RESILIENT_MODE " + Runtime.RESILIENT_MODE);
    }
    
    /*
     * Dispatcher methods
     */
    private static def getCurrentFS() {
        val a = Runtime.activity();
        return (a!=null) ? a.finishState() : null;
    }
    
    static def make(parent:FinishState):FinishState { // parent may be null
        var fs:FinishState;
        switch (Runtime.RESILIENT_MODE) {
        case Configuration.RESILIENT_MODE_DEFAULT:
        case Configuration.RESILIENT_MODE_PLACE0:
        {
            val p = (parent!=null) ? parent : getCurrentFS();
            if (verbose>=1) debug("FinishResilient.make called, parent=" + parent + " p=" + p);
            fs = FinishResilientPlace0.make(p);
            break;
        }
        case Configuration.RESILIENT_MODE_DIST_OPTIMISTIC:
        {
            val p = (parent!=null) ? parent : getCurrentFS();
            if (verbose>=1) debug("FinishResilient.make called, parent=" + parent + " p=" + p);
            fs = FinishResilientOptimistic.make(p);
            break;
        }
        case Configuration.RESILIENT_MODE_DIST_PESSIMISTIC:
        {
            val p = (parent!=null) ? parent : getCurrentFS();
            if (verbose>=1) debug("FinishResilient.make called, parent=" + parent + " p=" + p);
            fs = FinishResilientPessimistic.make(p);
            break;
        }
        case Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC:
        {
            val p = (parent!=null) ? parent : getCurrentFS();
            if (verbose>=1) debug("FinishResilient.make called, parent=" + parent + " p=" + p);
            fs = FinishResilientPlace0Optimistic.make(p);
            break;
        }
        case Configuration.RESILIENT_MODE_HC:
        {
           val p = (parent!=null) ? parent : getCurrentFS();
           if (verbose>=1) debug("FinishResilient.make called, parent=" + parent + " p=" + p);
           val o = p as Any;
           fs = makeFinishResilientHCLocal(o);
           break;
        }
        default:
            throw new UnsupportedOperationException("Unsupported RESILIENT_MODE " + Runtime.RESILIENT_MODE);
        }
        if (verbose>=1) debug("FinishResilient.make returning, fs=" + fs);
        return fs;
    }

    @Native("java", "x10.xrx.managed.FinishResilientHCLocal.make(#o)")
    private static def makeFinishResilientHCLocal(o:Any):FinishState {
        failJavaOnlyMode();
        return null;
    }

    static def notifyPlaceDeath() {
        if (verbose>=1) debug("FinishResilient.notifyPlaceDeath called");
        switch (Runtime.RESILIENT_MODE) {
        case Configuration.RESILIENT_MODE_DEFAULT:
        case Configuration.RESILIENT_MODE_PLACE0:
            FinishResilientPlace0.notifyPlaceDeath();
            break;
        case Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC:
            FinishResilientPlace0Optimistic.notifyPlaceDeath();
            break;
        case Configuration.RESILIENT_MODE_HC:
            notifyPlaceDeath_HC();
            break;
        case Configuration.RESILIENT_MODE_DIST_PESSIMISTIC:
        	FinishResilientPessimistic.notifyPlaceDeath();
        	break;
        case Configuration.RESILIENT_MODE_DIST_OPTIMISTIC:
        	FinishResilientOptimistic.notifyPlaceDeath();
        	break;
        default:
            throw new UnsupportedOperationException("Unsupported RESILIENT_MODE " + Runtime.RESILIENT_MODE);
        }
        atomic {
            // we do this in an atomic block to unblock whens which check for dead places
            if (verbose>=1) debug("FinishResilient.notifyPlaceDeath returning");
        }
    }

    @Native("java", "x10.xrx.managed.FinishResilientHC.notifyPlaceDeath()")
    private static def notifyPlaceDeath_HC():void {
        failJavaOnlyMode(); 
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
    
    static @Inline def deduct[K](map:HashMap[K,Int], k:K, cnt:Int) {
        val oldCount = map(k);
        if (oldCount == cnt) {
             map.remove(k);
        } else {
             map(k) = oldCount-cnt;
        }
    }
    
    static def printResolveReqs(countingReqs:HashMap[Int,OptResolveRequest]) {
        val s = new x10.util.StringBuilder();
        if (countingReqs.size() > 0) {
            for (e in countingReqs.entries()) {
                val pl = e.getKey();
                val reqs = e.getValue();
                val bkps = reqs.countChildren ;
                val recvs = reqs.countDropped;
                s.add("\nRecovery requests:\n");
                s.add("   To place: " + pl + "\n");
                if (bkps.size() > 0) {
                    s.add("  countChildren :{ ");
                    for (b in bkps.entries()) {
                        s.add(b.getKey() + " ");
                    }
                    s.add("}\n");
                }
                if (recvs.size() > 0) {
                    s.add("  countDropped:{");
                    for (r in recvs.entries()) {
                        s.add("<id="+r.getKey().id+",src="+r.getKey().src + ",sent="+r.getKey().sent+">, ");
                    }
                    s.add("}\n");
                }
            }
        }
        debug(s.toString());
    }
}

class RemoteCreationDenied extends Exception {}
class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterMigrating extends Exception {}
class MasterChanged(newMasterPlace:Int)  extends Exception {} //optimistic only
class MasterAndBackupDied extends Exception {}
class BackupCreationDenied extends Exception {}
class OptResolveRequest { //used in optimistic protocols only
    val countChildren  = new HashMap[FinishResilient.ChildrenQueryId, HashSet[FinishResilient.Id]]();
    val countDropped = new HashMap[FinishResilient.DroppedQueryId, Int]();
}
class SearchBackupResponse {
    var found:Boolean = false;
}
class GetNewMasterResponse {
    var found:Boolean = false;
    var newMasterPlace:Int = -1n;
    var newMasterId:FinishResilient.Id = FinishResilient.UNASSIGNED;
}