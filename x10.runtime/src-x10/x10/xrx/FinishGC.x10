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

import x10.util.HashMap;
import x10.util.Set;
import x10.util.HashSet;
import x10.util.concurrent.Lock;
import x10.compiler.Immediate;
import x10.util.concurrent.AtomicInteger;

/**
 * This class records the visited places per each finish in a Set[Int].
 * Cleaning the finishes is done in group of size GC_MAX_PENDING.
 * */
public class FinishGC {
    
    private static val lock = new Lock();
    
    private static grMap = new HashMap[GlobalRef[FinishState],Set[Long]]();
    private static grMapGCReady = new HashMap[Long,Set[GlobalRef[FinishState]]]();

    private static idMap = new HashMap[FinishResilient.Id,Set[Long]]();
    private static idMapGCReady = new HashMap[Long,Set[FinishResilient.Id]]();

    private static val pending = new AtomicInteger(0n);
    
    public static val GC_DISABLED = System.getenv("FINISH_GC_DISABLE") == null ? false : Long.parseLong(System.getenv("FINISH_GC_DISABLE")) == 1;
    public static val GC_DEBUG = System.getenv("FINISH_GC_DEBUG") == null ? false : Long.parseLong(System.getenv("FINISH_GC_DEBUG")) == 1;
    public static val GC_MAX_PENDING = System.getenv("FINISH_GC_MAX_PENDING") == null ? 25 : Int.parseInt(System.getenv("FINISH_GC_MAX_PENDING"));
    public static val GC_PIGGYBACKING = System.getenv("FINISH_GC_PIGGYBACKING") == null ? false : Long.parseLong(System.getenv("FINISH_GC_PIGGYBACKING")) == 1;
    
    public static def add(gr:GlobalRef[FinishState], places:Set[Long]) {
        if (GC_DISABLED) return;
        lock.lock();
        grMap.put(gr, places);
        lock.unlock();
    }
    
    public static def addGCReady(gr:GlobalRef[FinishState], places:Set[Long]) {
        if (GC_DISABLED) return;
        lock.lock();
        addGCReadyUnsafe(gr, places);
        lock.unlock();
    }
    
    public static def addGCReady(gr:GlobalRef[FinishState]) {
        if (GC_DISABLED) return;
        lock.lock();
        val places = grMap.remove(gr);
        addGCReadyUnsafe(gr, places);
        lock.unlock();
    }
    
    private static def addGCReadyUnsafe(gr:GlobalRef[FinishState], places:Set[Long]) {
        for (p in places) {
            var s:Set[GlobalRef[FinishState]] = grMapGCReady.getOrElse(p, null);
            if (s == null) {
                s = new HashSet[GlobalRef[FinishState]]();
                grMapGCReady.put(p, s);
            }
            s.add(gr);
        }
        
        if (pending.incrementAndGet() == GC_MAX_PENDING) {
            cleanGrMapUnsafe();
            pending.set(0n);
        }
    }
    
    private static def cleanGrMapUnsafe() {
        for (placeId in grMapGCReady.keySet()) {
            val grSet = grMapGCReady.getOrThrow(placeId);
            at(Place(placeId)) @Immediate("gr_remoteFinishCleanup") async {
                if (GC_DEBUG) Console.OUT.println("GCLog: here["+here+"] deleting objects " + logSet(grSet));
                for (root in grSet) {
                    Runtime.finishStates.remove(root);
                }
            }
        }
        grMapGCReady.clear();
    }
    
    public static def add(id:FinishResilient.Id, places:Set[Long]) {
        if (GC_DISABLED) return;
        lock.lock();
        idMap.put(id, places);
        lock.unlock();
    }
    
    public static def addGCReady(id:FinishResilient.Id, places:Set[Long]) {
        if (GC_DISABLED) return;
        lock.lock();
        addGCReadyUnsafe(id, places);
        lock.unlock();
    }
    
    public static def addGCReady(id:FinishResilient.Id) {
        if (GC_DISABLED) return;
        lock.lock();
        val places = idMap.remove(id);
        addGCReadyUnsafe(id, places);
        lock.unlock();
    }
    
    private static def addGCReadyUnsafe(id:FinishResilient.Id, places:Set[Long]) {
        for (p in places) {
            var s:Set[FinishResilient.Id] = idMapGCReady.getOrElse(p, null);
            if (s == null) {
                s = new HashSet[FinishResilient.Id]();
                idMapGCReady.put(p, s);
            }
            s.add(id);
        }
        if (pending.incrementAndGet() == GC_MAX_PENDING) {
            cleanIdMapUnsafe();
            pending.set(0n);
        }
    }
    
    private static def cleanIdMapUnsafe() {
        for (placeId in idMapGCReady.keySet()) {
            val idSet = idMapGCReady.getOrThrow(placeId);
            at(Place(placeId)) @Immediate("id_remoteFinishCleanup") async {
                if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC) {
                    if (GC_DEBUG) Console.OUT.println("GCLog: here["+here+"] deleting objects " + logSet(idSet));
                    FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObjects(idSet);
                } else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC){
                    if (GC_DEBUG) Console.OUT.println("GCLog: here["+here+"] deleting objects " + logSet(idSet));
                    FinishResilientOptimistic.OptimisticRemoteState.deleteObjects(idSet);
                }
            }
        }
        idMapGCReady.clear();
    }
    
    private static def logSet(idSet:Set[FinishResilient.Id]) {
        if (idSet == null)
            return "";
        var str:String = "";
        for (s in idSet)
            str += s + " : ";
        return str;
    }
    private static def logSet(grSet:Set[GlobalRef[FinishState]]) {
        if (grSet == null)
            return "";
        var str:String = "";
        for (s in grSet)
            str += s + " : ";
        return str;
    }

}