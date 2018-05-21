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
import x10.util.HashSet;
import x10.util.concurrent.Lock;
import x10.compiler.Immediate;

/**
 * This class records the visited places per each finish in a HashSet[Int].
 * Cleaning the finishes is done in group of size GC_MAX_PENDING.
 * */
public class FinishGC {
    
    private static val lock = new Lock();
    
    private static grMap = new HashMap[GlobalRef[FinishState],HashSet[Int]]();
    private static grMapGCReady = new HashMap[GlobalRef[FinishState],HashSet[Int]]();

    private static idMap = new HashMap[FinishResilient.Id,HashSet[Int]]();
    private static idMapGCReady = new HashMap[FinishResilient.Id,HashSet[Int]]();

    public static val GC_MAX_PENDING = System.getenv("FINISH_GC_MAX_PENDING") == null ? 100 : Long.parseLong(System.getenv("FINISH_GC_MAX_PENDING"));
    public static val GC_DEBUG = System.getenv("FINISH_GC_DEBUG") == null ? false : Long.parseLong(System.getenv("FINISH_GC_DEBUG")) == 1;
    public static val GC_DISABLED = System.getenv("FINISH_GC_DISABLE") == null ? false : Long.parseLong(System.getenv("FINISH_GC_DISABLE")) == 1;
    
    public static def add(gr:GlobalRef[FinishState], places:HashSet[Int]) {
        if (GC_DISABLED) return;
        lock.lock();
        grMap.put(gr, places);
        lock.unlock();
    }
    
    public static def addGCReady(gr:GlobalRef[FinishState], places:HashSet[Int]) {
        if (GC_DISABLED) return;
        lock.lock();
        grMap.remove(gr);
        grMapGCReady.put(gr, places);
        if (grMapGCReady.size() == GC_MAX_PENDING)
            cleanGrMapUnsafe();
        lock.unlock();
    }
    
    private static def cleanGrMapUnsafe() {
        val start = System.nanoTime();
        //merge GC requests per place
        val gcPerPlace = new HashMap[ Int , HashSet[ GlobalRef[FinishState] ] ]();
        for ( entry in grMapGCReady.entries()) {
            val gr = entry.getKey();
            val places = entry.getValue();
            for (p in places) {
                var s:HashSet[GlobalRef[FinishState]] = gcPerPlace.getOrElse(p, null);
                if (s == null) {
                    s = new HashSet[GlobalRef[FinishState]]();
                    gcPerPlace.put(p, s);
                }
                s.add(gr);
            }
        }
        grMapGCReady.clear();
        
        for (placeId in gcPerPlace.keySet()) {
            val grSet = gcPerPlace.getOrThrow(placeId);
            at(Place(placeId)) @Immediate("gr_remoteFinishCleanup") async {
                for (root in grSet)
                    Runtime.finishStates.remove(root);
            }
        }
        val end = System.nanoTime();
        if (GC_DEBUG)
            Console.OUT.println("GC time: " + (end-start)/1E9 + " seconds");
    }
    
    public static def add(id:FinishResilient.Id, places:HashSet[Int]) {
        if (GC_DISABLED) return;
        lock.lock();
        idMap.put(id, places);
        lock.unlock();
    }
    
    public static def addGCReady(id:FinishResilient.Id, places:HashSet[Int]) {
        if (GC_DISABLED) return;
        lock.lock();
        idMap.remove(id);
        idMapGCReady.put(id, places);
        if (idMapGCReady.size() == GC_MAX_PENDING)
            cleanIdMapUnsafe();
        lock.unlock();
    }
    
    private static def cleanIdMapUnsafe() {
        val start = System.nanoTime();
        //merge GC requests per place
        val gcPerPlace = new HashMap[ Int , HashSet[ FinishResilient.Id ] ]();
        for ( entry in idMapGCReady.entries()) {
            val id = entry.getKey();
            val places = entry.getValue();
            for (p in places) {
                var s:HashSet[ FinishResilient.Id ] = gcPerPlace.getOrElse(p, null);
                if (s == null) {
                    s = new HashSet[ FinishResilient.Id ]();
                    gcPerPlace.put(p, s);
                }
                s.add(id);
            }
        }
        idMapGCReady.clear();
        
        for (placeId in gcPerPlace.keySet()) {
            val idSet = gcPerPlace.getOrThrow(placeId);
            at(Place(placeId)) @Immediate("id_remoteFinishCleanup") async {
                if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC)
                    FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObjects(idSet);
                else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC)
                    FinishResilientOptimistic.OptimisticRemoteState.deleteObjects(idSet);
            }
        }
        val end = System.nanoTime();
        if (GC_DEBUG)
            Console.OUT.println("GC time: " + (end-start)/1E9 + " seconds");
    }
}