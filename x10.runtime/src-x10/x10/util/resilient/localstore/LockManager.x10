/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;

public class LockManager (store:ResilientStore, mapName:String, list:PlaceLocalHandle[TransactionsList]) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public def startBlockingTransaction():BlockingTx {
        assert(store.plh().virtualPlaceId != -1);
        val id = store.plh().masterStore.getNextTransactionId();
        val tx = new BlockingTx(id);
        list().addBlockingTx(tx);
        return tx;
    }
    
    /***************** Locking ********************/
    
    public def lockWrite(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) store.plh().masterStore.lockWrite(mapName, id, key1);
            at (p2) store.plh().masterStore.lockWrite(mapName, id, key2);
        }
        else {
            at (p2) store.plh().masterStore.lockWrite(mapName, id, key2);
            at (p1) store.plh().masterStore.lockWrite(mapName, id, key1);
        }
    }
    public def unlockWrite(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) store.plh().masterStore.unlockWrite(mapName, id, key1);
            at (p2) store.plh().masterStore.unlockWrite(mapName, id, key2);
        }
        else {
            at (p2) store.plh().masterStore.unlockWrite(mapName, id, key2);
            at (p1) store.plh().masterStore.unlockWrite(mapName, id, key1);
        }
    }
    
    public def lockRead(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) store.plh().masterStore.lockRead(mapName, id, key1);
            at (p2) store.plh().masterStore.lockRead(mapName, id, key2);
        }
        else {
            at (p2) store.plh().masterStore.lockRead(mapName, id, key2);
            at (p1) store.plh().masterStore.lockRead(mapName, id, key1);
        }
    }
    public def unlockRead(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) store.plh().masterStore.unlockRead(mapName, id, key1);
            at (p2) store.plh().masterStore.unlockRead(mapName, id, key2);
        }
        else {
            at (p2) store.plh().masterStore.unlockRead(mapName, id, key2);
            at (p1) store.plh().masterStore.unlockRead(mapName, id, key1);
        }
    }
    
    public def lockWrite(key:String, id:Long) {
        store.plh().masterStore.lockWrite(mapName, id, key);
    }
    
    public def unlockWrite(key:String, id:Long) {
        store.plh().masterStore.unlockWrite(mapName, id, key);
    }
    
    public def lockRead(key:String, id:Long) {
        store.plh().masterStore.lockRead(mapName, id, key);
    }
    
    public def unlockRead(key:String, id:Long) {
        store.plh().masterStore.unlockRead(mapName, id, key);
    }
    
    /***************** Get ********************/
    public def getLocked(key:String, id:Long):Cloneable {
        return store.plh().masterStore.getLocked(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def putLocked(key:String, value:Cloneable, id:Long):Cloneable {
        return store.plh().masterStore.putLocked(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def deleteLocked(key:String, id:Long):Cloneable {
        return store.plh().masterStore.deleteLocked(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet(id:Long):Set[String] {
        return store.plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        at (dest) closure();
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return (at (dest) closure()) as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
        val future = Future.make[Any]( () => at (dest) { closure(); return null;} );                
        return future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
        val future = Future.make[Any]( () => at (dest) { return closure(); } );                
        return future;
    }   
    
    
    public def printTxStatistics() {
        Console.OUT.println("Calculating execution statistics ...");
        val pl_stat = new ArrayList[BlockingTxPlaceStatistics]();
        for (p in store.activePlaces) {
            val pstat = at (p) {
            	val totalList = new ArrayList[Double](); 
            	val lockingList = new ArrayList[Double](); 
            	val processingList = new ArrayList[Double](); 
            	val waitingList = new ArrayList[Double]();
            	val unlockingList = new ArrayList[Double]();
                

                for (tx in list().blockingTx) {
                	totalList.add(tx.totalElapsedTime);
                	lockingList.add(tx.lockingElapsedTime);
                	processingList.add(tx.processingElapsedTime);
                	waitingList.add(tx.waitElapsedTime);
                	unlockingList.add(tx.unlockingElapsedTime);
                }
                 
                new BlockingTxPlaceStatistics(here, totalList, lockingList, processingList, waitingList, unlockingList)
            };
            pl_stat.add(pstat);
        }
        
        val allTotalList = new ArrayList[Double]();
        val allLockingList = new ArrayList[Double]();
        val allProcList = new ArrayList[Double]();
        val allWaitList = new ArrayList[Double]();
        val allUnlockingList = new ArrayList[Double]();
        
        for (pstat in pl_stat) {
            val str = pstat.toString();
            if (!str.equals(""))
                Console.OUT.println(str);
            allTotalList.addAll(pstat.totalList);
            allLockingList.addAll(pstat.lockingList);
            allProcList.addAll(pstat.processingList);
            allWaitList.addAll(pstat.waitingList);
            allUnlockingList.addAll(pstat.unlockingList);
        }
        
        val cnt   = allTotalList.size();
        val totalMean  = TxStatistics.mean(allTotalList);
        val totalSTDEV = TxStatistics.stdev(allTotalList, totalMean);
        val totalBox   = TxStatistics.boxPlot(allTotalList);
        
        val lockingMean  = TxStatistics.mean(allLockingList);
        val lockingSTDEV = TxStatistics.stdev(allLockingList, lockingMean);
        val lockingBox   = TxStatistics.boxPlot(allLockingList);
        
        val procMean  = TxStatistics.mean(allProcList);
        val procSTDEV = TxStatistics.stdev(allProcList, procMean);
        val procBox   = TxStatistics.boxPlot(allProcList);
        
        val waitMean  = TxStatistics.mean(allWaitList);
        val waitSTDEV = TxStatistics.stdev(allWaitList, waitMean);
        val waitBox   = TxStatistics.boxPlot(allWaitList);
        
        val unlockingMean  = TxStatistics.mean(allUnlockingList);
        val unlockingSTDEV = TxStatistics.stdev(allUnlockingList, unlockingMean);
        val unlockingBox   = TxStatistics.boxPlot(allUnlockingList);
        
        Console.OUT.println("Summary:count:"+ cnt + 
        		":totalMeanMS:"     + totalMean     + ":totalSTDEV:"     + totalSTDEV      + ":totalBox:(:" + totalBox + ":)" +
        		":lockingMeanMS:"   + lockingMean   + ":lockingSTDEV:"   + lockingSTDEV    + ":lockingBox:(:" + lockingBox + ":)" +
        		":procMeanMS:"      + procMean      + ":procSTDEV:"      + procSTDEV       + ":procBox:(:" + procBox + ":)" +
        		":waitMeanMS:"      + waitMean      + ":waitTDEV:"       + waitSTDEV       + ":waitBox:(:" + waitBox + ":)" +
        		":unlockingMeanMS:" + unlockingMean + ":unlockingSTDEV:" + unlockingSTDEV  + ":unlockingBox:(:" + unlockingBox + ":)"
        ); 
       
    }
    
}

class BlockingTxPlaceStatistics(p:Place, totalList:ArrayList[Double], lockingList:ArrayList[Double], processingList:ArrayList[Double], waitingList:ArrayList[Double], unlockingList:ArrayList[Double]) {
	public def toString() {
		val totalMean     = TxStatistics.mean(totalList);
		val totalSTDEV    = TxStatistics.stdev(totalList, totalMean);
		val totalBox      = TxStatistics.boxPlot(totalList);        
		
		val lockingMean     = TxStatistics.mean(lockingList);
		val lockingSTDEV    = TxStatistics.stdev(lockingList, lockingMean);
		val lockingBox      = TxStatistics.boxPlot(lockingList);
		
		val procMean     = TxStatistics.mean(processingList);
		val procSTDEV    = TxStatistics.stdev(processingList, procMean);
		val procBox      = TxStatistics.boxPlot(processingList);
		
		val waitMean     = TxStatistics.mean(waitingList);
		val waitSTDEV    = TxStatistics.stdev(waitingList, waitMean);
		val waitBox      = TxStatistics.boxPlot(waitingList);
		
		val unlockingMean     = TxStatistics.mean(unlockingList);
		val unlockingSTDEV    = TxStatistics.stdev(unlockingList, unlockingMean);
		val unlockingBox      = TxStatistics.boxPlot(unlockingList);
		

		return p + ":count:" + totalList.size() + 
				":totalMeanMS:"   + totalMean   + ":totalSTDEV:"   + totalSTDEV   + ":totalBox:(:" + totalBox     + ":)" + 
		        ":lockingMeanMS:" + lockingMean + ":lockingSTDEV:" + lockingSTDEV + ":lockingBox:(:" + lockingBox + ":)" +
		        ":procMeanMS:"    + procMean    + ":procSTDEV:"    + procSTDEV    + ":procBox:(:"    + procBox + ":)" +
		        ":waitMeanMS:"    + waitMean    + ":waitSTDEV:"    + waitSTDEV    + ":waitBox:(:" + waitBox + ":)" +
		        ":unlockingMeanMS:" + unlockingMean + ":unlockingSTDEV:" + unlockingSTDEV + ":unlockingBox:(:" + unlockingBox + ":)";
	}
}