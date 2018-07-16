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

import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicInteger;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.tx.logging.TxDesc;

public class MasterStore[K] {K haszero} {
    /*Each map has an object of TxManager (same object even after failures)*/
    private val txManager:TxManager[K];
    private val sequence:AtomicInteger;
    private val immediateRecovery:Boolean;
    public static val TX_FACTOR=1000000;
    
    public def this(masterMap:HashMap[K,Cloneable], immediateRecovery:Boolean) {
        this.immediateRecovery = immediateRecovery;
        this.sequence = new AtomicInteger();
        this.txManager = TxManager.make[K](new MapData[K](masterMap), immediateRecovery);
    }   
    
    public def isReadOnlyTransaction(id:Long) {
        return txManager.isReadOnlyTransaction(id);
    }
    
    public def getTxCommitLog(id:Long) {
        return txManager.getTxCommitLog(id);
    }
    
    public def get(id:Long, key:K):Cloneable {
        return txManager.get(id, key);
    }
    
    public def put(id:Long, key:K, value:Cloneable):Cloneable {
        return txManager.put(id, key, value);
    }
    
    public def delete(id:Long, key:K):Cloneable {
        return txManager.delete(id, key, false);
    }
    
    public def deleteTxDesc(id:Long, key:K):Cloneable {
        return txManager.delete(id, key, true);
    }
    
    public def validate(id:Long) {
        txManager.validate(id);
    }
    
    public def commit(id:Long) {
        txManager.commit(id);
    }
    
    public def commit(log:TxLog[K]) {
        txManager.commit(log);
    }
    
    public def abort(id:Long) {
        txManager.abort(id);
    }
    
    public def keySet(id:Long) {
        return txManager.keySet(id);
    }
    
    public def tryLockWrite(id:Long, key:K) {
        return txManager.tryLockWrite(id, key);
    }

    public def tryLockRead(id:Long, key:K) {
        return txManager.tryLockRead(id, key);
    }
    
    public def unlockRead(id:Long, key:K) {
        txManager.unlockRead(id, key);
    }
    
    public def unlockWrite(id:Long, key:K) {
        txManager.unlockWrite(id, key);
    }

    public def getState() = txManager.data;
    
    public def getNextTransactionId() {
        val placeId = here.id as Int;
        val localTxId = sequence.incrementAndGet();
        val txId = ((placeId as Long) << 32) | localTxId as Long;
        return txId;
    }
    
    /*Lock based method*/
    public def lockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        txManager.lockAll(id, start, opPerPlace, keys, readFlags);
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        txManager.unlockAll(id, start, opPerPlace, keys, readFlags);
    }
    
    public def getLocked(id:Long, key:K):Cloneable {
        return txManager.get(id, key);
    }
    
    public def deleteLocked(id:Long, key:K):Cloneable {
        return txManager.delete(id, key, false);
    }
    
    public def putLocked(id:Long, key:K, value:Cloneable):Cloneable {
        return txManager.put(id, key, value);
    }
    
    public def filterCommitted(txList:ArrayList[Long]) {
    	if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " started MasterStore.filterCommitted ...");
        val list = new ArrayList[Long]();
        if (!TxConfig.get().LOCK_FREE)
            txManager.data.lock(-1);
        
        for (txId in txList) {
        	val obj = txManager.data.getTxDesc(txId);
            if (obj != null && (obj as TxDesc).status == TxDesc.COMMITTING ) {
                list.add(txId);
            }
        }
        
        if (!TxConfig.get().LOCK_FREE)
            txManager.data.unlock(-1);
        if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " completed MasterStore.filterCommitted ...");
        return list;
    }
    
    public def isActive() {
        return txManager.isActive();
    }
    
    public def pausing() {
        if (TxConfig.get().TMREC_DEBUG) Console.OUT.println("Recovering " + here + " MasterStore.pasuing started");
    	txManager.pausing();
    	if (TxConfig.get().TMREC_DEBUG) Console.OUT.println("Recovering " + here + " MasterStore.pasuing completed");
    }
    
    public def paused() {
        txManager.paused();
    }
    
    public def reactivate() {
        txManager.reactivate();
    }
    
    public def waitUntilPaused() {
    	txManager.waitUntilPaused();
    }
    
}
