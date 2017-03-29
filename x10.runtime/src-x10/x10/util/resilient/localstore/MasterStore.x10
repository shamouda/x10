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

public class MasterStore {
    /*Each map has an object of TxManager (same object even after failures)*/
    private val txManager:TxManager;
    private val sequence:AtomicInteger;

    public static val TX_FACTOR=1000000;
    
    public def this(masterMap:HashMap[String,Cloneable]) {
        this.sequence = new AtomicInteger();
        this.txManager = TxManager.make(new MapData(masterMap));
    }   
    
    public def getTxCommitLog(id:Long) {
        return txManager.getTxCommitLog(id);
    }
    
    public def get(mapName:String, id:Long, key:String):Cloneable {
        return txManager.get(id, mapName+key);
    }
    
    public def put(mapName:String, id:Long, key:String, value:Cloneable):Cloneable {
        return txManager.put(id, mapName+key, value);
    }
    
    public def delete(mapName:String, id:Long, key:String):Cloneable {
        return txManager.delete(id, mapName+key);
    }
    
    public def validate(id:Long) {
        txManager.validate(id);
    }
    
    public def commit(id:Long) {
        txManager.commit(id);
    }
    
    public def commit(log:TxLog) {
        txManager.commit(log);
    }
    
    public def abort(id:Long) {
        txManager.abort(id);
    }
    
    public def keySet(mapName:String, id:Long) {
        return txManager.keySet(mapName, id);
    }
    
    public def getState() = txManager.data;
    
    public def getNextTransactionId() {
        val placeId = here.id as Int;
        val localTxId = sequence.incrementAndGet();
        val txId = ((placeId as Long) << 32) | localTxId as Long;
        return txId;
    }
    
    /*Lock based method*/
    public def lockRead(mapName:String, id:Long, key:String) {
         txManager.lockRead(id, mapName+key);
    }
    
    public def lockWrite(mapName:String, id:Long, key:String) {
        txManager.lockWrite(id, mapName+key);
    }
    
    public def unlockRead(mapName:String, id:Long, key:String) {
        txManager.unlockRead(id, mapName+key);
    }
    
    public def unlockWrite(mapName:String, id:Long, key:String) {
        txManager.unlockWrite(id, mapName+key);
    }
    
    public def getLocked(mapName:String, id:Long, key:String):Cloneable {
        return txManager.get(id, mapName+key);
    }
    
    public def deleteLocked(mapName:String, id:Long, key:String):Cloneable {
        return txManager.delete(id, mapName+key);
    }
    
    public def putLocked(mapName:String, id:Long, key:String, value:Cloneable):Cloneable {
        return txManager.put(id, mapName+key, value);
    }
    
    public def filterCommitted(txList:ArrayList[Long]) {
    	if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " started MasterStore.filterCommitted ...");
        val list = new ArrayList[Long]();
        val metadata = txManager.data.getMap();
        if (!TxConfig.get().LOCK_FREE)
            metadata.lockAll();
        
        for (txId in txList) {
            val obj = metadata.getOrThrowUnsafe("_TxDesc_"+"tx"+txId).getAtomicValueLocked(false, "_TxDesc_"+"tx"+txId, -1).value;
            if (obj != null && ( (obj as TxDesc).status == TxDesc.COMMITTED || (obj as TxDesc).status == TxDesc.COMMITTING) ) {
                list.add(txId);
            }
        }
        if (!TxConfig.get().LOCK_FREE)
            metadata.unlockAll();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " completed MasterStore.filterCommitted ...");
        return list;
    }
    
    public def isActive() = txManager.isActive();
    
    public def pause() {
    	txManager.pause();
    }
    
    public def waitUntilPaused() {
    	txManager.waitUntilPaused();
    }
    
}
