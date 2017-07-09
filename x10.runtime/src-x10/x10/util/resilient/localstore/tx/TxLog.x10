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

package x10.util.resilient.localstore.tx;

import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.TxConfig;
import x10.util.RailUtils;
import x10.util.GrowableRail;

/*
 * The log to track actions done on a key. 
 *  * In resilient mode, it is used to replicate the key changes on the slave.
 * 
 * Only one thread will be accessing a TxLog at a certain place
 * X10-STM does not allow multiple threads accessing same Tx in the same place.
 * However, an abort request may occur concurrenctly with other requests, that is why we have a lock to prevent
 * interleaving between abort and other operations.
 **/
public class TxLog[K] {K haszero} {
    private static class TxLogKeysList[K] {K haszero} {
        private val rdKeys:GrowableRail[TxKeyChange[K]];
        private val wtKeys:GrowableRail[TxKeyChange[K]];
    
        public def this() {
            rdKeys = new GrowableRail[TxKeyChange[K]](TxConfig.get().PREALLOC_TXKEYS);
            wtKeys = new GrowableRail[TxKeyChange[K]](TxConfig.get().PREALLOC_TXKEYS);
        }
    
        public def clear() {
            rdKeys.clear();
            wtKeys.clear();
        }
        
        public def isReadOnlyTransaction() {
            return wtKeys.size() == 0;
        }
        
        public def getWriteKeys()  {
            return wtKeys;
        }
        
        public def getReadKeys()  {
            return rdKeys;
        }
        
        public def add(log:TxKeyChange[K]) {
            rdKeys.add(log);
            //Console.OUT.println("TxLogKeysList adding key  { " + log + " } ");
            log.setIndx(rdKeys.size()-1);
        }
        
        private def search(key:K, read:Boolean) {
            val rail = read ? rdKeys : wtKeys;
            for (var i:Long = 0; i < rail.size(); i++) {
                if (rail(i).key().equals(key))
                    return rail(i);
            }
            return null;
        }
        
        public def get(key:K) {
            val rdVal = search(key, true);
            if (rdVal != null)
                return rdVal;
            return search(key, false);
        }
        
        public def getOrThrow(key:K) {
            val obj = get(key);            
            if (obj == null)
                throw new Exception("Not found:" + key);
            return obj;
        }
    
        private def fromReadToWrite(key:K) {
            val last = rdKeys.size() -1;
            var indx:Long = -1;
            for (indx = 0 ; indx < rdKeys.size(); indx++) {
                if (rdKeys(indx).key().equals(key))
                    break;
            }
            assert (indx != -1) : "fatal txkeychange not found";
            //swap with last
            val tmp = rdKeys(indx);
            rdKeys(indx) = rdKeys(last);
            rdKeys(last) = tmp;
            
            rdKeys(indx).setIndx(indx);
            rdKeys(last).setIndx(last);
            
            val log = rdKeys.removeLast();
            wtKeys.add(log);
            log.setIndx(wtKeys.size()-1);
            return log;
        }
        
        private def fromReadToWriteByIndex(indx:Long) {
            val last = rdKeys.size() -1;
            assert (indx != -1) : "fatal txkeychange not found";
            //swap with last
            val tmp = rdKeys(indx);
            rdKeys(indx) = rdKeys(last);
            rdKeys(last) = tmp;
            rdKeys(indx).setIndx(indx);
            rdKeys(last).setIndx(last);
            
            val log = rdKeys.removeLast();
            wtKeys.add(log);
            log.setIndx(wtKeys.size()-1);
        }

        public def logPut(key:K, copiedValue:Cloneable) {
            var log:TxKeyChange[K] = search(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            return log.update(copiedValue);
        }
        
        public def logPut(log:TxKeyChange[K], copiedValue:Cloneable) {
            if (log.getReadOnly())
                fromReadToWriteByIndex(log.indx());
            return log.update(copiedValue);
        }
        
        public def logDelete(key:K) {
            var log:TxKeyChange[K] = search(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            return log.delete();
        }

        public def logDelete(log:TxKeyChange[K]) {
            if (log.getReadOnly())
                fromReadToWriteByIndex(log.indx());
            return log.delete();
        }
        
        public def setAllWriteFlags(log:TxKeyChange[K], locked:Boolean, deleted:Boolean) {
            if (log.getReadOnly())
                fromReadToWriteByIndex(log.indx());
            log.setReadOnly(false);
            log.setLockedWrite(locked);
            log.setDeleted(deleted);
        }
        
        public def writeKeysAsString() {
            var str:String = "";
            for (var i:Long = 0 ; i < wtKeys.size(); i++) {
                str += wtKeys(i).key() + " "; 
            }
            return str;
        }
        
        public def readKeysAsString() {
            var str:String = "";
            for (var i:Long = 0 ; i < rdKeys.size(); i++) {
                str += rdKeys(i).key() + " "; 
            }
            return str;
        }
    }

    private val keysList:TxLogKeysList[K];
    public var aborted:Boolean = false;
    public var writeValidated:Boolean = false;
    private var id:Long = -1;
    private var lock:Lock;
    
    public def id() = id;
    public def setId(i:Long) {
    	if (i < 0)
    		throw new FatalTransactionException(here + " fatal error, TxLog.setId(-1)");
    	
    	id = i;
    }
    //used to reduce searching for memory units after calling TxManager.logInitialIfNotLogged
    private var lastUsedMemoryUnit:MemoryUnit[K];
    private var lastUsedKeyLog:TxKeyChange[K];
    
    public def this() {
        keysList = new TxLogKeysList[K]();
        if (!TxConfig.get().LOCK_FREE)
            lock = new Lock();
        else
            lock = null;
    }
    
    public def getOrAddKeyLog(key:K) { 
        var log:TxKeyChange[K] = keysList.get(key);
        if (log == null) {
            log = new TxKeyChange[K]();
            keysList.add(log);
        }
        return log;
    }
    
    public def reset() {
        val tmp = id;
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] TxLog.reset ");
        id = -1;
        keysList.clear();
        aborted = false;
        writeValidated = false;
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tmp+"] " + TxManager.txIdToString(tmp) + " here["+here+"] TxLog.reset done ");
    }

    public def getValue(copy:Boolean, key:K) {
        val value = keysList.getOrThrow(key).getValue();
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        return v;
    }
    
    public def getValue(copy:Boolean, keyLog:TxKeyChange[K]) {
        val value = keyLog.getValue();
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        return v;
    }
    
    public def getLastUsedKeyLog() = lastUsedKeyLog;
    public def setLastUsedKeyLog(kLog:TxKeyChange[K]) {
        lastUsedKeyLog = kLog;
    }
    
    // get version
    public def getInitVersion(key:K) {
        return keysList.getOrThrow(key).getInitVersion();
    }
    
    // get TxId
    public def getInitTxId(key:K) {
        return keysList.getOrThrow(key).getInitTxId();
    }
       
    public def getMemoryUnit(key:K) {
        var log:TxKeyChange[K] = keysList.get(key);
        if (log == null)
            return null;
        else
            return log.getMemoryUnit();
    }

    public def logPut(keyLog:TxKeyChange[K], copiedValue:Cloneable) {
        return keysList.logPut(keyLog, copiedValue);
    }
    
    public def logDelete(keyLog:TxKeyChange[K]) {
        return keysList.logDelete(keyLog);
    }
    
    public def setAllWriteFlags(keyLog:TxKeyChange[K], locked:Boolean, deleted:Boolean) {
        keysList.setAllWriteFlags(keyLog, locked, deleted);
    }
    
    //*used by Undo Logging*//
    public def getReadOnly(key:K) {
        return keysList.getOrThrow(key).getReadOnly();
    }
    
    public def getDeleted(key:K) {
        return keysList.getOrThrow(key).getDeleted();
    }
    
    // mark as locked for read
    public def setLockedRead(key:K, lr:Boolean) {
        keysList.getOrThrow(key).setLockedRead(lr);
    }
    
    public def isReadOnlyTransaction() {
        return keysList.isReadOnlyTransaction();
    }
    
    public def prepareCommitLog():HashMap[K,Cloneable] {
        //Console.OUT.println("prepareCommitLog readKeys {" + keysList.readKeysAsString() + "}  writeKeys {" + keysList.writeKeysAsString() + "} ");
        
        val map = new HashMap[K,Cloneable]();
        val wtKeys = keysList.getWriteKeys();
        if (TxConfig.get().WRITE_BUFFERING) {
            for (var i:Long = 0 ; i < wtKeys.size(); i++) {
                val log = wtKeys(i);
                map.put( log.key() , log.getValue());   /*SS_CHECK  I don't clone the objects here, why I did so in the past???**/
            }
        }
        else {
            for (var i:Long = 0 ; i < wtKeys.size(); i++) {
                val log = wtKeys(i);
                val key = log.key();
                val memory = log.getMemoryUnit();
                if (memory == null) {
                    Console.OUT.println("TxLog fatal bug, key["+key+"] has null memory unit");
                    throw new FatalTransactionException("TxLog fatal bug, key["+key+"] has null memory unit");
                }
                
                if (memory.isDeletedLocked()) {
                    map.put(key, null);
                }
                else {
                    map.put(key, memory.getValueLocked(false, key, id));   /*SS_CHECK  I don't clone the objects here, why I did so in the past???**/
                }
            
            }
        }
        return map;
    }
    
    public def lock(i:Long) {
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] ["+lock+"] <<LOCK>> "+i+"...");
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] ["+lock+"] <<LOCK_DONE>> "+i+"...");
    }
    
    public def unlock(i:Long) {
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] ["+lock+"] <<UNLOCK>> "+i+"...");
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
        lastUsedMemoryUnit = null;
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] ["+lock+"] <<UNLOCK_DONE>> "+i+"...");
    }
    
    public def unlock(i:Long, tmpId:Long) {
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tmpId+"] " + TxManager.txIdToString(tmpId) + " here["+here+"] ["+lock+"] <<UNLOCK>> "+i+"...");
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
        lastUsedMemoryUnit = null;
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tmpId+"] " + TxManager.txIdToString(tmpId) + " here["+here+"] ["+lock+"] <<UNLOCK_DONE>> "+i+"...");
    }
    
    public def getWriteKeys() {
        return keysList.getWriteKeys();
    }
    
    public def getReadKeys() {
        return keysList.getReadKeys();
    }

    public def keysToString() {
    	var rd:String = "";
        val rList = keysList.getReadKeys();
    	for (var indx:Long = 0 ; indx < rList.size(); indx++) {
    		rd += rList(indx).key() + ", " ;
    	}
        
        var wt:String = "";
        val wList = keysList.getWriteKeys();
    	for (var indx:Long = 0 ; indx < wList.size(); indx++) {
    		wt += wList(indx).key() + ", " ;
    	}
        return " Read{"+rd+"} Write{"+wt+"}";
    }
}