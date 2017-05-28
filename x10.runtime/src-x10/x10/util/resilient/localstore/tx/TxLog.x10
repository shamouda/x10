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
public class TxLog {
    private static class TxLogKeysList {
        private val rdKeys:GrowableRail[TxKeyChange];
        private val wtKeys:GrowableRail[TxKeyChange];
    
        public def this() {
            rdKeys = new GrowableRail[TxKeyChange](TxConfig.get().PREALLOC_TXKEYS);
            wtKeys = new GrowableRail[TxKeyChange](TxConfig.get().PREALLOC_TXKEYS);
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
        
        public def add(log:TxKeyChange) {
            rdKeys.add(log);
        }
        
        
        private def get(key:String, read:Boolean) {
            val rail = read ? rdKeys : wtKeys;
            for (var i:Long = 0; i < rail.size(); i++) {
                if (rail(i).key().equals(key))
                    return rail(i);
            }
            return null;
        }
    
        
        public def get(key:String) {
            val rdVal = get(key, true);
            if (rdVal != null)
                return rdVal;
            return get(key, false);
        }
        
        public def getOrThrow(key:String) {
            val obj = get(key);            
            if (obj == null)
                throw new Exception("Not found:" + key);
            return obj;
        }
    
        private def fromReadToWrite(key:String) {
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
            val log = rdKeys.removeLast();
            wtKeys.add(log);
            return log;
        }

        public def logPut(key:String, copiedValue:Cloneable) {
            var log:TxKeyChange = get(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            return log.update(copiedValue);
        }
    
        public def logDelete(key:String) {
            var log:TxKeyChange = get(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            return log.delete();
        }

        public def setAllWriteFlags(key:String, locked:Boolean, deleted:Boolean) {
            var log:TxKeyChange = get(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            log.setReadOnly(false);
            log.setLockedWrite(locked);
            log.setDeleted(deleted);
        }

        public def setLockedWrite(key:String) {
            var log:TxKeyChange = get(key, false); //get from write
            if (log == null)
                log = fromReadToWrite(key);
            log.setLockedWrite(true);
        }
        
    }

    private val keysList:TxLogKeysList;
    public var aborted:Boolean = false;
    public var writeValidated:Boolean = false;
    public var id:Long = -1;
    private var lock:Lock;
    
    public def this() {
        keysList = new TxLogKeysList();
        if (!TxConfig.get().LOCK_FREE)
            lock = new Lock();
        else
            lock = null;
    }
    
    public def reset() {
        id = -1;
        keysList.clear();
        aborted = false;
        writeValidated = false;
    }

    // get currently logged value (throws an exception if value was not set before)
    public def getValue(copy:Boolean, key:String) {
        val value = keysList.getOrThrow(key).getValue();
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        return v;
    }
    
    // get version
    public def getInitVersion(key:String) {
        return keysList.getOrThrow(key).getInitVersion();
    }
    
    // get TxId
    public def getInitTxId(key:String) {
        return keysList.getOrThrow(key).getInitTxId();
    }
       
    public def getMemoryUnit(key:String) {
        var log:TxKeyChange = keysList.get(key);
        if (log == null)
            return null;
        else
            return log.getMemoryUnit();
    }
    
    /*MUST be called before logPut and logDelete*/
    public def logInitialValue(key:String, txId:Long, lockedRead:Boolean, memU:MemoryUnit, added:Boolean) {
        var log:TxKeyChange = keysList.get(key);
        if (log == null) {
            log = new TxKeyChange(key, txId, lockedRead, memU, added);
            memU.initializeTxKeyLog(key, lockedRead, log);
            keysList.add(log);
        }
    }
    
    public def logPut(key:String, copiedValue:Cloneable) {
        return keysList.logPut(key, copiedValue);
    }
    
    public def logDelete(key:String) {
        return keysList.logDelete(key);
    }

    public def setAllWriteFlags(key:String, locked:Boolean, deleted:Boolean) {
        keysList.setAllWriteFlags(key, locked, deleted);
    }
    
    //*used by Undo Logging*//
    public def getReadOnly(key:String) {
        return keysList.getOrThrow(key).getReadOnly();
    }
    
    public def getDeleted(key:String) {
        return keysList.getOrThrow(key).getDeleted();
    }
    
    // mark as locked for read
    public def setLockedRead(key:String, lr:Boolean) {
        keysList.getOrThrow(key).setLockedRead(lr);
    }
    
    // mark as locked for write
    public def setLockedWrite(key:String, lr:Boolean) {
        keysList.setLockedWrite(key);
    }
    
    public def getLockedRead(key:String) {
        var result:Boolean = false;
        val log = keysList.get(key);
        if (log == null)
            result = false;
        else
            result = log.getLockedRead();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] key["+key+"] getLockedRead?["+result+"]");
        return result;
    }
    
    public def getLockedWrite(key:String) {
        var result:Boolean = false;
        val log = keysList.get(key);
        if (log == null)
            result = false;
        else
            result = log.getLockedWrite();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] key["+key+"] getLockedWrite?["+result+"]");
        return result;
    }
    
    public def isReadOnlyTransaction() {
        return keysList.isReadOnlyTransaction();
    }
    
    public def prepareCommitLog():HashMap[String,Cloneable] {
        val map = new HashMap[String,Cloneable]();
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
    public def lock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
    }
    
    public def getWriteKeys() {
        return keysList.getWriteKeys();
    }
    
    public def getReadKeys() {
        return keysList.getReadKeys();
    }

}