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

package x10.xrx.txstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.compiler.Ifdef;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.Runtime;

/* Slave methods are being called by only one place at a time,
 * either its master during normal execution, or the store coordinator during failure recovery */
public class TxSlaveStore[K] {K haszero} {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    private var masterState:HashMap[K,Cloneable];
    // the order of the transactions in this list is important for recovery
    // TODO: change to a HashMap[place index, list]
    private var logs:ArrayList[TxSlaveLog[K]]; 
    private transient val lock:Lock;
    
    public def this() {
        assert(resilient);
        masterState = new HashMap[K,Cloneable]();
        logs = new ArrayList[TxSlaveLog[K]]();
        if (!TxConfig.get().LOCK_FREE) {
            lock = new Lock();
        } else {
            lock = null;
        }
    }
    
    public def this(state:HashMap[K,Cloneable]) {
        assert(resilient);
        masterState = state;
        logs = new ArrayList[TxSlaveLog[K]]();
        if (!TxConfig.get().LOCK_FREE) {
            lock = new Lock();
        } else {
            lock = null;
        }
    }
    
    /******* Recovery functions (called by one place) , no risk of race condition *******/
    public def addMasterPlace(state:HashMap[K,Cloneable]) {
        masterState = state;
        logs = new ArrayList[TxSlaveLog[K]]();
    }
    
    public def getSlaveMasterState():HashMap[K,Cloneable] {
        return masterState;
    }
    /******* Prepare/Commit/Abort functions *******/
    /*Used by LocalTx to commit a transaction. TransLog is applied immediately and not saved in the logs map*/
    public def commit(id:Long, transLog:HashMap[K,Cloneable], ownerPlaceIndex:Long) {
        try {
            slaveLock();
            commitLockAcquired(new TxSlaveLog[K](id, ownerPlaceIndex, transLog));
        }
        finally {
            slaveUnlock();
        }
    }
    
    /*Used by Tx to commit a transaction that was previously prepared. TransLog is removed from the logs map after commit*/
    public def commit(id:Long) {
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] Slave.commit ...");
        try {
            slaveLock();
            val txLog = getLog(id);
            if (txLog != null) {
                commitLockAcquired(txLog);
                logs.remove(txLog);
            }
        } finally {
            slaveUnlock();
        }
    }
    
    private def commitLockAcquired(txLog:TxSlaveLog[K]) {
        try {
            val data = masterState;
            val iter = txLog.transLog.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = txLog.transLog.getOrThrow(key);
                if (value == null)
                    data.delete(key);
                else
                    data.put(key, value);
            }
        }catch(ex:Exception){
            throw ex;
        }
    }
    
    public def prepare(id:Long, entries:HashMap[K,Cloneable], ownerPlaceIndex:Long) {
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] Slave.prepare ...");
        try {
            slaveLock();
            var txSlaveLog:TxSlaveLog[K] = getLog(id);
            if (txSlaveLog == null) {
                txSlaveLog = new TxSlaveLog[K](id, ownerPlaceIndex);
                logs.add(txSlaveLog);
            }
            txSlaveLog.transLog = entries;
        }
        finally {
            slaveUnlock();
        }
    }
    
    public def isPrepared(id:Long) {
        try {
            slaveLock();
            if (getLog(id) == null)
                return false;
            else return true;
        } finally {
            slaveUnlock();
        }
    }
    
    public def abort(id:Long) {
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] Slave.abort ...");
        try {
            slaveLock();
            val log = getLog(id);
            logs.remove(log);
        }
        finally {
            slaveUnlock();
        }
    }
    
    public def clusterTransactions() {
        val map = new HashMap[Long,ArrayList[Long]]();
        try {
            slaveLock();
            for (log in logs) {
                var list:ArrayList[Long] = map.getOrElse(log.ownerPlaceIndex, null);
                if (list == null){
                    list = new ArrayList[Long]();
                    map.put(log.ownerPlaceIndex, list);
                }
                list.add(log.id);
            }
        } 
        finally {
            slaveUnlock();
        }
        return map;
    }
    
    public def getPendingTransactions() {
        val list = new ArrayList[Long]();
        try {
            slaveLock();
            for (log in logs){
                list.add(log.id);
            }
        } 
        finally {
            slaveUnlock();
        }
        return list;
    }
    
    public def commitAll(committed:ArrayList[Long]) {
        try {
            slaveLock();
            for (log in logs){
                if (committed.contains(log.id))
                    commitLockAcquired(log);
            }
            logs.clear();
        }
        finally {
            slaveUnlock();
        }
    }
    
    private def getLog(id:Long) {
        for (log in logs){
            if (log.id == id)
                return log;
        }
        return null;
    }
    
    private def slaveLock(){
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
    }
    
    private def slaveUnlock(){
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
    }
    
    
    public def waitUntilPaused() {
        try {
            slaveLock();
            if (TxConfig.get().TMREC_DEBUG) Console.OUT.println("Recovering " + here + " - TxSlaveStore.waitUntilPaused started logsSize["+logs.size() +"] ...");
            Runtime.increaseParallelism();
            var count:Long = 0;
            while (logs.size() != 0) {
                slaveUnlock();
                TxConfig.waitSleep();
                slaveLock();
                if (count++ % 1000 == 0){
                    var str:String = "";
                    for (log in logs){
                        str += "log{" + log.toString() + "}  , " ;
                    }
                    Console.OUT.println(here + " maybe a bug, waited too long ..." + str);
                }
            }
        
        } finally {
        	if (TxConfig.get().TMREC_DEBUG) Console.OUT.println("Recovering " + here + " - TxSlaveStore.waitUntilPaused completed ...");
            Runtime.decreaseParallelism(1n);
            slaveUnlock();
        }
    }
}