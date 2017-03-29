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
import x10.compiler.Ifdef;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;

/* Slave methods are being called by only one place at a time,
 * either its master during normal execution, or the store coordinator during failure recovery */
public class SlaveStore {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    private var masterState:HashMap[String,Cloneable];
    // the order of the transactions in this list is important for recovery
    // TODO: change to a HashMap[place index, list]
    private var logs:ArrayList[TxSlaveLog]; 
    private transient val lock:Lock;
    public var master:Place;
    
    public def this(master:Place) {
        assert(resilient);
        masterState = new HashMap[String,Cloneable]();
        logs = new ArrayList[TxSlaveLog]();
        if (!TxConfig.get().LOCK_FREE)
            lock = new Lock();
        else
            lock = null;
        this.master = master;
    }
    
    /******* Recovery functions (called by one place) , no risk of race condition *******/
    public def addMasterPlace(state:HashMap[String,Cloneable]) {
        masterState = state;
        logs = new ArrayList[TxSlaveLog]();
    }
    
    public def getSlaveMasterState():HashMap[String,Cloneable] {
        return masterState;
    }
    
    /******* Prepare/Commit/Abort functions *******/
    /*Used by LocalTx to commit a transaction. TransLog is applied immediately and not saved in the logs map*/
    public def commit(id:Long, transLog:HashMap[String,Cloneable], ownerPlaceIndex:Long) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit1() started ...");
        try {
            slaveLock();
            commitLockAcquired(new TxSlaveLog(id, ownerPlaceIndex, transLog));
        }
        finally {
            slaveUnlock();
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit1() committed ...");
    }
    
    /*Used by Tx to commit a transaction that was previously prepared. TransLog is removed from the logs map after commit*/
    public def commit(id:Long) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit2() started ...");
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit2() committed ...");
    }
    
    private def commitLockAcquired(txLog:TxSlaveLog) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() started ...");
        try {
            val data = masterState;
            val iter = txLog.transLog.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = txLog.transLog.getOrThrow(key);
                data.put(key, value);
            }
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() completed ...");
        }catch(ex:Exception){
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() exception["+ex.getMessage()+"] ...");
            throw ex;
        }
    }
    
    public def prepare(id:Long, entries:HashMap[String,Cloneable], ownerPlaceIndex:Long) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() started ...");
        try {
            slaveLock();
            var txSlaveLog:TxSlaveLog = getLog(id);
            if (txSlaveLog == null) {
                txSlaveLog = new TxSlaveLog(id, ownerPlaceIndex);
                logs.add(txSlaveLog);
            }
            txSlaveLog.transLog = entries;
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() logs.put(id="+id+") ...");
        }
        finally {
            slaveUnlock();
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() completed ...");
    }
    
    public def abort(id:Long) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.abort() started ...");
        try {
            slaveLock();
            val log = getLog(id);
            logs.remove(log);
        }
        finally {
            slaveUnlock();
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.abort() completed ...");
    }
    
    public def filterCommitted(txList:ArrayList[Long]) {
    	if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " started SlaveStore.filterCommitted ...");
        val list = new ArrayList[Long]();
        for (txId in txList) {
            val obj = masterState.getOrElse("_TxDesc_"+"tx"+txId, null);
            if (obj != null && ((obj as TxDesc).status == TxDesc.COMMITTED || (obj as TxDesc).status == TxDesc.COMMITTING)) {
                list.add(txId);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " completed SlaveStore.filterCommitted ...");
        return list;
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
    	
    }
    
}