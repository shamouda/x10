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
    private var masterStateRail:Rail[K];
    
    private val storeType:Int;
    
    // the order of the transactions in this list is important for recovery
    // TODO: change to a HashMap[place index, list]
    private var logs:ArrayList[TxSlaveLog[K]]; 
    private transient val lock:Lock;
    
    public def this(storeType:Int, size:Long, init:(Long)=>K) {
        assert(resilient);
        this.storeType = storeType;
        if (storeType == TxLocalStore.KV_TYPE)
            masterState = new HashMap[K,Cloneable]();
        else 
            masterStateRail = new Rail[K](size, init);
        logs = new ArrayList[TxSlaveLog[K]]();
        if (!TxConfig.LOCK_FREE) {
            lock = new Lock();
        } else {
            lock = null;
        }
    }
    
    public def this(state:HashMap[K,Cloneable], rail:Rail[K]) {
        assert(resilient);
        if (state != null) {
            masterState = state;
            storeType = TxLocalStore.KV_TYPE;
        } else {
            masterStateRail = rail;            
            storeType = TxLocalStore.RAIL_TYPE;
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxSlaveStore created, received rail["+masterStateRail+"] ..."); 
        }
        logs = new ArrayList[TxSlaveLog[K]]();
        if (!TxConfig.LOCK_FREE) {
            lock = new Lock();
        } else {
            lock = null;
        }
    }
    
    /******* Recovery functions (called by one place) , no risk of race condition *******/
    public def addMasterPlace(state:HashMap[K,Cloneable], state2:Rail[K]) {
        masterState = state;
        masterStateRail = state2;
        logs = new ArrayList[TxSlaveLog[K]]();
    }
    
    public def getSlaveMasterState():HashMap[K,Cloneable] {
        return masterState;
    }
    
    public def getSlaveMasterStateRail():Rail[K] {
        return masterStateRail;
    }
    
    /******* Prepare/Commit/Abort functions *******/
    /*Used by LocalTx to commit a transaction. TransLog is applied immediately and not saved in the logs map*/
    public def commit(id:Long, transLog:HashMap[K,Cloneable], transLogRail:HashMap[Long,K], ownerPlaceIndex:Long) {
        try {
            slaveLock();
            commitLockAcquired(new TxSlaveLog[K](id, ownerPlaceIndex, transLog, transLogRail));
        }
        finally {
            slaveUnlock();
        }
    }
    
    /*Used by Tx to commit a transaction that was previously prepared. TransLog is removed from the logs map after commit*/
    public def commit(id:Long) {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] Slave.commit ...");
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
            if (storeType == TxLocalStore.KV_TYPE) {
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
            } else {
                val data = masterStateRail;
                val iter = txLog.transLogRail.keySet().iterator();
                while (iter.hasNext()) {
                    val index = iter.next();
                    val value = txLog.transLogRail.getOrThrow(index);
                    data(index) = value;
                }
            }
        }catch(ex:Exception){
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] " + TxConfig.txIdToString (txLog.id)+ " here["+here+"] Slave.commitLockAcquired failed ex["+ex.getMessage()+"], masterStateRail="+masterStateRail+", transLogRail="+txLog.transLogRail+"...");
            throw ex;
        }
    }
    
    public def prepare(id:Long, entries1:HashMap[K,Cloneable], entries2:HashMap[Long,K], ownerPlaceIndex:Long) {
        if (TxConfig.TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] Slave.prepare ...");
        try {
            slaveLock();
            var txSlaveLog:TxSlaveLog[K] = getLog(id);
            if (txSlaveLog == null) {
                txSlaveLog = new TxSlaveLog[K](id, ownerPlaceIndex);
                logs.add(txSlaveLog);
            }
            txSlaveLog.transLog = entries1;
            txSlaveLog.transLogRail = entries2;
        }
        finally {
            slaveUnlock();
        }
    }
    
    public def abort(id:Long) {
        if (TxConfig.TM_DEBUG) 
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
    
    private def getLog(id:Long) {
        for (log in logs){
            if (log.id == id)
                return log;
        }
        return null;
    }
    
    private def slaveLock(){
        if (!TxConfig.LOCK_FREE)
            lock.lock();
    }
    
    private def slaveUnlock(){
        if (!TxConfig.LOCK_FREE)
            lock.unlock();
    }
    
    
    public def waitUntilPaused() {
        try {
            slaveLock();
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " - TxSlaveStore.waitUntilPaused started logsSize["+logs.size() +"] ...");
            Runtime.increaseParallelism();
            var count:Long = 0;
            var printMsg:Boolean = false;
            while (logs.size() != 0) {
                slaveUnlock();
                TxConfig.waitSleep();
                slaveLock();
                if (count++ % 1000 == 0){
                    var str:String = "";
                    for (log in logs){
                        str += "log{" + log.toString() + "}  , " ;
                    }
                    Console.OUT.println(here + " maybe a bug, slave waited too long ..." + str);
                    printMsg = true;
                }
            }
            if (printMsg) Console.OUT.println(here + " NOT a bug, slave finished waiting ...");
        } finally {
        	if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " - TxSlaveStore.waitUntilPaused completed ...");
            Runtime.decreaseParallelism(1n);
            slaveUnlock();
        }
    }
}