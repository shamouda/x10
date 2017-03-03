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
    static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    private var masterState:HashMap[String,Cloneable];
    // the order of the transactions in this list is important for recovery
    // TODO: change to a HashMap[place index, list]
    private var logs:ArrayList[TxSlaveLog]; 
    private transient val lock:Lock = new Lock(); 
    
    public def this() {
        assert(resilient);
        masterState = new HashMap[String,Cloneable]();
        logs = new ArrayList[TxSlaveLog]();
    }
    
    /******* Recovery functions (called by one place) , no risk of race condition *******/
    public def addMasterPlace(masterVirtualId:Long, state:HashMap[String,Cloneable]) {
        masterState = state;
        logs = new ArrayList[TxSlaveLog]();
    }
    
    public def getSlaveMasterState():HashMap[String,Cloneable] {
        return masterState;
    }
    
    /******* Prepare/Commit/Abort functions *******/
    public def commit(id:Long, transLog:HashMap[String,Cloneable], placeIndex:Long) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit1() started ...");
        try {
            lock.lock();
            commitLockAcquired(new TxSlaveLog(id, transLog, placeIndex));
        }
        finally {
            lock.unlock();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit1() committed ...");
    }
    
    public def commit(id:Long) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit2() started ...");
        try {
            lock.lock();
            val txLog = getLog(id);
            if (txLog != null) {
                commitLockAcquired(txLog);
                logs.remove(txLog);
            }
        } finally {
            lock.unlock();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.commit2() committed ...");
    }
    
    private def commitLockAcquired(txLog:TxSlaveLog) {
    	if (TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() started ...");
        try {
            val data = masterState;
            val iter = txLog.transLog.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = txLog.transLog.getOrThrow(key);
                data.put(key, value);
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() completed ...");
        }catch(ex:Exception){
            if (TM_DEBUG) Console.OUT.println("Tx["+txLog.id+"] here["+here+"] SlaveStore.commitLockAcquired() exception["+ex.getMessage()+"] ...");
            throw ex;
        }
    }
    
    public def prepare(id:Long, entries:HashMap[String,Cloneable], placeIndex:Long) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() started ...");
        try {
            lock.lock();
            var txSlaveLog:TxSlaveLog = getLog(id);
            if (txSlaveLog == null) {
                txSlaveLog = new TxSlaveLog( id, new HashMap[String,Cloneable](), placeIndex);
                logs.add(txSlaveLog);
            }
            val iter = entries.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = entries.getOrThrow(key);
                txSlaveLog.transLog.put(key, value);
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() logs.put(id="+id+") ...");
        }
        finally {
            lock.unlock();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.prepare() completed ...");
    }
    
    public def abort(id:Long) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.abort() started ...");
        try {
            lock.lock();
            val log = getLog(id);
            logs.remove(log);
        }
        finally {
            lock.unlock();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] SlaveStore.abort() completed ...");
    }
    
    public def filterCommitted(txList:ArrayList[Long]) {
    	val list = new ArrayList[Long]();
    	for (txId in txList) {
    		val obj = masterState.getOrElse("_TxDesc_"+"tx"+txId, null);
    		if (obj != null && ((obj as TxDesc).status == TxDesc.COMMITTED || (obj as TxDesc).status == TxDesc.COMMITTING)) {
    			list.add(txId);
    		}
    	}
    	return list;
    }
    
    public def clusterTransactions() {
    	val map = new HashMap[Long,ArrayList[Long]]();
    	try {
    		lock.lock();
    		for (log in logs) {
    			var list:ArrayList[Long] = map.getOrElse(log.placeIndex, null);
    			if (list == null){
    				list = new ArrayList[Long]();
    				map.put(log.placeIndex, list);
    			}
    			list.add(log.id);
    		}
    	} 
    	finally {
    		lock.unlock();
        }
    	return map;
    }
    
    public def getPendingTransactions() {
    	val list = new ArrayList[Long]();
    	try {
    		lock.lock();
    		for (log in logs){
    			list.add(log.id);
    		}
    	} 
    	finally {
    		lock.unlock();
        }
    	return list;
    }
    
    public def commitAll(committed:ArrayList[Long]) {
    	try {
    		lock.lock();

    		for (log in logs){
    			if (committed.contains(log.id))
    				commitLockAcquired(log);
    		}
    		logs.clear();
    	}
    	finally {
    		lock.unlock();
    	}
    }
    
    private def getLog(id:Long) {
    	for (log in logs){
    		if (log.id == id)
    			return log;
    	}
    	return null;
    }
}