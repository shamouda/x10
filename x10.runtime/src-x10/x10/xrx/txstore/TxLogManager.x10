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

import x10.xrx.txstore.TxConfig;
import x10.util.concurrent.Lock;
import x10.xrx.TxStoreConcurrencyLimitException;
import x10.util.HashMap;

public class TxLogManager[K] {K haszero} {
    
    public static val TXLOG_NEW = System.getenv("TXLOG_NEW") != null && System.getenv("TXLOG_NEW").equals("1");
    
    private val txLogs:Rail[TxLog[K]];

    private val lock:Lock;
    private var insertIndex:Long;

    private var lastTaken:Long = -1;
    private val map:HashMap[Long,TxLog[K]];
    
    public def this() {
        //pre-allocate transaction logs
        if (TxConfig.get().STM) {
            if (TXLOG_NEW) {
                txLogs = null;
                map = new HashMap[Long,TxLog[K]]();
            } else {
                txLogs = new Rail[TxLog[K]](TxConfig.MAX_CONCURRENT_TXS);
                for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                    txLogs(i) = new TxLog[K]();
                }
                map = null;
            }
        } else {
            txLogs = null;
            map = null;
        }
        lock = new Lock();
        insertIndex = 0;
    }
    
    public def searchTxLog(id:Long) {
        try {
            lock();
            if (TXLOG_NEW)
                return map.getOrElse(id, null);
            else {
                for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                    if (txLogs(i).id() == id) {
                        return txLogs(i);
                    }
                }
                return null;
            }
        } finally {
            unlock();
        }
    }
    
    public def getOrAddTxLog(id:Long) {
        try {
            lock();
            if (TXLOG_NEW) {
                var log:TxLog[K] = map.getOrElse(id, null);
                if (log == null) {
                    log = new TxLog[K]();
                    log.setId(id);
                    map.put(id,log);
                }
                return log;
            }
            else {
                for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                    if (txLogs(i).id() == id)
                        return txLogs(i);
                }
                var obj:TxLog[K] = null;
                for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                    if (txLogs(i).id() == -1) {
                        obj = txLogs(i);                  
                        break;
                    }
                }
                if (obj == null) {
                    //throw new TxStoreConcurrencyLimitException(here + " TxStoreConcurrencyLimitException");
                    Console.OUT.println(here + "FATAL ERROR: TxStoreConcurrencyLimitException");
                    System.killHere();
                }
                obj.setId(id); //allocate it
                return obj;                
            }
        }
        finally {
            unlock();
        }
    }
    
    public def deleteTxLog(log:TxLog[K]) {
        try {
            lock();
            if (TXLOG_NEW) {
                map.remove(log.id());
            }
            else {
                log.reset();
            }
        }
        finally {
            unlock();
        }
    }
    
    public def deleteAbortedTxLog(log:TxLog[K]) {
        deleteTxLog(log);
    }
    
    public def activeTransactionsExist() {
        try {
            lock();
            if (TXLOG_NEW) {
                for (e in map.entries()) {
                    if (e.getValue().writeValidated) {
                        return true;
                    }
                }
                return false;
            }
            else {
                for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                    if (txLogs(i).id() != -1 && txLogs(i).writeValidated) {
                        Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused  found a non-aborted transaction Tx["+txLogs(i).id()+"] " + TxManager.txIdToString (txLogs(i).id()));
                        return true;
                    }
                }
                return false;
            }
        }
        finally {
            unlock();
        }
    }
    
    
    public def lock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
    }
}