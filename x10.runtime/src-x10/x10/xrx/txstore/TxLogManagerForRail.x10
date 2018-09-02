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
import x10.util.GrowableRail;

public class TxLogManagerForRail[K] {K haszero} {
    public static val TXLOG_NEW = System.getenv("TXLOG_NEW") != null && System.getenv("TXLOG_NEW").equals("1");
    private val txLogs:GrowableRail[TxLogForRail[K]];
    private val lock:Lock;
    private var insertIndex:Long;
    private var lastTaken:Long = -1;
    private val map:HashMap[Long,TxLogForRail[K]];
    
    public def this() {
        if (TxConfig.STM) {
            if (TXLOG_NEW) {
                txLogs = null;
                map = new HashMap[Long,TxLogForRail[K]]();
            } else {
                txLogs = new GrowableRail[TxLogForRail[K]](TxConfig.MIN_CONCURRENT_TXS);
                for (var i:Long = 0 ; i < txLogs.capacity(); i++) {
                    txLogs.add(new TxLogForRail[K]());
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
                for (var i:Long = 0 ; i < txLogs.size(); i++) {
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
                var log:TxLogForRail[K] = map.getOrElse(id, null);
                if (log == null) {
                    log = new TxLogForRail[K]();
                    log.setId(id);
                    map.put(id,log);
                }
                return log;
            }
            else {
                for (var i:Long = 0 ; i < txLogs.size(); i++) {
                    if (txLogs(i).id() == id)
                        return txLogs(i);
                }
                var obj:TxLogForRail[K] = null;
                for (var i:Long = 0 ; i < txLogs.size(); i++) {
                    if (txLogs(i).id() == -1) {
                        obj = txLogs(i);
                        break;
                    }
                }
                if (obj == null) {
                    if (txLogs.size() < TxConfig.MAX_CONCURRENT_TXS) {
                        val oldSize = txLogs.capacity();
                        txLogs.grow(txLogs.capacity() + TxConfig.MIN_CONCURRENT_TXS);
                        for (var x:Long = oldSize; x < txLogs.capacity(); x++) {
                            txLogs.add(new TxLogForRail[K]());
                        }
                        obj = txLogs(oldSize);
                    } else {
                        Console.OUT.println(here + "FATAL ERROR: TxStoreConcurrencyLimitException");
                        System.killHere();
                    }
                }
                obj.setId(id); //allocate it
                return obj;                
            }
        } finally {
            unlock();
        }
    }
    
    public def deleteTxLog(log:TxLogForRail[K]) {
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
    
    public def deleteAbortedTxLog(log:TxLogForRail[K]) {
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
        if (!TxConfig.LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.LOCK_FREE)
            lock.unlock();
    }
}