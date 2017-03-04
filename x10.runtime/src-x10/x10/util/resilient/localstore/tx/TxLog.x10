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

/*
 * The log to track actions done on a key. 
 *  * In resilient mode, it is used to replicate the key changes on the slave.
 * 
 * Only one thread will be accessing a TxLog at a certain place
 * X10-STM does not allow multiple threads accessing same Tx in the same place.
 * However, an abort request may occur concurrenctly with other requests, that is why we have a lock to prevent
 * interleaving between abort and other operations.
 **/
public class TxLog (id:Long) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public val transLog:HashMap[String,TxKeyChange];
    public var aborted:Boolean = false;
    private val lock:Lock;
    
    
    public def this(id:Long) {
        property(id);
        transLog = new HashMap[String,TxKeyChange]();
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	lock = new Lock();
        else
        	lock = null;
    } 
    
    public def getChangeLog(key:String) {
        return transLog.getOrThrow(key);
    }
    
    // get currently logged value (throws an exception if value was not set before)
    public def getValue(copy:Boolean, key:String) {
        val value = transLog.getOrThrow(key).getValue();
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        return v;
    }
    
    // get version
    public def getInitVersion(key:String) {
        return transLog.getOrThrow(key).getInitVersion();
    }
    
    // get TxId
    public def getInitTxId(key:String) {
        return transLog.getOrThrow(key).getInitTxId();
    }
    
    /*MUST be called before logPut and logDelete*/
    public def logInitialValue(key:String, copiedValue:Cloneable, version:Int, txId:Long, lockedRead:Boolean) {
        var log:TxKeyChange = transLog.getOrElse(key, null);
        if (log == null) {
            log = new TxKeyChange(copiedValue, version, txId, lockedRead);
            transLog.put(key, log);
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] initial read ver["+version+"] val["+copiedValue+"]");
        }
    }
    
    public def logPut(key:String, copiedValue:Cloneable) {
        return transLog.getOrThrow(key).update(copiedValue);
    }
    
    public def setReadOnly(key:String, ro:Boolean) {
        transLog.getOrThrow(key).setReadOnly(ro);
    }
    
    //*used by Undo Logging*//
    public def getReadOnly(key:String) {
        return transLog.getOrThrow(key).getReadOnly();
    }

    // mark as locked for read
    public def setLockedRead(key:String, lr:Boolean) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] setLockedRead("+lr+") ");
        transLog.getOrThrow(key).setLockedRead(lr);
    }
    
    // mark as locked for write
    public def setLockedWrite(key:String, lw:Boolean) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] setLockedWrite("+lw+") ");
        transLog.getOrThrow(key).setLockedWrite(lw);
    }
    
    public def getLockedRead(key:String) {
        val result = transLog.getOrThrow(key).getLockedRead();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] getLockedRead?["+result+"]");
        return result;
    }
    
    public def getLockedWrite(key:String) {
        val result = transLog.getOrThrow(key).getLockedWrite();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] getLockedWrite?["+result+"]");
        return result;
    }
    
    /*Get log without readonly changes*/
    public def removeReadOnlyKeys():HashMap[String,Cloneable] {
        val map = new HashMap[String,Cloneable]();
        val iter = transLog.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val log = transLog.getOrThrow(key);
            if (!log.getReadOnly()) {
                val copy = log.getValue() == null ? null:log.getValue().clone();
                map.put(key, copy);
            }
        }
        return map;
    }
    
    public def lock() {
    	if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
    		lock.lock();
    }
    
    public def unlock() {
    	if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
    		lock.unlock();
    }
}