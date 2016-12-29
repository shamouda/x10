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

/*
 * Only one thread will be accessing a TxLog at a certain place
 * X10-STM does not allow multiple threads accessing same Tx in the same place.
 * However, an abort request may occur concurrenctly with other requests, that is why we have a lock to prevent
 * interleaving between abort and other operations.
 **/
public class TxLog (id:Long, mapName:String) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public val transLog:HashMap[String,TxKeyChange];
    public var aborted:Boolean = false;
    private val lock = new Lock();
    
    public def this(id:Long, mapName:String) {
        property(id, mapName);
        transLog = new HashMap[String,TxKeyChange]();
    } 
    
    public def getChangeLog(key:String) {
        return transLog.getOrThrow(key);
    }
    
    // get currently logged value (throws an exception if value was not set before)
    public def getValue(key:String) {
        return transLog.getOrThrow(key).getValue();
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
    public def logInitialValue(key:String, copiedValue:Cloneable, version:Int, txId:Long) {
        var log:TxKeyChange = transLog.getOrElse(key, null);
        if (log == null) {
            log = new TxKeyChange(copiedValue, version, txId);
            transLog.put(key, log);
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] initial read ver["+version+"] val["+copiedValue+"]");
        }
    }
    
    public def logPut(key:String, copiedValue:Cloneable) {
        //assert ( TxManager.TM_RECOVER == TxManager.WRITE_BUFFERING );
        transLog.getOrThrow(key).update(copiedValue);
    }
    
    public def logDelete(key:String) {
        assert ( TxManager.TM_RECOVER == TxManager.WRITE_BUFFERING );
        transLog.getOrThrow(key).delete();
    }
    
    //*used by Undo Logging*//
    public def markAsModified(key:String) {
        transLog.getOrThrow(key).markAsModified();
    }
    
    public def markAsDeleted(key:String) {
        transLog.getOrThrow(key).markAsDeleted();
    }
    
    //*used by Undo Logging*//
    public def isModified(key:String) {
        return !transLog.getOrThrow(key).readOnly();
    }

    // mark as locked
    public def markAsLocked(key:String) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] markAsLocked");
        transLog.getOrThrow(key).markAsLocked();
    }
    
    public def isLocked(key:String) {
        val result = transLog.getOrThrow(key).isLocked();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] key["+key+"] isLocked?["+result+"]");
        return result;
    }
    
    /*Get log without readonly changes*/
    public def removeReadOnlyKeys():TxLog {
        val tmpLog = new TxLog(id, mapName);
        val iter = transLog.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val log = transLog.getOrThrow(key);
            if (!log.readOnly()) {
                tmpLog.transLog.put(key, log.clone());
            }
        }
        return tmpLog;
    }
    
    public def lock() {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] ["+here+"] LOG.lock() before");
        val startWhen = System.nanoTime();
        lock.lock();
        val endWhen = System.nanoTime();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] ["+here+"] LOG.lock() after  lockingtime [" +((endWhen-startWhen)/1e6) + "] ms");
    }
    public def unlock() {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] ["+here+"] LOG.unlock() before");
        lock.unlock();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] ["+here+"] LOG.unlock() after");
    }
}