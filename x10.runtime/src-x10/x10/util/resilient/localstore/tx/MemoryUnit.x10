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

import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.TxConfig;

public class MemoryUnit {
    
    private var version:Int;
    private var value:Cloneable;
    private val txLock:TxLock;

    private val internalLock:Lock;
    private var deleted:Boolean = false;
    
    public def this(v:Cloneable) {
        value = v;
        if (TxConfig.get().BASELINE) { //Baseline
            txLock = null;
            internalLock = null;
        }
        else if (TxConfig.get().LOCKING) { //Locking
            txLock = new TxLockCREWBlocking();
            internalLock = new Lock();
        }
        else { //STM
            txLock = new TxLockCREW();
            internalLock = new Lock();
        }
    }
    
    public def getValue(copy:Boolean, key:String, txId:Long) {
        try {
            lockExclusive(); //lock is used to ensure that value/version are always in sync as a composite value
            ensureNotDeleted(key);
            var v:Cloneable = value;
            if (copy) {
                v = value == null?null:value.clone();
            }
            return v;
        }
        finally {
            unlockExclusive();
        }
    }
    
    public def initializeTxKeyLog(key:String, locked:Boolean, log:TxKeyChange) {
        try {
            if (!locked)
                lockExclusive(); 
            ensureNotDeleted(key);
            val v = value == null?null:value.clone();
            log.initValueVersion(v, version);
        }
        finally {
            if (!locked)
                unlockExclusive();
        }
    }
    
    public def rollbackValueLocked(oldValue:Cloneable, oldVersion:Int, key:String, txId:Long) {
        version = oldVersion; 
        value = oldValue;
        deleted = false;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " rollsetvv key["+key+"] ver["+version+"] val["+value+"]");
    }
       
    public def lockRead(txId:Long, key:String) {
        if (!TxConfig.get().LOCK_FREE) {
            txLock.lockRead(txId, key);
            try {
                ensureNotDeleted(key);
            }
            catch(ex:Exception) {
                txLock.unlockRead(txId, key);
                throw ex;
            }
        }
    }
    
    public def unlockRead(txId:Long, key:String) {
        if (!TxConfig.get().LOCK_FREE)
            txLock.unlockRead(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
        if (!TxConfig.get().LOCK_FREE) {
            txLock.lockWrite(txId, key);
            try {
                ensureNotDeleted(key);
            }
            catch(ex:Exception) {
                txLock.unlockWrite(txId, key);
                throw ex;
            }
        }
    }
    
    public def unlockWrite(txId:Long, key:String) {
        if (!TxConfig.get().LOCK_FREE)
            txLock.unlockWrite(txId, key);
    }

    public def toString() {
        return "version:"+version+":value:"+value;
    }
    
    public def deleteLocked(txId:Long, key:String) {       
        deleted = true;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " deleteLocked key["+key+"] ");
    }
    
    public def ensureNotDeleted(key:String) {
        if (deleted)
            throw new ConflictException("ConflictException here["+here+"] accessing a deleted memory unit key["+key+"]", here);
    }
    
    public def isDeletedLocked() = deleted;
    
    /********  Lock based methods *********/
    public def getValueLocked(copy:Boolean, key:String, txId:Long) {
        ensureNotDeleted(key);
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " getvv key["+key+"] ver["+version+"] val["+v+"]");
        return v;
    }
    
    public def getVersionLocked(copy:Boolean, key:String, txId:Long) {
        ensureNotDeleted(key);
        return version;
    }
    
    public def setValueLocked(v:Cloneable, key:String, txId:Long, deleted:Boolean) {
        ensureNotDeleted(key);
        val oldValue = value;
        version++;
        value = v;
        this.deleted = deleted;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " setvv key["+key+"] ver["+version+"] val["+value+"] deleted["+deleted+"] ");
        return oldValue;
    }
    
    /**************************************/
    private def lockExclusive() {
        if (!TxConfig.get().LOCK_FREE)
            internalLock.lock();
    }
    
    private def unlockExclusive() {
        if (!TxConfig.get().LOCK_FREE)
            internalLock.unlock();
    }
    
    /********  Baseline methods *********/
    public def baselineGet() = value;
    
    public def isDeleted() = deleted;
}