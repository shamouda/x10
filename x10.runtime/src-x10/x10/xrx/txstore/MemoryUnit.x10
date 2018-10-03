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

import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.TxStoreConflictException;

public class MemoryUnit[K] {K haszero} {
    
    private var version:Int;
    private var value:Cloneable;
    private val txLock:TxLock;

    private val internalLock:Lock;
    private var deleted:Boolean = false;
    
    public def this(v:Cloneable) {
        value = v;
        if (TxConfig.LOCKING && !TxConfig.LOCKING_COMPLEX) { //Locking
            txLock = new TxLockCREWBlocking();
            internalLock = new Lock();
        }
        else { //STM
            if (TxConfig.MUST_PROGRESS)
                txLock = new TxLockCREW();
            else
                txLock = new TxLockCREWSimple();
            internalLock = new Lock();
        }
    }
    
    public def getValue(copy:Boolean, key:K, txId:Long) {
        try {
            lockExclusive(); //lock is used to ensure that value/version are always in sync as a composite value
            ensureNotDeleted();
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
    
    public def initializeTxKeyLog(key:K, txId:Long, locked:Boolean, added:Boolean, keyLog:TxKeyChange[K]) {
        try {
            if (!locked)
                lockExclusive(); 
            ensureNotDeleted();
            val v = value == null?null:value.clone();
            keyLog.init(key, locked, this, added, v, version);
        }
        finally {
            if (!locked)
                unlockExclusive();
        }
    }
    
    public def setInitialVersion(v:Int) {
        version = v;
    }
    
    public def rollbackValueLocked(oldValue:Cloneable, oldVersion:Int, key:K, txId:Long) {
        version = oldVersion; 
        value = oldValue;
        deleted = false;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " here["+here+"] rollsetvv key["+key+"] ver["+version+"] val["+value+"]");
    }
       
    public def lockRead(txId:Long) {
        if (!TxConfig.LOCK_FREE) {
            txLock.lockRead(txId);
            try {
                ensureNotDeleted();
            }
            catch(ex:Exception) {
                txLock.unlockRead(txId);
                throw ex;
            }
        }
    }
    
    public def tryLockRead(txId:Long):Boolean {
        if (TxConfig.LOCK_FREE) {
            return true;
        }
        val s  = txLock.tryLockRead(txId);
        if (s) {
            try {
                ensureNotDeleted();
            } catch(ex:Exception) {
                txLock.unlockRead(txId);
                throw ex;
            }
        }
        return s;
    }
    
    public def tryLockWrite(txId:Long):Boolean {
        if (TxConfig.LOCK_FREE) {
            return true;
        }
        val s  = txLock.tryLockWrite(txId);
        if (s) {
            try {
                ensureNotDeleted();
            } catch(ex:Exception) {
                txLock.unlockWrite(txId);
                throw ex;
            }
        }
        return s;
    }
    
    public def unlockRead(txId:Long) {
        if (!TxConfig.LOCK_FREE)
            txLock.unlockRead(txId);
    }
    
    public def lockWrite(txId:Long) {
        if (!TxConfig.LOCK_FREE) {
            txLock.lockWrite(txId);
            try {
                ensureNotDeleted();
            }
            catch(ex:Exception) {
                txLock.unlockWrite(txId);
                throw ex;
            }
        }
    }
    
    public def unlockWrite(txId:Long) {
        if (!TxConfig.LOCK_FREE)
            txLock.unlockWrite(txId);
    }

    public def toString() {
        return "version:"+version+":value:"+value;
    }
    
    public def deleteLocked() {       
        deleted = true;
    }
    
    public def ensureNotDeleted() {
        if (deleted)
            throw new TxStoreConflictException("ConflictException here["+here+"] accessing a deleted memory unit ", here);
    }
    
    public def isDeletedLocked() = deleted;
    
    /********  Lock based methods *********/
    public def getValueLocked(copy:Boolean, key:K, txId:Long) {
        return getValueLocked(copy, key, txId, true);
    }
    
    public def getValueLockedNoDebug(copy:Boolean, key:K, txId:Long) {
        return getValueLocked(copy, key, txId, false);
    }
    
    private def getValueLocked(copy:Boolean, key:K, txId:Long, print:Boolean) {
        ensureNotDeleted();
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        if (TxConfig.TM_DEBUG && print) 
            Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " here["+here+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
        return v;
    }
    
    public def getVersionLocked() {
        return version;
    }
    
    public def getVersionLocked(copy:Boolean, key:K, txId:Long) {
        ensureNotDeleted();
        return version;
    }
    
    public def setValueLocked(v:Cloneable, key:K, txId:Long, deleted:Boolean) {
        ensureNotDeleted();
        val oldValue = value;
        version++;
        value = v;
        this.deleted = deleted;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " here["+here+"] setvv key["+key+"] ver["+version+"] val["+value+"] deleted["+deleted+"] ");
        return oldValue;
    }
    
    /**************************************/
    private def lockExclusive() {
        if (!TxConfig.LOCK_FREE)
            internalLock.lock();
    }
    
    private def unlockExclusive() {
        if (!TxConfig.LOCK_FREE)
            internalLock.unlock();
    }
    
    /********  Baseline methods *********/
    public def baselineGet() = value;
    
    public def isDeleted() = deleted;
}