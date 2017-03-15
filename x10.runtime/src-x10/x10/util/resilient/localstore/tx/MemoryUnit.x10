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
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    private var version:Int;
    private var value:Cloneable;
    private val txLock:TxLock;

    private val internalLock:Lock;
    
    public def this(v:Cloneable) {
        value = v;
        if (TxConfig.getInstance().BASELINE) { //Baseline
        	txLock = null;
            internalLock = null;
        }
        else if (TxConfig.getInstance().LOCKING) { //Locking
            txLock = new TxLockCREWBlocking();
            internalLock = new Lock();
        }
        else { //STM
            txLock = new TxLockCREW();
            internalLock = new Lock();
        }
    }
    
    public def getAtomicValue(copy:Boolean, key:String, txId:Long) {
        try {
        	lockExclusive(); //lock is used to ensure that value/version are always in sync as a composite value 
            var v:Cloneable = value;
            if (copy) {
                v = value == null?null:value.clone();
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
            return new AtomicValue(version, v);
        }
        finally {
        	unlockExclusive();
        }
    }
    
    public def getAtomicValueLocked(copy:Boolean, key:String, txId:Long) {
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
        return new AtomicValue(version, v);
    }
    
    public def rollbackValueLocked(oldValue:Cloneable, oldVersion:Int, key:String, txId:Long) {
        version = oldVersion; 
        value = oldValue;
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] rollsetvv key["+key+"] ver["+version+"] val["+value+"]");
    }
       
    public def lockRead(txId:Long, key:String) {
    	if (!TxConfig.getInstance().LOCK_FREE)
    		txLock.lockRead(txId, key);
    }
    
    public def unlockRead(txId:Long, key:String) {
    	if (!TxConfig.getInstance().LOCK_FREE)
    		txLock.unlockRead(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
    	if (!TxConfig.getInstance().LOCK_FREE)
    		txLock.lockWrite(txId, key);
    }
    
    public def unlockWrite(txId:Long, key:String) {
    	if (!TxConfig.getInstance().LOCK_FREE)
    		txLock.unlockWrite(txId, key);
    }

    public def toString() {
        return "version:"+version+":value:"+value;
    }
    
    /********  Lock based methods *********/
    public def getValueLocked(copy:Boolean, key:String, txId:Long) {
        var v:Cloneable = value;
        if (copy) {
            v = value == null?null:value.clone();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
        return v;
    }
    
    public def setValueLocked(v:Cloneable, key:String, txId:Long) {
        val oldValue = value;
        version++;
        value = v;
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] setvv key["+key+"] ver["+version+"] val["+value+"]");
        return oldValue;
    }
    
    /**************************************/
    private def lockExclusive() {
    	if (!TxConfig.getInstance().LOCK_FREE)
            internalLock.lock();
    }
    
    private def unlockExclusive() {
        if (!TxConfig.getInstance().LOCK_FREE)
            internalLock.unlock();
    }
    
    /********  Baseline methods *********/
    public def baselineGet() = value;
    
}