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

import x10.util.concurrent.AtomicInteger;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;

public class MemoryUnit {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    private var version:Int;
    private var value:Cloneable;
    private val lock:TxLock;

    public def this(v:Cloneable) {
        value = v;
        if (TxManager.TM_DISABLED) 
            lock = new TxLockExclusiveBlocking();
        else
            lock = new TxLockCREW();
    }
    
    public def getAtomicValue(copy:Boolean, key:String, txId:Long) {
        atomic {
            var v:Cloneable = value;
            if (copy) {
                v = value == null?null:value.clone();
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
            return new AtomicValue(version, v);
        }
    }
    
    public def setValue(v:Cloneable, key:String, txId:Long) {
        var oldValue:Cloneable;
        atomic {
            oldValue = value;
            version++;
            value = v;
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] setvv key["+key+"] ver["+version+"] val["+value+"]");
        }
        return oldValue;
    }
    
    public def rollbackValue(oldValue:Cloneable, oldVersion:Int, key:String, txId:Long) {
        atomic {
            version = oldVersion; 
            value = oldValue;
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] rollsetvv key["+key+"] ver["+version+"] val["+value+"]");
        }
    }
    
    public def lock(txId:Long, key:String) {
        lock.lock(txId, key);
    }
    
    public def unlock(txId:Long, key:String) {
        lock.unlock(txId, key);
    }
    
    public def lockRead(txId:Long, key:String) {
        lock.lockRead(txId, key);
    }
    
    public def unlockRead(txId:Long, key:String) {
        lock.unlockRead(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
        lock.lockWrite(txId, key);
    }
    
    public def unlockWrite(txId:Long, key:String) {
        lock.unlockWrite(txId, key);
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
    
}