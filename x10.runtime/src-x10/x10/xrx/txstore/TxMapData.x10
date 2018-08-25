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
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreFatalException;

/*
 * TxMapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class TxMapData[K] {K haszero} {
    val metadata:HashMap[K,MemoryUnit[K]];
    val lock:Lock;

    //var print:Boolean = false;
    public def this() {
        metadata = new HashMap[K,MemoryUnit[K]]();
        lock = new Lock();
    }
    
    public def this(values:HashMap[K,Cloneable]) {
        metadata = new HashMap[K,MemoryUnit[K]]();
        lock = new Lock();
        
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.put(k, new MemoryUnit[K](v));
        }
    }
    
    public def getMap() = metadata;
    
    public def getKeyValueMap() {
        try {
            lock(-1);
            val values = new HashMap[K,Cloneable]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val k = iter.next();
                val memU = metadata.getOrThrow(k);
                if (!memU.isDeleted()) {
                    val v = metadata.getOrThrow(k).getValueLockedNoDebug(false, k, -1);
                    values.put(k, v);
                }
            }
            return values;
            
        }finally {
            unlock(-1);
        }
    }
    
    public def getMemoryUnit(k:K):MemoryUnit[K] {
        var res:MemoryUnit[K] = null;
        try {
            lock(-1);
            res = metadata.getOrElse(k, null);
            if (res == null) {
                res = new MemoryUnit[K](null);
                metadata.put(k, res);
            }
            res.ensureNotDeleted();
            return res;
        }finally {
            unlock(-1);
        }
    }
    
    public def initLog(key:K, active:Boolean, log:TxLog[K], keyLog:TxKeyChange[K], lockRead:Boolean):MemoryUnit[K] {        
        val txId = log.id();
        //if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TxMapData.initLog(" + key + ")");
        
        if (txId < 0) 
            throw new TxStoreFatalException(here + " fatal error, TxMapData.initLog txId = -1");
        
        var memory:MemoryUnit[K] = null;
        var added:Boolean  = false;
        try {
            lock(txId);
            memory = metadata.getOrElse(key, null);
            if (memory == null) {
                if (!active)
                    throw new TxStorePausedException(here + " TxMapData can not put values while the store is paused ");
                memory = new MemoryUnit[K](null);
                metadata.put(key, memory);
                //if (print)
                //    Console.OUT.println(here + " TxMapData.put ("+k+")");
                added = true;
                val size = metadata.size(); 
                if (size %10000 == 0) {
                    Console.OUT.println(here + " TxMapData.size = " + size);
                    //print = true;
                }
            }
        }finally {
            unlock(txId);
        }
        
        memory.ensureNotDeleted();
        if (lockRead) {
            memory.lockRead(log.id());
        }
        
        memory.initializeTxKeyLog(key, log.id(), lockRead, added, keyLog);
        
        return memory;
    }
    
    
    public def deleteMemoryUnit(txId:Long, key:K):void {
        try {
            lock(txId);
            metadata.delete(key);
            //if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TxMapData.delete(" + key + ")");
        }finally {
            unlock(txId);
        }
    }
    
    public def keySet() {
        try {
            lock(-1);
            val set = new HashSet[K]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                set.add(key);
            }
            return set;
        }finally {
            unlock(-1);
        }
    }
    
    public def lock(txId:Long){
        if (!TxConfig.LOCK_FREE) {
            lock.lock();
        }
    }
    
    public def unlock(txId:Long) {
        if (!TxConfig.LOCK_FREE) {
            lock.unlock();
        }
    }
    
    public def toString() {
        try {
            lock(-1);
            var str:String = here+"--->\n";
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = metadata.getOrThrow(key);
                str += "Key["+key+"] Value["+value.toString()+"]\n";
            }
            return str;
        }finally {
            unlock(-1);
        }
    }
    
    public def baselineGetValue(key:K):Cloneable {
        val memU = metadata.getOrElse(key, null);
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }
    
    public def baselinePutValue(key:K, value:Cloneable):Cloneable {
        var memU:MemoryUnit[K] = metadata.getOrElse(key, null);
        if (memU == null) {
            memU = new MemoryUnit[K](value);
            metadata.put(key, memU);
        }
        
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }

}