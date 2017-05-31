package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.SafeBucketHashMap;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.tx.TxLog;
import x10.util.resilient.localstore.tx.TxKeyChange;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData {
    val metadata:HashMap[String,MemoryUnit];
    val lock:Lock;
    //var print:Boolean = false;
    public def this() {
        metadata = new HashMap[String,MemoryUnit]();
        lock = new Lock();
    }
    
    public def this(values:HashMap[String,Cloneable]) {
        metadata = new HashMap[String,MemoryUnit]();
        lock = new Lock();
        
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.put(k, new MemoryUnit(v));
        }
    }
    
    public def getMap() = metadata;
    
    public def getKeyValueMap() {
        try {
            lock(-1);
            val values = new HashMap[String,Cloneable]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val k = iter.next();
                val memU = metadata.getOrThrow(k);
                if (!memU.isDeleted()) {
                    val v = metadata.getOrThrow(k).getValueLocked(false, k, -1);
                    values.put(k, v);
                }
            }
            return values;
            
        }finally {
            unlock(-1);
        }
    }
    
    public def getMemoryUnit(k:String):MemoryUnit {
        var res:MemoryUnit = null;
        try {
            lock(-1);
            res = metadata.getOrElse(k, null);
            if (res == null) {
                res = new MemoryUnit(null);
                metadata.put(k, res);
                //if (print)
                //    Console.OUT.println(here + " MapData.put ("+k+")");
                val size = metadata.size(); 
                if (size %10000 == 0) {
                    Console.OUT.println(here + " MapData.size = " + size);
                    //print = true;
                }
            }
            res.ensureNotDeleted(k);
            return res;
        }finally {
            unlock(-1);
        }
    }
    
    public def initLog(key:String, active:Boolean, log:TxLog, keyLog:TxKeyChange, lockRead:Boolean):MemoryUnit {
        val txId = log.id;
        var memory:MemoryUnit = null;
        var added:Boolean  = false;
        try {
            lock(txId);
            memory = metadata.getOrElse(key, null);
            if (memory == null) {
                if (!active)
                    throw new StorePausedException(here + " MapData can not put values while the store is paused ");
                memory = new MemoryUnit(null);
                metadata.put(key, memory);
                //if (print)
                //    Console.OUT.println(here + " MapData.put ("+k+")");
                added = true;
                val size = metadata.size(); 
                if (size %10000 == 0) {
                    Console.OUT.println(here + " MapData.size = " + size);
                    //print = true;
                }
            }
        }finally {
            unlock(txId);
        }
        
        memory.ensureNotDeleted(key);
        if (lockRead) {
            memory.lockRead(log.id, key);
        }
        
        if (keyLog.key() == null) {
            memory.initializeTxKeyLog(key, log.id, lockRead, added, keyLog);
        }
        return memory;
    }
    
    
    public def deleteMemoryUnit(txId:Long, k:String):void {
        try {
            lock(txId);
            metadata.delete(k);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " MapData.delete(" + k + ")");
        }finally {
            unlock(txId);
        }
    }
    
    public def keySet(mapName:String) {
        try {
            lock(-1);
            val set = new HashSet[String]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                if (key.startsWith(mapName)) {
                    set.add(key.substring(mapName.length() as Int , key.length() as Int));
                }
            }
            return set;
        }finally {
            unlock(-1);
        }
    }
    
    public def lock(txId:Long){
        if (!TxConfig.get().LOCK_FREE) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " " + here +" mapdata.lock() start");
            lock.lock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " " + here +" mapdata.lock() end");
        }
    }
    
    public def unlock(txId:Long) {
        if (!TxConfig.get().LOCK_FREE) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " " + here +" mapdata.unlock() start");
            lock.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + TxManager.txIdToString(txId) + " " + here +" mapdata.unlock() end");
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
    
    public def baselineGetValue(k:String):Cloneable {
        val memU = metadata.getOrElse(k, null);
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }
    
    public def baselinePutValue(k:String, value:Cloneable):Cloneable {
        var memU:MemoryUnit = metadata.getOrElse(k, null);
        if (memU == null) {
            memU = new MemoryUnit(value);
            metadata.put(k, memU);
        }
        
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }

}