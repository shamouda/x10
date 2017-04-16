package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.SafeBucketHashMap;
import x10.util.resilient.localstore.tx.TxManager;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData {
    val metadata:SafeBucketHashMap[String,MemoryUnit];
    //var print:Boolean = false;
    public def this() {
        metadata = new SafeBucketHashMap[String,MemoryUnit](TxConfig.get().BUCKETS_COUNT);
    }
    
    public def this(values:HashMap[String,Cloneable]) {
        metadata = new SafeBucketHashMap[String,MemoryUnit](TxConfig.get().BUCKETS_COUNT);
        if (!TxConfig.get().LOCK_FREE)
            metadata.lockAll();
        
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.putUnsafe(k, new MemoryUnit(v));
        }
        
        if (!TxConfig.get().LOCK_FREE)
            metadata.unlockAll();
    }
    
    public def getMap() = metadata;
    
    public def getKeyValueMap() {
        try {
            lockAll();
            val values = new HashMap[String,Cloneable]();
            val iter = metadata.keySetUnsafe().iterator();
            while (iter.hasNext()) {
                val k = iter.next();
                val memU = metadata.getOrThrowUnsafe(k);
                if (!memU.deleted) {
                    val v = metadata.getOrThrowUnsafe(k).getAtomicValueLocked(false, k, -1).value;
                    values.put(k, v);
                }
            }
            return values;
            
        }finally {
            unlockAll();
        }
    }
    
    public def getMemoryUnit(k:String, active:Boolean):MemUnitResponse {
        var res:MemoryUnit = null;
        var added:Boolean  = false;
        try {
            lockKey(k);
            res = metadata.getOrElseUnsafe(k, null);
            if (res == null) {
                if (!active)
                    throw new StorePausedException(here + " MapData can not put values while the store is paused ");
                res = new MemoryUnit(null);
                metadata.putUnsafe(k, res);
                //if (print)
                //    Console.OUT.println(here + " MapData.put ("+k+")");
                added = true;
                val size = metadata.sizeUnsafe(); 
                if (size %10000 == 0) {
                    Console.OUT.println(here + " MapData.size = " + size);
                    //print = true;
                }
            }
            res.ensureNotDeleted(k);
            return new MemUnitResponse(res, added);
        }finally {
            unlockKey(k);
        }
    }
    
    public def deleteMemoryUnit(txId:Long, k:String):void {
        try {
            lockKey(k);
            metadata.deleteUnsafe(k);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " MapData.delete(" + k + ")");
        }finally {
            unlockKey(k);
        }
    }
    
    public def keySet(mapName:String) {
        try {
            lockAll();
            val set = new HashSet[String]();
            val iter = metadata.keySetUnsafe().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                if (key.startsWith(mapName)) {
                    set.add(key.substring(mapName.length() as Int , key.length() as Int));
                }
            }
            return set;
        }finally {
            unlockAll();
        }
    }
    
    private def lockAll(){
        if (!TxConfig.get().LOCK_FREE)
            metadata.lockAll();
    }
    
    private def unlockAll() {
        if (!TxConfig.get().LOCK_FREE)
            metadata.unlockAll();
    }
    
    private def lockKey(key:String) {
        if (!TxConfig.get().LOCK_FREE)
            metadata.lock(key);
    }
    
    private def unlockKey(key:String) {
        if (!TxConfig.get().LOCK_FREE)
            metadata.unlock(key);
    }
    
    public def toString() {
        try {
            lockAll();
            var str:String = here+"--->\n";
            val iter = metadata.keySetSafe().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val value = metadata.getOrThrowUnsafe(key);
                str += "Key["+key+"] Value["+value.toString()+"]\n";
            }
            return str;
        }finally {
            unlockAll();
        }
    }
    
    public def baselineGetValue(k:String):Cloneable {
        val memU = metadata.getOrElseUnsafe(k, null);
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }
    
    public def baselinePutValue(k:String, value:Cloneable):Cloneable {
        var memU:MemoryUnit = metadata.getOrElseUnsafe(k, null);
        if (memU == null) {
            memU = new MemoryUnit(value);
            metadata.putUnsafe(k, memU);
        }
        
        if (memU == null)
            return null;
        else
            return memU.baselineGet();
    }
    
    public static struct MemUnitResponse {
        public val mem:MemoryUnit;
        public val added:Boolean;
        public def this(mem:MemoryUnit, added:Boolean) {
            this.mem = mem;
            this.added = added;
        }
    };
}