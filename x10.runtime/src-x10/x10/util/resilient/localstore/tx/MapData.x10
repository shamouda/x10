package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.SafeBucketHashMap;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData {
    val metadata:SafeBucketHashMap[String,MemoryUnit];

    public def this() {
        metadata = new SafeBucketHashMap[String,MemoryUnit](TxConfig.getInstance().BUCKETS_COUNT);
    }
    
    public def this(values:HashMap[String,Cloneable]) {
        metadata = new SafeBucketHashMap[String,MemoryUnit]();
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	metadata.lockAll();
        
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.putUnsafe(k, new MemoryUnit(v));
        }
        
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
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
                val v = metadata.getOrThrowUnsafe(k).getAtomicValue(false, k, -1).value;
                values.put(k, v);
            }
            return values;
            
        }finally {
            unlockAll();
        }
    }
    
    public def getMemoryUnit(k:String):MemoryUnit {
    	var res:MemoryUnit = null;
        try {
            lockKey(k);
            res = metadata.getOrElseUnsafe(k, null);
            if (res == null) {
                res = new MemoryUnit(null);
                metadata.putUnsafe(k, res);
            }
        }finally {
            unlockKey(k);
        }
        return res;
    }
    
    public def keySet(mapName:String) {
        try {
            lockAll();
            val set = new HashSet[String]();
            val iter = metadata.keySetUnsafe().iterator();
            while (iter.hasNext()) {
            	val key = iter.next();
            	if (key.startsWith(mapName))
            		set.add(key);
            }
            return set;
        }finally {
            unlockAll();
        }
    }
    
    private def lockAll(){
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	metadata.lockAll();
    }
    
    private def unlockAll() {
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	metadata.unlockAll();
    }
    
    private def lockKey(key:String){
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	metadata.lock(key);
    }
    
    private def unlockKey(key:String) {
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	metadata.unlock(key);
    }
    
    public def toString() {
        var str:String = here+"--->\n";
        val iter = metadata.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val value = metadata.getOrThrow(key);
            str += "Key["+key+"] Value["+value.toString()+"]\n";
        }
        return str;
    }
}