package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData {
    val metadata:HashMap[String,MemoryUnit];
    val lock:Lock;

    public def this() {
        metadata = new HashMap[String,MemoryUnit]();
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
            lock = new Lock();
        else
            lock = null;
    }
    
    public def this(values:HashMap[String,Cloneable]) {
        metadata = new HashMap[String,MemoryUnit]();
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.put(k, new MemoryUnit(v));
        }
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
            lock = new Lock();
        else
            lock = null;
    }
    
    public def getMap() = metadata;
    
    public def getKeyValueMap() {
        try {
            lockWholeMap();
            val values = new HashMap[String,Cloneable]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val k = iter.next();
                val v = metadata.getOrThrow(k).getAtomicValue(false, k, -1).value;
                values.put(k, v);
            }
            return values;
            
        }finally {
            unlockWholeMap();
        }
    }
    
    public def getMemoryUnit(k:String):MemoryUnit {
    	var res:MemoryUnit = null;
        try {
            lockWholeMap();
            res = metadata.getOrElse(k, null);
            if (res == null) {
                res = new MemoryUnit(null);
                metadata.put(k, res);
            }
        }finally {
            unlockWholeMap();
        }
        return res;
    }
    
    public def keySet(mapName:String) {
        try {
            lockWholeMap();
            val set = new HashSet[String]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
            	val key = iter.next();
            	if (key.startsWith(mapName))
            		set.add(key);
            }
            return set;
        }finally {
            unlockWholeMap();
        }
    }
    
    private def lockWholeMap(){
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
            lock.lock();
    }
    
    private def unlockWholeMap() {
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
            lock.unlock();
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