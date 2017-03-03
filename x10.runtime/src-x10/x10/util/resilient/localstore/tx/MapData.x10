package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData {
    val metadata:HashMap[String,MemoryUnit];
    val lock:Lock;

    public def this() {
        metadata = new HashMap[String,MemoryUnit]();
        lock = new Lock();
    }
    
    public def this(values:HashMap[String,Cloneable]) {
        metadata = new HashMap[String,MemoryUnit]();
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.put(k, new MemoryUnit(v));
        }
        lock = new Lock();
    }
    
    public def getMap() = metadata;
    
    public def getKeyValueMap() {
        try {
            lock.lock();
            val values = new HashMap[String,Cloneable]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
                val k = iter.next();
                val v = metadata.getOrThrow(k).getAtomicValue(false, k, -1).value;
                values.put(k, v);
            }
            return values;
            
        }finally {
            lock.unlock();
        }
    }
    
    public def getMemoryUnit(k:String):MemoryUnit {
    	var res:MemoryUnit = null;
        try {
            lock.lock();
            res = metadata.getOrElse(k, null);
            if (res == null) {
                res = new MemoryUnit(null);
                metadata.put(k, res);
            }
        }finally {
            lock.unlock();
        }
        return res;
    }
    
    public def keySet(mapName:String) {
        try {
            lock.lock();
            val set = new HashSet[String]();
            val iter = metadata.keySet().iterator();
            while (iter.hasNext()) {
            	val key = iter.next();
            	if (key.startsWith(mapName))
            		set.add(key);
            }
            return set;
        }finally {
            lock.unlock();
        }
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