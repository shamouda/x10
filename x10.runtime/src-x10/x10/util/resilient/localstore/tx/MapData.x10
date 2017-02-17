package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData(name:String) {
    val metadata:HashMap[String,MemoryUnit];
    val lock:Lock;

    public def this(name:String) {
        property(name);
        metadata = new HashMap[String,MemoryUnit]();
        lock = new Lock();
    }
    
    public def this(name:String, values:HashMap[String,Cloneable]) {
        property(name);
        metadata = new HashMap[String,MemoryUnit]();
        val iter = values.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();
            val v = values.getOrThrow(k);
            metadata.put(k, new MemoryUnit(v));
        }
        lock = new Lock();
    }
    
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
    
    public def keySet() {
        try {
            lock.lock();
            return metadata.keySet();
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