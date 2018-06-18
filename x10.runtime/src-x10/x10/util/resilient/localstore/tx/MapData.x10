package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.tx.TxLog;
import x10.util.resilient.localstore.tx.TxKeyChange;
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreFatalException;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock to synchronize access to the shared metadata hashmap.
 **/
public class MapData[K] {K haszero} {
    val metadata:HashMap[K,MemoryUnit[K]];
    val masterTxDesc:HashMap[Long,TxDesc];

    val lock:Lock;

    val txDescLock:Lock;

    //var print:Boolean = false;
    public def this() {
        metadata = new HashMap[K,MemoryUnit[K]]();
        masterTxDesc = new HashMap[Long,TxDesc]();
        txDescLock = new Lock();
        lock = new Lock();
    }
    
    public def this(values:HashMap[K,Cloneable]) {
        metadata = new HashMap[K,MemoryUnit[K]]();
        masterTxDesc = new HashMap[Long,TxDesc]();
        txDescLock = new Lock();
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
                //if (print)
                //    Console.OUT.println(here + " MapData.put ("+k+")");
                val size = metadata.size(); 
                if (size %10000 == 0) {
                    Console.OUT.println(here + " MapData.size = " + size);
                    //print = true;
                }
            }
            res.ensureNotDeleted();
            return res;
        }finally {
            unlock(-1);
        }
    }
    
    public def initLog(key:K, active:Boolean, log:TxLog[K], keyLog:TxKeyChange[K], lockRead:Boolean):MemoryUnit[K] {        
        val txId = log.id();
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] MapData.initLog(" + key + ")");
        
        if (txId < 0) 
            throw new TxStoreFatalException(here + " fatal error, MapData.initLog txId = -1");
        
        var memory:MemoryUnit[K] = null;
        var added:Boolean  = false;
        try {
            lock(txId);
            memory = metadata.getOrElse(key, null);
            if (memory == null) {
                if (!active)
                    throw new TxStorePausedException(here + " MapData can not put values while the store is paused ");
                memory = new MemoryUnit[K](null);
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
            //if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] MapData.delete(" + key + ")");
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
        if (!TxConfig.get().LOCK_FREE) {
            lock.lock();
        }
    }
    
    public def unlock(txId:Long) {
        if (!TxConfig.get().LOCK_FREE) {
            lock.unlock();
        }
    }
    
    public def getTxDesc(id:Long) {
    	try {
    		lockTxDesc();
    		if (TxConfig.get().TM_DEBUG) Console.OUT.println("TxDesc["+id+"] " + " here["+here+"] MapData.get() started ...");
    		val desc = masterTxDesc.getOrElse(id, null);
    		if (TxConfig.get().TM_DEBUG) Console.OUT.println("TxDesc["+id+"] " + " here["+here+"] MapData.get() completed => {"+desc+"} ...");
    		return desc;
    	} finally {
    		unlockTxDesc();
    	}
    }
    
    public def putTxDesc(id:Long, desc:TxDesc) {
    	try {
    		lockTxDesc();
    		masterTxDesc.put(id, desc);
    		if (TxConfig.get().TM_DEBUG) Console.OUT.println("TxDesc["+id+"] " + " here["+here+"] MapData.put() => {"+desc+"} ...");
    	} finally {
    		unlockTxDesc();
    	}
    }
    
    public def updateTxDescStatus(id:Long, newStatus:Long) {
    	try {
    		lockTxDesc();
    		val desc = masterTxDesc.getOrThrow(id);
    		desc.status = newStatus;
    		if (TxConfig.get().TM_DEBUG) Console.OUT.println("TxDesc["+id+"] " + " here["+here+"] MapData.updateTxDescStatus() => {"+desc+"} ...");
    	} finally {
    		unlockTxDesc();
    	}
    }
    
    public def addTxDescMember(id:Long, memId:Long) {
    	try {
    		lockTxDesc();
    		var desc:TxDesc = masterTxDesc.getOrElse(id, null);
    		if (desc == null) {
    			desc = new TxDesc(id, false); 
    			masterTxDesc.put(id, desc);
    		}
    		desc.addVirtualMember(memId);
    	} finally {
    		unlockTxDesc();
    	}
    }
    
    public def removeTxDesc(id:Long) {
    	try {
    		lockTxDesc();
    		return masterTxDesc.remove(id);
    	} finally {
    		unlockTxDesc();
    	}
    }
    
    public def lockTxDesc(){
        if (!TxConfig.get().LOCK_FREE) {
        	txDescLock.lock();
        }
    }
    
    public def unlockTxDesc() {
        if (!TxConfig.get().LOCK_FREE) {
        	txDescLock.unlock();
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