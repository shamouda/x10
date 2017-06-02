package x10.util.resilient.localstore.tx;

import x10.util.resilient.localstore.TxConfig;
import x10.util.concurrent.Lock;

public class TxLogManager[K] {K haszero} {
    private val txLogs:Rail[TxLog[K]];
    private val lock:Lock;
    private var insertIndex:Long;
    
    public def this() {
        txLogs = new Rail[TxLog[K]](TxConfig.get().MAX_CONCURRENT_TXS);
        //pre-allocate transaction logs
        for (var i:Long = 0 ; i < TxConfig.get().MAX_CONCURRENT_TXS; i++) {
            txLogs(i) = new TxLog[K]();
        }
        lock = new Lock();
        insertIndex = 0;
    }
    
    
    public def search(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.get().MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id == id)
                    return txLogs(i);
            }
            return null;
        }
        finally {
            unlock();
        }
    }
    
    public def getOrAdd(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.get().MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id == id)
                    return txLogs(i);
            }
            var obj:TxLog[K] = null;
            for (var i:Long = 0 ; i < TxConfig.get().MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id == -1) {
                    txLogs(i).id = id;
                    obj = txLogs(i);                  
                    break;
                }
            }
            if (obj == null) {
                throw new ConcurrentTransactionsLimitExceeded(here + " ConcurrentTransactionsLimitExceeded");
            }
            return obj;
        }
        finally {
            unlock();
        }
    }
    
    public def delete(log:TxLog[K]) {
        try {
            lock();
            log.reset();
        }
        finally {
            unlock();
        }
    }
    
    public def deleteAborted(log:TxLog[K]) {
        //SS_CHECK keep track of aborted transactions
        delete(log);
    }
    
    public def activeTransactionsExist() {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.get().MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id != -1 && txLogs(i).writeValidated) {
                    Console.OUT.println("Recovering " + here + " MasterStore.waitUntilPaused  found a non-aborted transaction Tx["+txLogs(i).id+"] " + TxManager.txIdToString (txLogs(i).id));
                    return true;
                }
            }
            return false;
        }
        finally {
            unlock();
        }
    }
    
    
    public def lock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
    }
}