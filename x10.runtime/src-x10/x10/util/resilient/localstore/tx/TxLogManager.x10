package x10.util.resilient.localstore.tx;

import x10.util.resilient.localstore.TxConfig;
import x10.util.concurrent.Lock;
import x10.xrx.TxStoreConcurrencyLimitException;

public class TxLogManager[K] {K haszero} {
    private val txLogs:Rail[TxLog[K]];

    private val lock:Lock;
    private var insertIndex:Long;

    private var lastTaken:Long = -1;
    public def this() {
        //pre-allocate transaction logs
        if (TxConfig.get().STM) {
            txLogs = new Rail[TxLog[K]](TxConfig.MAX_CONCURRENT_TXS);
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                txLogs(i) = new TxLog[K]();
            }
        } else {
            txLogs = null;
        }
        
        lock = new Lock();
        insertIndex = 0;
    }
    
    public def searchTxLog(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id() == id)
                    return txLogs(i);
            }
            return null;
        }
        finally {
            unlock();
        }
    }
    
    public def getOrAddTxLog(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id() == id)
                    return txLogs(i);
            }
            var obj:TxLog[K] = null;
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id() == -1) {
                    obj = txLogs(i);                  
                    break;
                }
            }
            if (obj == null) {
                throw new TxStoreConcurrencyLimitException(here + " TxStoreConcurrencyLimitException");
            }
            obj.setId(id); //allocate it
            return obj;
        }
        finally {
            unlock();
        }
    }
    
    public def deleteTxLog(log:TxLog[K]) {
        try {
            lock();
            log.reset();
        }
        finally {
            unlock();
        }
    }
    
    public def deleteAbortedTxLog(log:TxLog[K]) {
        //SS_CHECK keep track of aborted transactions
        deleteTxLog(log);
    }
    
    public def activeTransactionsExist() {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id() != -1 && txLogs(i).writeValidated) {
                    Console.OUT.println("Recovering " + here + " MasterStore.waitUntilPaused  found a non-aborted transaction Tx["+txLogs(i).id()+"] " + TxManager.txIdToString (txLogs(i).id()));
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