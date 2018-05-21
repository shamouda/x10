package x10.util.resilient.localstore.tx;

import x10.util.resilient.localstore.TxConfig;
import x10.util.concurrent.Lock;

public class TxLogManager[K] {K haszero} {
    private val txLogs:Rail[TxLog[K]];
    private val lockingTxLogs:Rail[LockingTxLog[K]];

    private val lock:Lock;
    private var insertIndex:Long;

    private val abortedTxs:Rail[Long];
    private val abortLock:Lock;
	private var abortIndex:Long;
	private var abortEnd:Long;
    private static val ABORTED_SIZE = 100;
    
    private var lastTaken:Long = -1;
    public def this() {
        //pre-allocate transaction logs
        if (TxConfig.get().STM) {
            txLogs = new Rail[TxLog[K]](TxConfig.MAX_CONCURRENT_TXS);
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                txLogs(i) = new TxLog[K]();
            }
            lockingTxLogs = null;
        } else {
            lockingTxLogs = new Rail[LockingTxLog[K]](TxConfig.MAX_CONCURRENT_TXS);
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                lockingTxLogs(i) = new LockingTxLog[K]();
            }
            txLogs = null;
        }
        
        lock = new Lock();
        insertIndex = 0;
        
        abortedTxs = new Rail[Long](ABORTED_SIZE);
        abortLock = new Lock();
        abortIndex = -1;
        abortEnd = -1;
    }
    
    
    public def markAborted(id:Long) {
    	//if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] markAborted started ...");
    	try {
    		abortLock.lock();
    		if (abortEnd < ABORTED_SIZE -1 )
    			abortEnd++;
    		abortIndex = (abortIndex + 1)%ABORTED_SIZE;
    		abortedTxs(abortIndex) = id;
    	} finally {
    		abortLock.unlock();
    	}
    	//if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] markAborted completed, end="+abortEnd+" ...");
    }
    
    public def isAborted(id:Long) {
    	//if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] isAborted started ...");
    	var result:Boolean = false;
    	try {
    		abortLock.lock();
    		for (var i:Long = 0; i <= abortEnd; i++){
    			if (id == abortedTxs(i)){
    				result = true;
    				break;
    			}
    		}
    	} finally {
    		abortLock.unlock();
    	}
    	//if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] isAborted result="+result+" ...");
    	return result;
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
    
    public def searchTxLogForAbort(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (txLogs(i).id() == id)
                    return txLogs(i);
            }
            markAborted(id);
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
                throw new ConcurrentTransactionsLimitExceeded(here + " ConcurrentTransactionsLimitExceeded");
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
    
    
    public def deleteLockingLog(log:LockingTxLog[K]) {
        try {
            lock();
            log.reset();
        }
        finally {
            unlock();
        }
    }
    
    public def getOrAddLockingLog(id:Long) {
        try {
            lock();
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                if (lockingTxLogs(i).id == id)
                    return lockingTxLogs(i);
            }
            var s:String = "";
            var obj:LockingTxLog[K] = null;
            for (var i:Long = 0 ; i < TxConfig.MAX_CONCURRENT_TXS; i++) {
                s += lockingTxLogs(i).id + " , ";
                if (lockingTxLogs(i).id == -1) {
                    obj = lockingTxLogs(i);                  
                    break;
                }
            }
            if (obj == null) {
                throw new ConcurrentTransactionsLimitExceeded(here + " ConcurrentTransactionsLimitExceeded");
            }
            obj.id = id; //allocate it
            return obj;
        }
        finally {
            unlock();
        }
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