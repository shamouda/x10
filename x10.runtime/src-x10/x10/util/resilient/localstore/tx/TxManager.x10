package x10.util.resilient.localstore.tx;

import x10.util.HashMap;
import x10.util.Set;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.MasterStore;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.TxConfig;

public abstract class TxManager(data:MapData, immediateRecovery:Boolean) {
    protected var status:Long; // 0 (not paused), 1 (preparing to pause), 2 (paused)
    
    private val STATUS_ACTIVE = 0;
    private val STATUS_PAUSING = 1;
    private val STATUS_PAUSED = 2;
    private var resilientStatusLock:Lock;
    
    //stm
    private val txLogManager:TxLogManager;
    
    //locking
    protected val lockingTxLogs:SafeBucketHashMap[Long,LockingTxLog];
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public def this(data:MapData, immediateRecovery:Boolean) {
        property(data, immediateRecovery);
        if (TxConfig.get().BASELINE) {
            txLogManager = null;
            lockingTxLogs = null;
        }
        else if (TxConfig.get().STM ){
            txLogManager = new TxLogManager();
            lockingTxLogs = null;
        }
        else {
            txLogManager = null;
            lockingTxLogs = new SafeBucketHashMap[Long,LockingTxLog](TxConfig.get().BUCKETS_COUNT);
        }
        if (resilient)
        	resilientStatusLock = new Lock();
    }
    
    public static def make(data:MapData, immediateRecovery:Boolean):TxManager {
        if (TxConfig.get().BASELINE )
             return new TxManager_Baseline(data, immediateRecovery);
        else if (TxConfig.get().LOCKING )
            return new TxManager_Locking(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RL_EA_UL"))
            return new TxManager_RL_EA_UL(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RL_EA_WB"))
            return new TxManager_RL_EA_WB(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RL_LA_WB"))
            return new TxManager_RL_LA_WB(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RV_EA_UL"))
            return new TxManager_RV_EA_UL(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RV_EA_WB"))
            return new TxManager_RV_EA_WB(data, immediateRecovery);
        else if (TxConfig.get().TM.equals("RV_LA_WB"))
            return new TxManager_RV_LA_WB(data, immediateRecovery);
        else
            throw new Exception("Wrong Tx Manager Configuration (undo logging can not be selected with late acquire");
    }
    

    public def isReadOnlyTransaction(id:Long) {
        val log = txLogManager.search(id);
        if (log == null /**SS_CHECK|| log.aborted**/)
            return true;
        
        try {
            log.lock();
            return log.isReadOnlyTransaction();
        }finally {
            log.unlock();
        }
    }
    
    /* Used in resilient mode to transfer the changes done by a transaction to the Slave.
     * We filter the TxLog object to remove read-only keys,
     * so that we transfer only the update operations.
     * Accordingly, read-only transactions incurs no replication overhead.
     */
    public def getTxCommitLog(id:Long):HashMap[String,Cloneable] {
        val log = txLogManager.search(id);
        if (log == null /**SS_CHECK|| log.aborted**/)
            return null;
        
        try {
            log.lock();
            return log.prepareCommitLog();
        }finally {
            log.unlock();
        }
    }
   
    public def getOrAddLockingTxLog(id:Long) {
        var log:LockingTxLog = null;
        try {
            lockingTxLogs.lock(id);

            log = lockingTxLogs.getOrElseUnsafe(id, null);
            if (log == null) {
                log = new LockingTxLog(id);
                lockingTxLogs.putUnsafe(id, log);
            }
        }
        finally{
            lockingTxLogs.unlock(id);
        }
        return log;
    }
    
    /**************   Pausing for Recovery    ****************/
    public def waitUntilPaused() {
        Console.OUT.println("Recovering " + here + " MasterStore.waitUntilPaused started ...");
        val buckets = TxConfig.get().BUCKETS_COUNT;
        try {
            Runtime.increaseParallelism("Master.waitUntilPaused");
            
            while (true) {
                if (!txLogManager.activeTransactionsExist()) {
                    break;
                }
                TxConfig.waitSleep();
            }
        
        }finally {
            Runtime.decreaseParallelism(1n, "Master.waitUntilPaused");
        }
        paused();
        Console.OUT.println("Recovering " + here + " MasterStore.waitUntilPaused completed ...");
    }
    
    private def ensureActiveStatus() {
        try {
            statusLock();
            if (status != STATUS_ACTIVE)
                throw new StorePausedException(here + " MasterStore paused for recovery");
        }
        finally {
            statusUnlock();
        }
    }
    
    public def pausing() {
    	try {
    		statusLock();
    		assert(status == STATUS_ACTIVE);
    		status = STATUS_PAUSING;
    		Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_ACTIVE to STATUS_PAUSING");
    	}
    	finally {
    		statusUnlock();
    	}
    }
    
    public def paused() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSING);
    		status = STATUS_PAUSED;
    		Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_PAUSING to STATUS_PAUSED");
    	}
    	finally {
    		statusUnlock();
    	}
    }
    
    public def reactivate() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSED);
    		status = STATUS_ACTIVE;
    		Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_PAUSED to STATUS_ACTIVE");
    	}
    	finally {
    		statusUnlock();
    	}
    }
    
    public def isActive() {
    	try {
    		statusLock();
    		return status == STATUS_ACTIVE;
    	}
    	finally {
    		statusUnlock();
    	}
    }
    
    /********************************************************/
    
    public static def checkDeadCoordinator(txId:Long, key:String) {
        val placeId = TxConfig.getTxPlaceId(txId);
        if (Place(placeId as Long).isDead()) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + txIdToString (txId)+ " key["+key+"] coordinator place["+Place(placeId)+"] died !!!!");
            throw new DeadPlaceException(Place(placeId));
        }
    }
    
    /*************** Abstract Methods ****************/
    public abstract def get(id:Long, key:String):Cloneable;
    public abstract def put(id:Long, key:String, value:Cloneable):Cloneable;
    public abstract def delete(id:Long, key:String, txDesc:Boolean):Cloneable;
    public abstract def validate(log:TxLog):void ;
    public abstract def commit(log:TxLog):void ;
    public abstract def abort(log:TxLog):void; //log must be locked
    public abstract def lockRead(id:Long, key:String):void;
    public abstract def lockWrite(id:Long, key:String):void;
    public abstract def unlockRead(id:Long, key:String):void;
    public abstract def unlockWrite(id:Long, key:String):void;
    public abstract def lockRead(id:Long, keys:ArrayList[String]):void;
    public abstract def lockWrite(id:Long, keys:ArrayList[String]):void;
    public abstract def unlockRead(id:Long, keys:ArrayList[String]):void;
    public abstract def unlockWrite(id:Long, keys:ArrayList[String]):void;
    /*************************************************/
    
    public def validate(id:Long) {
        val log = txLogManager.search(id);
        if (log == null)
            return;
        
        try {
            log.lock();
            validate(log);
        } finally {
            log.unlock();
        }
    }
    
    public def commit(id:Long) {
        val log = txLogManager.search(id);
        if (log == null)
            return;
        
        try {
            log.lock();
            commit(log);
        } finally {
            log.unlock();
        }
        
        txLogManager.delete(log);
    }
    
    public def abort(id:Long) {
        /*Abort may reach before normal Tx operations, wait until we have a txLog to abort*/
        val log = txLogManager.search(id);
        if (log == null) {
            return;
        }

        try {
            log.lock();
            abort(log);
        } finally {
            log.unlock();
        }
        
        txLogManager.deleteAborted(log);
    }
    
    public def keySet(mapName:String, id:Long):Set[String] {
        return data.keySet(mapName);
    }
    
    /********************** Utils ***************************/
    /*throws an exception if a conflict was found*/
    protected def logInitialIfNotLogged(id:Long, key:String, lockRead:Boolean) {
        val log = txLogManager.getOrAdd(id);
        log.lock();
        try {
            //SS_CHECK uncomment if going to record aborted tx
            /*
            if (log.aborted) {
                throw new AbortedTransactionException("AbortedTransactionException");
            }
            */
            val keyLog = log.getOrAddKeyLog(key);
            var memory:MemoryUnit = keyLog.getMemoryUnit();
            if (memory == null) {
                memory = data.initLog(key, status == STATUS_ACTIVE, log, keyLog, lockRead);
            }
            log.setLastUsedMemoryUnit(memory);
            log.setLastUsedKeyLog(keyLog);
            
            return log;
        } catch(ex:AbortedTransactionException) {
            throw ex;
        } catch(ex:Exception) {
            try {
                abortAndThrowException(log, ex);
            }
            finally {
                log.unlock();
            }
            throw ex;
        }
    }
    
    protected def abortAndThrowException(log:TxLog, ex:Exception) {
        if (log != null) {
            abort(log);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+log.id+"] " + txIdToString (log.id)+ " " + here + "   TxManager.abortAndThrowException   throwing exception["+ex.getMessage()+"] ");
        }
        throw ex;
    }
    
    /************  Common Implementations for Get/Put/Delete/Commit/Abort/Validate ****************/
    private def lockWriteRV(id:Long, key:String, memory:MemoryUnit, log:TxLog, keyLog:TxKeyChange, delete:Boolean) {
        if (!keyLog.getLockedWrite()) {
            //there is no need to do unlockRead, in read versioning we don't lock the keys while reading
            memory.lockWrite(id, key); 
            val curVer = memory.getVersionLocked(false, key, id);
            val initVer = keyLog.getInitVersion();
            if (curVer != initVer) {
                /*another transaction has modified it and committed since we read the initial value*/
                memory.unlockWrite(id, key);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new ConflictException("ConflictException["+here+"] Tx["+id+"] " + txIdToString (id) , here);
            }
            log.setAllWriteFlags(keyLog, true, delete);
        }
    }
    
    private def lockWriteRL(id:Long, key:String, memory:MemoryUnit, log:TxLog, keyLog:TxKeyChange, delete:Boolean) {
        if (!keyLog.getLockedWrite()) {
            
            if (keyLog.getLockedRead())
                keyLog.setLockedRead(false);
            
            memory.lockWrite(id, key); //lockWrite unlockRead if upgrading fails 
            
            log.setAllWriteFlags(keyLog, true, delete);
        }
    }
    
    protected def put_RV_EA_WB(id:Long, key:String, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog = null;
        try {     	
            log = logInitialIfNotLogged(id, key, false);        	
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            if (resilient && immediateRecovery && !txDesc)
                ensureActiveStatus();
            
            /*** EarlyAcquire ***/
            lockWriteRV(id, key, memory, log, keyLog, delete);
            
            /*** Write Buffering ***/
            if (delete)
                return log.logDelete(keyLog);
            else{
                val copy1 = (value == null)? null:value.clone();
                return log.logPut(keyLog, copy1);
            }
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:ConcurrentTransactionsLimitExceeded) { /**SS_CHECK  repeat**/
            throw ex;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_WB(id:Long, key:String, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog = null;
        try {
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            if (resilient && immediateRecovery && !txDesc)
                ensureActiveStatus();
            
            /*** EarlyAcquire ***/
            lockWriteRL(id, key, memory, log, keyLog, delete);
            
            /*** Write Buffering ***/
            if (delete)
                return log.logDelete(keyLog);
            else {
                val copy1 = (value == null)? null:value.clone();
                return log.logPut(keyLog, copy1);
            }
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_UL(id:Long, key:String, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL started, key["+key+"] delete["+delete+"] ");
        var log:TxLog = null;
        try {
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            if (resilient && immediateRecovery && !txDesc)
                ensureActiveStatus();
            
            /*** Early Acquire ***/
            lockWriteRL(id, key, memory, log, keyLog, delete);
            
            /*** Undo Logging ***/
            if (delete){
                return memory.setValueLocked(null, key, id, delete);
            }
            else {
                val copy1 = (value == null)? null:value.clone();
                return memory.setValueLocked(copy1, key, id, delete);
            }
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL completed");
        }
    }
    
    protected def put_RV_EA_UL(id:Long, key:String, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL started, key["+key+"] delete["+delete+"] ");
        var log:TxLog = null;
        try {
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            if (resilient && immediateRecovery && !txDesc)
                ensureActiveStatus();
            
            /*** EarlyAcquire ***/
            lockWriteRV(id, key, memory, log, keyLog, delete);
            
            /*** Undo Logging ***/
            if (delete){
                return memory.setValueLocked(null, key, id, delete);
            }
            else {
                val copy1 = (value == null)? null:value.clone();
                return memory.setValueLocked(copy1, key, id, delete);
            }
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL completed");
        }
    }
    
    
    protected def put_LA_WB(id:Long, key:String, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog = null;
        try {
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            if (resilient && immediateRecovery && !txDesc)
                ensureActiveStatus();
            
            /*** DO NOT ACQUIRE WRITE LOCK HERE ***/
            
            /*** Write Buffering ***/
            val copy1 = (value == null)? null:value.clone();
            if (delete) {
                return log.logDelete(keyLog);
            }
            else
                return log.logPut(keyLog, copy1);
            
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB completed");
        }
    }
    
    /********************* End of put operations  *********************/
    
    protected def get_RL_UL(id:Long, key:String):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL started");
        var log:TxLog = null;
        try {
            /*** Read Locking ***/
            log = logInitialIfNotLogged(id, key, true);
            val memory = log.getLastUsedMemoryUnit();
            
            
            /*** Undo Logging ***/
            //true = send a different copy to use to avoid manipulating the log or the original data
            val atomicV = memory.getValueLocked(true, key, id);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " getvv  value["+atomicV+"]");
            return atomicV;
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL completed");
        }
    }
    
    protected def get_RL_WB(id:Long, key:String):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB started");
        var log:TxLog = null;
        try {
            log = logInitialIfNotLogged(id, key, true);
            val memory = log.getLastUsedMemoryUnit(); 
            val keyLog = log.getLastUsedKeyLog();
            
            /*** Write Buffering ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, keyLog); 
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB completed");
        }
    }
    
    protected def get_RV_WB(id:Long, key:String):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
            val keyLog = log.getLastUsedKeyLog();
            
            /*** WriteBuffering: read value from buffer ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, keyLog);            
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB completed");
        }
    }
    
    protected def get_RV_UL(id:Long, key:String):Cloneable {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            log = logInitialIfNotLogged(id, key, false);
            val memory = log.getLastUsedMemoryUnit();
           
            /*** UndoLogging: read value from memory ***/
            val copy = true; // send a different copy to use to avoid manipulating the log or the original data 
            return memory.getValue(copy, key, id);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL completed");
        }
    }
    
    /********************* End of get operations  *********************/
        
    protected def validate_RL_LA_WB(log:TxLog) {
        val id = log.id;
        var writeTx:Boolean = false;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB started");
        try {
            val writeList = log.getWriteKeys();
            for (var i:Int = 0n; i < writeList.size(); i++){
                val kLog = writeList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                
                assert(!kLog.getReadOnly());
                val deleted = kLog.getDeleted();
                lockWriteRL(id, key, memory, log, kLog, deleted);
                writeTx = true;
            }
            
            if (writeTx) {
                if (resilient && immediateRecovery)
                    ensureActiveStatus();
            	log.writeValidated = true;
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally{
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB completed");
        }
    }
    
    protected def validate_RV_LA_WB(log:TxLog) {
        val id = log.id;
        var writeTx:Boolean = false;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB started");
        try {
            val writeList = log.getWriteKeys();
            for (var i:Int = 0n; i < writeList.size(); i++){
                val kLog = writeList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                assert(!kLog.getReadOnly());

                memory.lockWrite(id, key);
                writeTx = true;
                
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockWrite(id, key);
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
                kLog.setLockedWrite(true);
            }
            val readList = log.getReadKeys();
            for (var i:Int = 0n; i < readList.size(); i++){
                val kLog = readList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                assert(kLog.getReadOnly());
                memory.lockRead(id, key); 
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockRead(id, key);
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
                kLog.setLockedRead(true);
            }
            
            if (writeTx) {
                if (resilient && immediateRecovery)
                    ensureActiveStatus();
            	log.writeValidated = true;
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB completed");
        }
    }
    
    protected def validate_RV_EA(log:TxLog) {
        val id = log.id;
        var writeTx:Boolean = false;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL started");
        try {
            val writeList = log.getReadKeys();
            if (writeList.size() > 0)
                writeTx = true;
            
            val readList = log.getReadKeys();
            for (var i:Int = 0n; i < readList.size(); i++){
                val kLog = readList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                
                assert (!kLog.getLockedWrite());
                //lock read only key
                
                memory.lockRead(id, key); 
                    
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockRead(id, key);
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
                kLog.setLockedRead(true);
            }
            if (writeTx) {
                if (resilient && immediateRecovery)
                    ensureActiveStatus();
            	log.writeValidated = true; // we can not start migratoin until all writeValidated transactions commit or abort
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL completed");
        }
    }
    
    /********************* End of validate operations  *********************/
    
    protected def commit_WB(log:TxLog) {
        val id = log.id;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB started");
        
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            memory.unlockRead(log.id, key);
        }
        
        val writeList = log.getWriteKeys();
        for (var i:Int = 0n; i < writeList.size(); i++){
            val kLog = writeList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            val deleted = kLog.getDeleted();
            memory.setValueLocked(kLog.getValue(), key, log.id, deleted);
            if (deleted) {
                memory.deleteLocked();
                data.deleteMemoryUnit(id, key);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB key["+key+"] deleted");
            }
            memory.unlockWrite(log.id, key);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB completed");
    }
    
    protected def commit_UL(log:TxLog) {
        val id = log.id;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL started");
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            memory.unlockRead(log.id, key);
        }
        
        val writeList = log.getWriteKeys();
        for (var i:Int = 0n; i < writeList.size(); i++){
            val kLog = writeList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
        
            val deleted = kLog.getDeleted();
            if (deleted) {
                memory.deleteLocked();
                data.deleteMemoryUnit(id, key);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL key["+key+"] deleted");
            }
            memory.unlockWrite(log.id, key);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL completed");
    }
    
    /********************* End of commit operations  *********************/
    protected def abort_UL(log:TxLog) {
        val id = log.id;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL started");
        if (log.aborted) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            if (kLog.getLockedRead()) {
                if (kLog.getAdded()){
                    memory.deleteLocked();
                    data.deleteMemoryUnit(id, key);
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL key["+key+"] deleted ");
                }
                memory.unlockRead(log.id, key);
            }
        }
        val writeList = log.getWriteKeys();
        for (var i:Int = 0n; i < writeList.size(); i++){
            val kLog = writeList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            
            if (kLog.getLockedWrite()) {
                if (kLog.getAdded()){
                    memory.deleteLocked();
                    data.deleteMemoryUnit(id, key);
                }
                else { 
                    memory.rollbackValueLocked(kLog.getValue(), kLog.getInitVersion(), key, log.id);    
                }
                memory.unlockWrite(log.id, key);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL completed");
    }
    
    /** With write buffering: memory is not impacted, just unlock the locked keys*/
    protected def abort_WB(log:TxLog) {
        val id = log.id;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB started");
        if (log.aborted) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
        }
        
        val writeList = log.getWriteKeys();
        for (var i:Int = 0n; i < writeList.size(); i++){
            val kLog = writeList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            if (kLog.getLockedWrite()) {
                if (kLog.getAdded()){
                    memory.deleteLocked();
                    data.deleteMemoryUnit(id, key);
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB key["+key+"] deleted ");
                }
                memory.unlockWrite(log.id, key);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB completed");
    }
    
    /********************* End of abort operations  *********************/
        
    
    /*******   TxManager_Locking methods *********/
    public def lockWrite_Locking(id:Long, key:String) {
        val log = getOrAddLockingTxLog(id);
        var memory:MemoryUnit = log.memUnits.getOrElse(key, null);
        if (memory == null) {
            memory = data.getMemoryUnit(key);
            log.memUnits.put(key, memory);
        }
        memory.lockWrite(id, key);
    }
    
    public def lockWrite_Locking(id:Long, keys:ArrayList[String]) {
        val log = getOrAddLockingTxLog(id);
        for (key in keys) {
            var memory:MemoryUnit = log.memUnits.getOrElse(key, null);
            if (memory == null) {
                memory = data.getMemoryUnit(key);
                log.memUnits.put(key, memory);
            }
            memory.lockWrite(id, key);
        }
    }
    
    public def lockRead_Locking(id:Long, key:String) {
        val log = getOrAddLockingTxLog(id);
        var memory:MemoryUnit = log.memUnits.getOrElse(key, null);
        if (memory == null) {
            memory = data.getMemoryUnit(key);
            log.memUnits.put(key, memory);
        }
        memory.lockRead(id, key);        
    }
    
    public def lockRead_Locking(id:Long, keys:ArrayList[String]) {
        val log = getOrAddLockingTxLog(id);
        for (key in keys) {
            var memory:MemoryUnit = log.memUnits.getOrElse(key, null);
            if (memory == null) {
                memory = data.getMemoryUnit(key);
                log.memUnits.put(key, memory);
            }
            memory.lockRead(id, key);
        }
    }
    
    public def unlockWrite_Locking(id:Long, key:String) {
        val log = getOrAddLockingTxLog(id);
        val memory = log.memUnits.getOrElse(key, null);
        assert (memory != null) : "locking mistake, unlocking a key that was not locked";
        memory.unlockWrite(id, key);
    }
    
    public def unlockWrite_Locking(id:Long, keys:ArrayList[String]) {
        val log = getOrAddLockingTxLog(id);
        for (key in keys) {
            val memory = log.memUnits.getOrElse(key, null);
            assert (memory != null) : "locking mistake, unlocking a key that was not locked";
            memory.unlockWrite(id, key);
        }
    }
    
    public def unlockRead_Locking(id:Long, key:String) {
        val log = getOrAddLockingTxLog(id);
        val memory = log.memUnits.getOrElse(key, null);
        assert (memory != null) : "locking mistake, unlocking a key that was not locked";
        memory.unlockRead(id, key);
    }
    
    public def unlockRead_Locking(id:Long, keys:ArrayList[String]) {
        val log = getOrAddLockingTxLog(id);
        for (key in keys) {
            val memory = log.memUnits.getOrElse(key, null);
            assert (memory != null) : "locking mistake, unlocking a key that was not locked";
            memory.unlockRead(id, key);
        }
    }
    
    public def getLocked(id:Long, key:String):Cloneable {
        val log = getOrAddLockingTxLog(id);
        val memory = log.memUnits.getOrElse(key, null);
        assert (memory != null) : "locking mistake, getting a value before locking it";
        return memory.getValueLocked(true, key, id);
    }
    
    public def  putLocked(id:Long, key:String, value:Cloneable):Cloneable {
        val log = getOrAddLockingTxLog(id);
        val memory = log.memUnits.getOrElse(key, null);
        assert (memory != null) : "locking mistake, putting a value before locking it";    
        return memory.setValueLocked(value, key, id, false);
    }
    
    public static def txIdToString (txId:Long) {
        val placeId =  ((txId >> 32) as Int);
        val txSeq = (txId as Int);
        return "TX["+ (placeId*100000000 + txSeq)+"] TXPLC["+placeId+"] TXSEQ["+txSeq+"]";
    }
    
    /*********************************************/
    private def statusLock(){
    	assert(resilient);
        if (!TxConfig.get().LOCK_FREE)
        	resilientStatusLock.lock();
    }
    
    private def statusUnlock(){
    	assert(resilient);
        if (!TxConfig.get().LOCK_FREE)
        	resilientStatusLock.unlock();
    }
}