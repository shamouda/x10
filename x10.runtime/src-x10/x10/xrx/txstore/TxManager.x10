/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.xrx.txstore;

import x10.util.HashMap;
import x10.util.Set;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;
import x10.xrx.TxStoreConflictException;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreFatalException;
import x10.xrx.TxStoreAbortedException;
import x10.xrx.TxCommitLog;

public abstract class TxManager[K] {K haszero} {
	public val data:TxMapData[K];

    public val immediateRecovery:Boolean;
	
    protected var status:Long; // 0 (not paused), 1 (preparing to pause), 2 (paused)
    
    private val STATUS_ACTIVE = 0;
    private val STATUS_PAUSING = 1;
    private val STATUS_PAUSED = 2;
    private var resilientStatusLock:Lock;
    
    private val txLogManager:TxLogManager[K];
    
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    private static val DO_NOT_CLONE = false;
    private static val NOT_DELETED = false;
    
    public def this(data:TxMapData[K], immediateRecovery:Boolean) {
    	this.data = data;
    	this.immediateRecovery = immediateRecovery;
        txLogManager = new TxLogManager[K]();
        if (resilient)
        	resilientStatusLock = new Lock();
    }
    
    public static def make[K](data:TxMapData[K], immediateRecovery:Boolean){K haszero}:TxManager[K] {
        if (TxConfig.LOCKING )
            return new TxManager_Locking[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RL_EA_UL"))
            return new TxManager_RL_EA_UL[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RL_EA_WB"))
            return new TxManager_RL_EA_WB[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RL_LA_WB"))
            return new TxManager_RL_LA_WB[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RV_EA_UL"))
            return new TxManager_RV_EA_UL[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RV_EA_WB"))
            return new TxManager_RV_EA_WB[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RV_LA_WB"))
            return new TxManager_RV_LA_WB[K](data, immediateRecovery);
        else
            throw new Exception("Wrong Tx Manager Configuration (undo logging can not be selected with late acquire");
    }

    /* Used in resilient mode to transfer the changes done by a transaction to the Slave.
     * We filter the TxLog object to remove read-only keys,
     * so that we transfer only the update operations.
     * Accordingly, read-only transactions incurs no replication overhead.
     */
    public def getTxCommitLog(id:Long):TxCommitLog[K] {
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1) /*SS_CHECK  txLogManager.isAborted(log.id()) */
            return null;
        
        //locking the TxLog is not required here, only one place calls this method
        return log.prepareCommitLog();
    }
    
    /**************   Pausing for Recovery    ****************/
    public def waitUntilPaused() {
    	if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused started ...");
        try {
            Runtime.increaseParallelism();
            try {
                while (true) {
                    if (!txLogManager.activeTransactionsExist()) {
                        break;
                    }
                    System.threadSleep(TxConfig.MASTER_WAIT_MS);
                }
            } catch (ex:Exception) {
                if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused failed ex["+ex.getMessage()+"] ...");
                ex.printStackTrace();
                throw ex;
            }
        } finally {
            Runtime.decreaseParallelism(1n);
        }
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused completed ...");
        paused();
    }
    
    private def ensureActiveStatus() {
        try {
            statusLock();
            if (status != STATUS_ACTIVE)
                throw new TxStorePausedException(here + " TxMasterStore paused for recovery");
        } finally {
            statusUnlock();
        }
    }
    
    public def pausing() {
    	try {
    		statusLock();
    		assert(status == STATUS_ACTIVE);
    		status = STATUS_PAUSING;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_ACTIVE to STATUS_PAUSING");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def paused() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSING);
    		status = STATUS_PAUSED;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_PAUSING to STATUS_PAUSED");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def reactivate() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSED);
    		status = STATUS_ACTIVE;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManager changed status from STATUS_PAUSED to STATUS_ACTIVE");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def isActive() {
    	try {
    		statusLock();
    		return status == STATUS_ACTIVE;
    	} finally {
    		statusUnlock();
    	}
    }
    
    /********************************************************/
    
    public static def checkDeadCoordinator(txId:Long) {
        val placeId = TxConfig.getTxPlaceId(txId);
        if (Place(placeId as Long).isDead()) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + txIdToString (txId)+ " coordinator place["+Place(placeId)+"] died !!!!");
            throw new DeadPlaceException(Place(placeId));
        }
    }
    
    /*************** Abstract Methods ****************/
    public abstract def get(id:Long, key:K):Cloneable;
    public abstract def put(id:Long, key:K, value:Cloneable):Cloneable;
    public abstract def delete(id:Long, key:K, txDesc:Boolean):Cloneable;
    public abstract def validate(log:TxLog[K]):void ;
    public abstract def commit(log:TxLog[K]):void ;
    public abstract def abort(log:TxLog[K]):void; //log must be locked
    
    public abstract def lockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]):void;
    public abstract def unlockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]):void;
    
    /*************************************************/
    
    public def validate(id:Long) {
        val log = txLogManager.searchTxLog(id);
        if (log == null)
            return;
        
        try {
            log.lock(3);
            validate(log);
        } finally {
            log.unlock(3);
        }
    }
    
    public def commit(id:Long) {
        val log = txLogManager.searchTxLog(id);
        if (log == null)
            return;
        
        try {
            log.lock(4);
            commit(log);
        } finally {
            log.unlock(4);
        }
        
        txLogManager.deleteTxLog(log);
    }
    
    public def abort(id:Long) {
        /*Abort may reach before normal Tx operations, wait until we have a txLog to abort*/
        val log = txLogManager.searchTxLog(id); 
        if (log == null) {
            return;
        }
        
        try {
            log.lock(5);
            abort(log);
        } catch (e:Exception) {
            if (TxConfig.TM_DEBUG) {
                e.printStackTrace();
            }
            throw e;
        } finally {
            log.unlock(5, id);
        }
    }
    
    public def keySet(id:Long):Set[K] {
        return data.keySet();
    }
    
    /********************** Utils ***************************/
    /*throws an exception if a conflict was found*/
    protected def lockAndGetTxLog(id:Long, key:K, lockRead:Boolean) {
        val log = txLogManager.getOrAddTxLog(id);
        log.lock(6);
        try {
            if ( log.id() != id /*|| txLogManager.isAborted(log.id()) */) {
                throw new TxStoreAbortedException("TxStoreAbortedException");
            }
            val keyLog = log.getOrAddKeyLog(key);
            var memory:MemoryUnit[K] = keyLog.getMemoryUnit();
            if (memory == null) {
                memory = data.initLog(key, status == STATUS_ACTIVE, log, keyLog, lockRead);
            }
            log.setLastUsedKeyLog(keyLog);
            return log;
        } catch(ex:TxStoreAbortedException) {
        	log.unlock(6);
            throw ex;
        } catch(ex:Exception) {
        	try {
                abortAndThrowException(log, ex, key);
            }
            finally {
                log.unlock(6, id);
            }
            return null;
        }
    }
    
    protected def abortAndThrowException(log:TxLog[K], ex:Exception, key:K) {
        if (log != null) {
            val id = log.id();
            abort(log);
        }
        throw ex;
    }

    protected def abortAndThrowException(log:TxLog[K], ex:Exception) {
        if (log != null) {
            val id = log.id();
            abort(log);
        }
        throw ex;
    }
    
    
    /************  Common Implementations for Get/Put/Delete/Commit/Abort/Validate ****************/
    private def lockWriteRV(id:Long, key:K, memory:MemoryUnit[K], log:TxLog[K], keyLog:TxKeyChange[K], delete:Boolean) {
        if (!keyLog.getLockedWrite()) {
            //there is no need to do unlockRead, in read versioning we don't lock the keys while reading
            memory.lockWrite(id); 
            val curVer = memory.getVersionLocked(false, key, id);
            val initVer = keyLog.getInitVersion();
            if (curVer != initVer) {
                /*another transaction has modified it and committed since we read the initial value*/
                memory.unlockWrite(id);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] " + txIdToString (id) , here);
            }
            log.setAllWriteFlags(keyLog, true, delete);
        }
    }
    
    private def lockWriteRL(id:Long, key:K, memory:MemoryUnit[K], log:TxLog[K], keyLog:TxKeyChange[K], delete:Boolean) {
        if (!keyLog.getLockedWrite()) {
            
            if (keyLog.getLockedRead())
                keyLog.setLockedRead(false);
            
            if (memory == null)
            	throw new TxStoreFatalException (here + " Tx["+id+"] lockWriteRL("+key+") fatal, memory ISNULL");
            
            memory.lockWrite(id); //lockWrite unlockRead if upgrading fails 
            
            log.setAllWriteFlags(keyLog, true, delete);
        }
    }
    
    protected def put_RV_EA_WB(id:Long, key:K, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog[K] = null;
        try {     	
            log = lockAndGetTxLog(id, key, false);   
        } catch(ex:Exception) {
            throw ex;
        }
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
            
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
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_WB(id:Long, key:K, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog[K] = null;
        try {
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            throw ex;
        }
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
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
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_UL(id:Long, key:K, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL started, key["+key+"] ");
        var log:TxLog[K] = null;
        try {
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL completed with Ex, key["+key+"] exception["+ex.getMessage()+"] ");
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
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
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL completed, key["+key+"] ");
        }
    }
    
    protected def put_RV_EA_UL(id:Long, key:K, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL started, key["+key+"] delete["+delete+"] ");
        var log:TxLog[K] = null;
        try {
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            throw ex;
        }
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
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
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL completed");
        }
    }
    
    
    protected def put_LA_WB(id:Long, key:K, value:Cloneable, delete:Boolean, txDesc:Boolean):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB started, key["+key+"] delete["+delete+"] ");
        var log:TxLog[K] = null;
        try {
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
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
            
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB completed");
        }
    }
    
    /********************* End of put operations  *********************/
    protected def get_RL_UL(id:Long, key:K):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL started, key["+key+"] ");
        var log:TxLog[K] = null;
        try {
            /*** Read Locking ***/
            log = lockAndGetTxLog(id, key, true);
        } catch(ex:Exception) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL completed with Ex, key["+key+"] exception["+ex.getMessage()+"] ");
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
            
            /*** Undo Logging ***/
            //true = send a different copy to use to avoid manipulating the log or the original data
            val atomicV = memory.getValueLocked(true, key, id);
            //if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " getvv  value["+atomicV+"]");
            return atomicV;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL completed, key["+key+"] ");
        }
    }
    
    protected def get_RL_WB(id:Long, key:K):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB started");
        var log:TxLog[K] = null;
        try {
            log = lockAndGetTxLog(id, key, true);
        } catch(ex:Exception) {
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
            
            /*** Write Buffering ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, keyLog); 
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB completed");
        }
    }
    
    protected def get_RV_WB(id:Long, key:K):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB started, key["+key+"] ");
        var log:TxLog[K] = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();
            
            /*** WriteBuffering: read value from buffer ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, keyLog);            
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB completed");
        }
    }
    
    protected def get_RV_UL(id:Long, key:K):Cloneable {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL started");
        var log:TxLog[K] = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            log = lockAndGetTxLog(id, key, false);
        } catch(ex:Exception) {
            throw ex;
        }
        
        try{
            val keyLog = log.getLastUsedKeyLog();
            val memory = keyLog.getMemoryUnit();           
            /*** UndoLogging: read value from memory ***/
            val copy = true; // send a different copy to use to avoid manipulating the log or the original data 
            return memory.getValue(copy, key, id);
        } catch(ex:Exception) {
            abortAndThrowException(log, ex, key);
            return null;
        } finally {
            if (log != null)
                log.unlock(6, id);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL completed");
        }
    }
    
    /********************* End of get operations  *********************/
        
    protected def validate_RL_LA_WB(log:TxLog[K]) {
        val id = log.id();
        var writeTx:Boolean = false;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB started");
        try {
            val writeList = log.getWriteKeys();
            for (var i:Int = 0n; i < writeList.size(); i++){
                val kLog = writeList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                
                if (kLog.getReadOnly())
                    throw new TxStoreFatalException("Tx["+id+"] validate_RL_LA_WB  fatal error, key["+key+"] is read only");
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
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB completed");
        }
    }
    
    protected def validate_RV_LA_WB(log:TxLog[K]) {
        val id = log.id();
        var writeTx:Boolean = false;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB started");
        try {
            val writeList = log.getWriteKeys();
            for (var i:Int = 0n; i < writeList.size(); i++){
                val kLog = writeList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                
                if (kLog.getReadOnly())
                    throw new TxStoreFatalException("Tx["+id+"] validate_RV_LA_WB  fatal error, key["+key+"] is read only");

                memory.lockWrite(id);
                writeTx = true;
                
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockWrite(id);
                    throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
                kLog.setLockedWrite(true);
            }
            val readList = log.getReadKeys();
            for (var i:Int = 0n; i < readList.size(); i++){
                val kLog = readList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                assert(kLog.getReadOnly());
                memory.lockRead(id); 
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockRead(id);
                    throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
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
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB completed");
        }
    }
    
    protected def validate_RV_EA(log:TxLog[K]) {
        val id = log.id();
        var writeTx:Boolean = false;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL started");
        try {
            val writeList = log.getReadKeys();
            if (writeList.size() > 0)
                writeTx = true;
            
            val readList = log.getReadKeys();
            for (var i:Int = 0n; i < readList.size(); i++){
                val kLog = readList(i);
                val key = kLog.key();
                val memory = kLog.getMemoryUnit();
                
                if (kLog.getLockedWrite())
                    throw new TxStoreFatalException("Tx["+id+"] validate_RV_EA  fatal error, key["+key+"] is already locked for write");
                
                //lock read only key
                
                memory.lockRead(id); 
                    
                /*Read Validation*/
                val initVer = kLog.getInitVersion();
                val curVer = memory.getVersionLocked(false, key, id);
                
                //detect write after read
                if (curVer != initVer) {
                    memory.unlockRead(id);
                    throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
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
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL completed");
        }
    }
    
    /********************* End of validate operations  *********************/
    
    protected def commit_WB(log:TxLog[K]) {
        val id = log.id();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB started");
        
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            memory.unlockRead(log.id());
        }
        
        val writeList = log.getWriteKeys();
        for (var i:Int = 0n; i < writeList.size(); i++){
            val kLog = writeList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            val deleted = kLog.getDeleted();
            memory.setValueLocked(kLog.getValue(), key, log.id(), deleted);
            if (deleted) {
                memory.deleteLocked();
                data.deleteMemoryUnit(id, key);
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB key["+key+"] deleted");
            }
            memory.unlockWrite(log.id());
        }
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB completed");
    }
    
    protected def commit_UL(log:TxLog[K]) {
        val id = log.id();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL started");
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            memory.unlockRead(log.id());
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
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL key["+key+"] deleted");
            }
            memory.unlockWrite(log.id());
        }
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL completed");
    }
    
    /********************* End of commit operations  *********************/
    protected def abort_UL(log:TxLog[K]) {
        val id = log.id();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL started");
        if (log.aborted) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        //if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] KEYSINFO["+log.keysToString()+"] ...");
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            if (kLog.getLockedRead()) {
                if (kLog.getAdded()){
                    memory.deleteLocked();
                    data.deleteMemoryUnit(id, key);
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL key["+key+"] deleted ");
                }
                memory.unlockRead(log.id());
                kLog.setLockedRead(false);
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
                    memory.rollbackValueLocked(kLog.getValue(), kLog.getInitVersion(), key, log.id());    
                }
                
                try {
                	memory.unlockWrite(log.id());
                }catch(tt:Exception) {
                	if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+log.id()+"] " + txIdToString (log.id())+ " here["+here+"] unlockWrite key["+key+"] FAILED with exception ["+tt.getMessage()+"] ");
                	throw tt;
                }
                kLog.setLockedWrite(false);
            }
        }
        
        txLogManager.deleteAbortedTxLog(log);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL completed");
    }
    
    /** With write buffering: memory is not impacted, just unlock the locked keys*/
    protected def abort_WB(log:TxLog[K]) {
        val id = log.id();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB started");
        if (log.aborted) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        
        val readList = log.getReadKeys();
        for (var i:Int = 0n; i < readList.size(); i++){
            val kLog = readList(i);
            val key = kLog.key();
            val memory = kLog.getMemoryUnit();
            
            if (kLog.getLockedRead()) {
                memory.unlockRead(log.id());
                kLog.setLockedRead(false);
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
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB key["+key+"] deleted ");
                }
                memory.unlockWrite(log.id());
                kLog.setLockedWrite(false);
            }
        }
        
        txLogManager.deleteAbortedTxLog(log);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB completed");
    }
    
    /********************* End of abort operations  *********************/
        
    
    /*******   TxManager_Locking methods *********/
    public def tryLockWrite(id:Long, key:K) {
        val memory = data.getMemoryUnit(key);
        val result = memory.tryLockWrite(id);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] tryLockWrite(key="+key+") result="+result);
        return result;
    }
    
    public def tryLockRead(id:Long, key:K) {
        val memory = data.getMemoryUnit(key);
        val result = memory.tryLockRead(id);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] tryLockRead(key="+key+") result="+result);
        return result;
    }
    
    public def unlockRead(id:Long, key:K) {
        val memory = data.getMemoryUnit(key);
        memory.unlockRead(id);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] unlockRead(key="+key+")");
    }
    
    public def unlockWrite(id:Long, key:K) {
        val memory = data.getMemoryUnit(key);
        memory.unlockWrite(id);
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] unlockWrite(key="+key+")");
    }
    
    public def lock(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        for (var x:Long = 0; x < opPerPlace ; x++) {
            val key = keys(start+x);
            val memory = data.getMemoryUnit(key);
            if (readFlags(start+x))
                memory.lockRead(id);
            else
                memory.lockWrite(id);
        }
    }
    
    public def unlock(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        for (var x:Long = 0; x < opPerPlace ; x++) {
            val key = keys(start+x);
            val memory = data.getMemoryUnit(key);
            if (readFlags(start+x))
                memory.unlockRead(id);
            else
                memory.unlockWrite(id);
        }
    }
    
    public def getLocked(id:Long, key:K):Cloneable {
        val memory = data.getMemoryUnit(key);
        return memory.getValueLocked(DO_NOT_CLONE, key, id);
    }
    
    public def  putLocked(id:Long, key:K, value:Cloneable):Cloneable {
        val memory = data.getMemoryUnit(key);  
        return memory.setValueLocked(value, key, id, NOT_DELETED);
    }
    
    public static def txIdToString (txId:Long) {
        val placeId =  ((txId >> 32) as Int);
        val txSeq = (txId as Int);
        return "TX["+ (placeId*100000000 + txSeq)+"] TXPLC["+placeId+"] TXSEQ["+txSeq+"]";
    }
    
    /*********************************************/
    private def statusLock(){
    	assert(resilient);
        if (!TxConfig.LOCK_FREE)
        	resilientStatusLock.lock();
    }
    
    private def statusUnlock(){
    	assert(resilient);
        if (!TxConfig.LOCK_FREE)
        	resilientStatusLock.unlock();
    }
    
}