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

public abstract class TxManager(data:MapData) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");

    protected val txLogs:SafeBucketHashMap[Long,TxLog];
    
    //locking
    protected val lockingTxLogs:SafeBucketHashMap[Long,LockingTxLog];
    
    public def this(data:MapData) {
    	property(data);
    	if (TxConfig.getInstance().BASELINE) {
    		txLogs = null;
    		lockingTxLogs = null;
    	}
    	else if (TxConfig.getInstance().STM ){
    	    txLogs = new SafeBucketHashMap[Long,TxLog](TxConfig.getInstance().BUCKETS_COUNT);
    	    lockingTxLogs = null;
    	}
    	else {
    	    txLogs = null;
    	    lockingTxLogs = new SafeBucketHashMap[Long,LockingTxLog](TxConfig.getInstance().BUCKETS_COUNT);
    	}
    }
    
    public static def make(data:MapData):TxManager {
    	if (TxConfig.getInstance().BASELINE )
             return new TxManager_Baseline(data);
    	else if (TxConfig.getInstance().LOCKING )
            return new TxManager_Locking(data);
        else if (TxConfig.getInstance().TM.equals("RL_EA_UL"))
            return new TxManager_RL_EA_UL(data);
        else if (TxConfig.getInstance().TM.equals("RL_EA_WB"))
            return new TxManager_RL_EA_WB(data);
        else if (TxConfig.getInstance().TM.equals("RL_LA_WB"))
            return new TxManager_RL_LA_WB(data);
        else if (TxConfig.getInstance().TM.equals("RV_EA_UL"))
            return new TxManager_RV_EA_UL(data);
        else if (TxConfig.getInstance().TM.equals("RV_EA_WB"))
            return new TxManager_RV_EA_WB(data);
        else if (TxConfig.getInstance().TM.equals("RV_LA_WB"))
            return new TxManager_RV_LA_WB(data);
        else
            throw new Exception("Wrong Tx Manager Configuration (undo logging can not be selected with late acquire");
    }
    
    /* Used in resilient mode to transfer the changes done by a transaction to the Slave.
     * We filter the TxLog object to remove the read-only keys,
     * so that we transfer only the update operations.
     * Accordingly, read-only transactions incurs no replication overhead.
     */
    public def getTxCommitLog(id:Long):HashMap[String,Cloneable] {
        val log = txLogs.getOrElseSafe(id, null);
        if (log == null || log.aborted)
            return null;
        
        try {
            log.lock();
            if (TxConfig.getInstance().TM.contains("WB")) { //write buffering
                return log.removeReadOnlyKeys();
            }
            else {
                val map = log.removeReadOnlyKeys();
                val iter = map.keySet().iterator();
                while (iter.hasNext()) {
                    val key = iter.next();
                    val memory = log.getMemoryUnit(key);
                    val atomicV = memory.getAtomicValueLocked(true, key, id);
                    map.put(key, atomicV.value);
                }
                return map;
            }
        }finally {
            log.unlock();
        }
    }
    
    public def getOrAddTxLog(id:Long) {
        var log:TxLog = null;
        try {
            txLogs.lock(id);
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] ");
            log = txLogs.getOrElseUnsafe(id, null);
            if (log == null) {
                log = new TxLog(id);
                txLogs.putUnsafe(id, log);
            }
        }
        finally{
            txLogs.unlock(id);
        }
        return log;
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
    
    public static def checkDeadCoordinator(txId:Long) {
    	val placeId = (txId >> 32) as Int;
        if (Place(placeId as Long).isDead()) {
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] " + txIdToString (txId)+ " coordinator place["+Place(placeId)+"] died !!!!");
            throw new DeadPlaceException(Place(placeId));
        }
    }
    
    /*************** Abstract Methods ****************/
    public abstract def get(id:Long, key:String):Cloneable;
    public abstract def put(id:Long, key:String, value:Cloneable):Cloneable;
    public abstract def delete(id:Long, key:String):Cloneable;
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
        val log = txLogs.getOrElseSafe(id, null);
        if (log == null)
            return;
        validate(log);
    }
    
    public def commit(id:Long) {
        val log = txLogs.getOrElseSafe(id, null);
        if (log == null)
            return;
        
        commit(log);
        
        //delete log to avoid repeated commit if a recovery commit is called
        txLogs.deleteSafe(id);
    }
    
    public def abort(id:Long) {
        /*Abort may reach before normal Tx operations, wait until we have a txLog to abort*/
        val log = txLogs.getOrElseSafe(id, null);
        if (log == null) {
            return;
        }

        try {
            log.lock();
            abort(log); 
        } finally {
            log.unlock();
        }
    }
    
    public def keySet(mapName:String, id:Long):Set[String] {
        return data.keySet(mapName);
    }
    
    /********************** Utils ***************************/
    /*throws an exception if a conflict was found*/
    protected def logInitialIfNotLogged(id:Long, key:String, lockRead:Boolean):LogContainer {
        val log = getOrAddTxLog(id);
        log.lock();
        try {
            if (log.aborted) {
                throw new AbortedTransactionException("AbortedTransactionException");
            }
            var memory:MemoryUnit = log.getMemoryUnit(key);
            if (memory == null) {
	            memory = data.getMemoryUnit(key);
	            if (lockRead)
	            	memory.lockRead(id, key);
	            
	            var atomicV:AtomicValue = null;
	            if (lockRead)
	            	atomicV = memory.getAtomicValueLocked(true, key, id);
	            else
	            	atomicV = memory.getAtomicValue(true, key, id); //will cause internal locking
	            val copy1 = atomicV.value;
	            val ver = atomicV.version;
	            log.logInitialValue(key, copy1, ver, id, lockRead, memory);
            }
            return new LogContainer(memory, log);
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
            if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"] " + txIdToString (log.id)+ " " + here + "   TxManager.abortAndThrowException   throwing exception["+ex.getMessage()+"] ");
        }
        throw ex;
    }
    
    /************  Common Implementations for Get/Put/Delete/Commit/Abort/Validate ****************/
    private def lockWriteRV(id:Long, key:String, cont:LogContainer) {
        val memory = cont.memory;
        val log = cont.log;
        var atomicV:AtomicValue = null;
        if (!log.getLockedWrite(key)) {
            //there is no need to do unlockRead, in read versioning we don't lock the keys while reading
            memory.lockWrite(id, key); 
            atomicV = memory.getAtomicValueLocked(false, key, id);

            val curVer = atomicV.version;
            val initVer = log.getInitVersion(key);
            if (curVer != initVer) {
                /*another transaction has modified it and committed since we read the initial value*/
                memory.unlockWrite(id, key);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new ConflictException("ConflictException["+here+"] Tx["+id+"] " + txIdToString (id) , here);
            }
            log.setLockedWrite(key, true);
            log.setReadOnly(key, false);
        }
        else 
            atomicV = memory.getAtomicValueLocked(false, key, id);
        
        return atomicV;
    }
    
    private def lockWriteRL(id:Long, key:String, cont:LogContainer) {
        val memory = cont.memory;
        val log = cont.log;
        
        if (!log.getLockedWrite(key)) {
        	
        	if (log.getLockedRead(key))
                log.setLockedRead(key, false);
        	
        	memory.lockWrite(id, key); //lockWrite unlockRead if upgrading fails 
        	
        	log.setLockedWrite(key, true);
            log.setReadOnly(key, false);
        }
    }
    
    protected def put_RV_EA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            lockWriteRV(id, key, cont);
            
            /*** Write Buffering ***/            
            val copy1 = (value == null)? null:value.clone();
            return log.logPut(key, copy1);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            lockWriteRL(id, key, cont);
            
            /*** Write Buffering ***/            
            val copy1 = (value == null)? null:value.clone();
            return log.logPut(key, copy1);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_UL(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** Early Acquire ***/
            lockWriteRL(id, key, cont);
            
            /*** Undo Logging ***/
            val copy1 = (value == null)? null:value.clone();
            return memory.setValueLocked(copy1, key, id);

        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RL_EA_UL completed");
        }
    }
    
    protected def put_RV_EA_UL(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            val atomicV = lockWriteRV(id, key, cont);
            
            /*** Undo Logging ***/
            val copy1 = (value == null)? null:value.clone();
            return memory.setValueLocked(copy1, key, id);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_RV_EA_UL completed");
        }
    }
    
    
    protected def put_LA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** DO NOT ACQUIRE WRITE LOCK HERE ***/
            
            /*** Write Buffering ***/
            val copy1 = (value == null)? null:value.clone();            
            log.setReadOnly(key, false);
            return log.logPut(key, copy1);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] put_LA_WB completed");
        }
    }
    
    /********************* End of put operations  *********************/
    
    protected def get_RL_UL(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL started");
        var log:TxLog = null;
        try {
            /*** Read Locking ***/
            val cont = logInitialIfNotLogged(id, key, true);
            val memory = cont.memory;
            log = cont.log;
            
            
            /*** Undo Logging ***/
            val atomicV = memory.getAtomicValueLocked(true, key, id);
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " getvv  ver["+atomicV.version+"] value["+atomicV.value+"]");
            return atomicV.value; //send a different copy to use to avoid manipulating the log or the original data
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_UL completed");
        }
    }
    
    protected def get_RL_WB(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, true);
            val memory = cont.memory;
            log = cont.log;           
            
            /*** Write Buffering ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, key); 
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RL_WB completed");
        }
    }
    
    protected def get_RV_WB(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** WriteBuffering: read value from buffer ***/
            val copy = true; //return a different copy to use to avoid manipulating the log or the original data
            return log.getValue(copy, key);            
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_WB completed");
        }
    }
    
    protected def get_RV_UL(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
           
            /*** UndoLogging: read value from memory ***/
            val copy = true; // send a different copy to use to avoid manipulating the log or the original data 
            return memory.getAtomicValue(copy, key, id).value;
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] get_RV_UL completed");
        }
    }
    
    /********************* End of get operations  *********************/
    
    protected def validate_RL_LA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB started");
        try {
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = log.getMemoryUnit(key);
                
                if (!log.getReadOnly(key)) {
                	lockWriteRL(id, key, new LogContainer( memory, log));                                   
                }
                else {
                    assert(log.getLockedRead(key));                    
                }
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally{
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RL_LA_WB completed");
        }
    }
    
    protected def validate_RV_LA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB started");
        try {
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = log.getMemoryUnit(key);
                
                if (log.getReadOnly(key))
                    memory.lockRead(id, key); 
                else
                    memory.lockWrite(id, key); 
                
                /*Read Validation*/
                val initVer = logMap.getOrThrow(key).getInitVersion();
                val curVer = memory.getAtomicValueLocked(false, key, id).version;
                //detect write after read
                if (curVer != initVer) {
                    if (log.getReadOnly(key))
                        memory.unlockRead(id, key);
                    else
                        memory.unlockWrite(id, key);
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
                
                if (log.getReadOnly(key))
                    log.setLockedRead(key, true);
                else
                    log.setLockedWrite(key, true);
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_LA_WB completed");
        }
    }
    
    protected def validate_RV_EA(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL started");
        try {
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = log.getMemoryUnit(key);
                
                //lock read only key
                if (!log.getLockedWrite(key)){
                    memory.lockRead(id, key); 
                        
                    /*Read Validation*/
                    val initVer = logMap.getOrThrow(key).getInitVersion();
                    val curVer = memory.getAtomicValueLocked(false, key, id).version;
                    
                    //detect write after read
                    if (curVer != initVer) {
                        memory.unlockRead(id, key);
                        throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                    }
                    
                    log.setLockedRead(key, true);
                }
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] validate_RV_EA_UL completed");
        }
    }
    
    /********************* End of validate operations  *********************/
    
    protected def commit_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = log.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else {
                memory.setValueLocked(kLog.getValue(), key, log.id);
                memory.unlockWrite(log.id, key);
            }
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_WB completed");
    }
    
    protected def commit_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = log.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else 
                memory.unlockWrite(log.id, key);
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] commit_UL completed");
    }
    
    /********************* End of commit operations  *********************/
    protected def abort_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL started");
        if (log.aborted) {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = log.getMemoryUnit(key);
           
            if (kLog.getLockedRead()) {
                memory.unlockRead(log.id, key);
            }
            else if (kLog.getLockedWrite()) {
                memory.rollbackValueLocked(kLog.getValue(), kLog.getInitVersion(), key, log.id);
                memory.unlockWrite(log.id, key);
            }
            else {
                if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"] " + txIdToString (id)+ " abort_UL key "+key+" is NOT locked !!!!");
            }
        }
        log.clearAborted();
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_UL completed");
    }
    
    /** With write buffering: memory is not impacted, just unlock the locked keys*/
    protected def abort_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB started");
        if (log.aborted) {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = log.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else if (kLog.getLockedWrite()) 
                memory.unlockWrite(log.id, key);
        }
        log.aborted = true;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + txIdToString (id)+ " here["+here+"] abort_WB completed");
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
    
    public def  putLocked(id:Long, key:String, value:Cloneable):Cloneable{
    	val log = getOrAddLockingTxLog(id);
    	val memory = log.memUnits.getOrElse(key, null);
    	assert (memory != null) : "locking mistake, putting a value before locking it";    
        return memory.setValueLocked(value, key, id);
    }
    
    public static def txIdToString (txId:Long) {
    	return "TXPLC["+((txId >> 32) as Int)+"] TXSEQ["+(txId as Int)+"]";
    }
}

class LogContainer(memory:MemoryUnit, log:TxLog){
    
}