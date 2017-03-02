package x10.util.resilient.localstore.tx;

import x10.util.HashMap;
import x10.util.Set;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.MasterStore;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;

public abstract class TxManager(data:MapData, validationRequired:Boolean, concurrencyMode:Int, WB:Boolean) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static val LOCK_BLOCKING = 0N;
    public static val LOCK_FREE = 1N;
    public static val LOCK_NON_BLOCKING = 2N;
    
    
    protected val logsLock = new Lock();
    protected val txLogs = new HashMap[Long,TxLog]();
    protected val abortedTxs = new ArrayList[Long](); //aborted NULL transactions 
    
    public static def make(name:String) = make(new MapData(name));
    
    public static def make(data:MapData, algorithm:String):TxManager {
        if (algorithm.equals("locking"))
            return new TxManager_LockBased(data, false, LOCK_BLOCKING, false);
        else if (algorithm.equals("lockfree"))
        	return new TxManager_LockFree(data, false, LOCK_FREE, false);
        else if (algorithm.equals("RL_EA_UL"))
            return new TxManager_RL_EA_UL(data, false, LOCK_NON_BLOCKING, false);
        else if (algorithm.equals("RL_EA_WB"))
            return new TxManager_RL_EA_WB(data, false, LOCK_NON_BLOCKING, true);
        else if (algorithm.equals("RL_LA_WB"))
            return new TxManager_RL_LA_WB(data, true, LOCK_NON_BLOCKING, true);
        else if (algorithm.equals("RV_EA_UL"))
            return new TxManager_RV_EA_UL(data, true, LOCK_NON_BLOCKING, false);
        else if (algorithm.equals("RV_EA_WB"))
            return new TxManager_RV_EA_WB(data, true, LOCK_NON_BLOCKING, true);
        else if (algorithm.equals("RV_LA_WB"))
            return new TxManager_RV_LA_WB(data, true, LOCK_NON_BLOCKING, true);
        else
            throw new Exception("Wrong Tx Manager Configuration (undo logging can not be selected with late acquire");
    }
    
    public def getTxCommitLog(id:Long):HashMap[String,Cloneable] {
        logsLock.lock();
        val log = txLogs.getOrElse(id, null);
        logsLock.unlock();
        
        if (log == null)
            return null;
        
        try {
            log.lock();
            if (WB) {
                return log.removeReadOnlyKeys();
            }
            else {
                val map = log.removeReadOnlyKeys();
                val iter = map.keySet().iterator();
                while (iter.hasNext()) {
                    val key = iter.next();
                    val memory = data.getMemoryUnit(key);
                    val atomicV = memory.getAtomicValue(true, key, id);
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
            logsLock.lock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abortedTxs.contains(id) = " + abortedTxs.contains(id));
            if (abortedTxs.contains(id))
                throw new AbortedTransactionException("AbortedTransactionException");
            log = txLogs.getOrElse(id, null);
            if (log == null) {
                log = new TxLog(id, data.name);
                txLogs.put(id, log);
            }
        }
        finally{
            logsLock.unlock();
        }
        return log;
    }
   
    public static def checkDeadCoordinator(txId:Long) {
        //FIXME: this does not hold when a spare place replaces a dead place
        val placeId = (txId / MasterStore.TX_FACTOR) -1;
        if (Place(placeId).isDead()) {
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] coordinator place["+Place(placeId)+"] died !!!!");
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
    /*************************************************/
    
    public def validate(id:Long) {
        logsLock.lock();
        val log = txLogs.getOrElse(id, null);
        logsLock.unlock();
        if (log == null)
            return;
        validate(log);
    }
    
    public def commit(id:Long) {
        logsLock.lock();
        val log = txLogs.getOrElse(id, null);
        logsLock.unlock();
        
        if (log == null)
            return;
        
        commit(log);
        
        //delete log to avoid repeated commit if a recovery commit is called
        logsLock.lock();
        txLogs.delete(id);
        logsLock.unlock();
        
    }
    
    public def abort(id:Long) {
        var log:TxLog = null;
        try {
            /*Abort may reach before normal Tx operations, wait until we have a txLog to abort*/
            logsLock.lock();
            log = txLogs.getOrElse(id, null);
            abortedTxs.add(id);
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] added to abortedTxs ...");
            if (log == null) {
                return;
            }
        }
        finally {
            logsLock.unlock();
        }
        try {
            log.lock();
            abort(log); 
        }finally {
            log.unlock();
        }
    }
    
    public def keySet(id:Long):Set[String] {
        return data.keySet();
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
            val memory = data.getMemoryUnit(key);
            if (lockRead) {
                memory.lockRead(id, key);
            }
            val atomicV = memory.getAtomicValue(true, key, id);
            val copy1 = atomicV.value;
            val ver = atomicV.version;
            log.logInitialValue(key, copy1, ver, id, lockRead);
            
            return new LogContainer(memory, log);
        }catch(ex:Exception) {
            log.unlock();
            throw ex;
        }
    }
    
    protected def abortAndThrowException(log:TxLog, ex:Exception) {
        if (log != null) {
            abort(log);
            if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"]  " + here + "   TxManager.abortAndThrowException   throwing exception["+ex.getMessage()+"] ");
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
            atomicV = memory.getAtomicValue(false, key, id);

            val curVer = atomicV.version;
            val initVer = log.getInitVersion(key);
            if (curVer != initVer) {
                /*another transaction have modified it and committed since we read the initial value*/
                memory.unlockWrite(id, key);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
            }
            log.setLockedWrite(key, true);
            log.setReadOnly(key, false);
        }
        else 
            atomicV = memory.getAtomicValue(false, key, id);
        
        return atomicV;
    }
    
    private def lockWriteRL(id:Long, key:String, cont:LogContainer) {
        val memory = cont.memory;
        val log = cont.log;
        
        if (!log.getLockedWrite(key)) {
        	if (log.getLockedRead(key)) {
                memory.unlockRead(id, key);
                log.setLockedRead(key, false);
            }
            /*another writer may write during this gap, need to check the version again*/
        	
        	memory.lockWrite(id, key); 
        	
            val atomicV = memory.getAtomicValue(false, key, id);
            val curVer = atomicV.version;
            val initVer = log.getInitVersion(key);
            if (curVer != initVer) {
                /*another transaction have modified it and committed since we read the initial value*/
                memory.unlockWrite(id, key);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
            }
            
            log.setLockedWrite(key, true);
            log.setReadOnly(key, false);
        }
    }
    
    protected def put_RV_EA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RV_EA_WB started");
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RV_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RL_EA_WB started");
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RL_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_UL(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RL_EA_UL started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** Early Acquire ***/
            lockWriteRL(id, key, cont);
            
            /*** Undo Logging ***/
            val copy1 = (value == null)? null:value.clone();
            return memory.setValue(copy1, key, id);

        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RL_EA_UL completed");
        }
    }
    
    protected def put_RV_EA_UL(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RV_EA_UL started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            val atomicV = lockWriteRV(id, key, cont);
            
            /*** Undo Logging ***/
            val copy1 = (value == null)? null:value.clone();
            return memory.setValue(copy1, key, id);
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RV_EA_UL completed");
        }
    }
    
    
    protected def put_LA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_LA_WB started");
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_LA_WB completed");
        }
    }
    
    /********************* End of put operations  *********************/
    
    protected def get_RL_UL(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RL_UL started");
        var log:TxLog = null;
        try {
            /*** Read Locking ***/
            val readLocking = true;
            val cont = logInitialIfNotLogged(id, key, readLocking);
            val memory = cont.memory;
            log = cont.log;
            
            
            /*** Undo Logging ***/
            val atomicV = memory.getAtomicValue(true, key, id);
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] getvv  ver["+atomicV.version+"] value["+atomicV.value+"]");
            return atomicV.value; //send a different copy to use to avoid manipulating the log or the original data
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RL_UL completed");
        }
    }
    
    protected def get_RL_WB(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RL_WB started");
        var log:TxLog = null;
        try {
            val readLocking = true;
            val cont = logInitialIfNotLogged(id, key, readLocking);
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RL_WB completed");
        }
    }
    
    protected def get_RV_WB(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RV_WB started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            val lockRead = false;
            val cont = logInitialIfNotLogged(id, key, lockRead);
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RV_WB completed");
        }
    }
    
    protected def get_RV_UL(id:Long, key:String):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RV_UL started");
        var log:TxLog = null;
        try {
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            val lockRead = false;
            val cont = logInitialIfNotLogged(id, key, lockRead);
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] get_RV_UL completed");
        }
    }
    
    /********************* End of get operations  *********************/
    
    protected def validate_RL_LA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RL_LA_WB started");
        try {
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RL_LA_WB completed");
        }
    }
    
    protected def validate_RV_LA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_LA_WB started");
        try {
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                
                if (log.getReadOnly(key))
                    memory.lockRead(id, key); 
                else
                    memory.lockWrite(id, key); 
                
                /*Read Validation*/
                val initVer = logMap.getOrThrow(key).getInitVersion();
                val curVer = memory.getAtomicValue(false, key, id).version;
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_LA_WB completed");
        }
    }
    
    protected def validate_RV_EA(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_UL started");
        try {
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                
                //lock read only key
                if (!log.getLockedWrite(key)){
                    memory.lockRead(id, key); 
                        
                    /*Read Validation*/
                    val initVer = logMap.getOrThrow(key).getInitVersion();
                    val curVer = memory.getAtomicValue(false, key, id).version;
                    
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
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_UL completed");
        }
    }
    
    /********************* End of validate operations  *********************/
    
    protected def commit_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_WB started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = data.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else {
                memory.setValue(kLog.getValue(), key, log.id);
                memory.unlockWrite(log.id, key);
            }
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_WB completed");
    }
    
    protected def commit_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_UL started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = data.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else 
                memory.unlockWrite(log.id, key);
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_UL completed");
    }
    
    /********************* End of commit operations  *********************/
    protected def abort_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_UL started");
        if (log.aborted) {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = data.getMemoryUnit(key);
           
            if (kLog.getLockedRead()) {
                memory.unlockRead(log.id, key);
            }
            else if (kLog.getLockedWrite()) {
                memory.rollbackValue(kLog.getValue(), kLog.getInitVersion(), key, log.id);
                memory.unlockWrite(log.id, key);
            }
            else {
                if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"]  abort_UL key "+key+" is NOT locked !!!!");
            }
        }
        log.aborted = true;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_UL completed");
    }
    
    /** With write buffering: memory is not impacted, just unlock the locked keys*/
    protected def abort_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_WB started");
        if (log.aborted) {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] WARNING: an attempt to abort an already aborted transaction");
            return;
        }
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = data.getMemoryUnit(key);
            if (kLog.getLockedRead())
                memory.unlockRead(log.id, key);
            else if (kLog.getLockedWrite()) 
                memory.unlockWrite(log.id, key);
        }
        log.aborted = true;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_WB completed");
    }
    
    /********************* End of abort operations  *********************/
   
    
    /*******   Blocking Lock methods *********/
    public def lockWrite_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.lockWrite(id, key);
    }
    
    public def lockRead_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.lockRead(id, key);
    }
    
    public def unlockWrite_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.unlockWrite(id, key);
    }
    
    public def unlockRead_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.unlockRead(id, key);
    }
    
    public def get_LockBased(id:Long, key:String):Cloneable {
        val memory = data.getMemoryUnit(key);
        return memory.getValueLocked(true, key, id);
    }
    
    public def  put_LockBased(id:Long, key:String, value:Cloneable):Cloneable{
        val memory = data.getMemoryUnit(key);        
        return memory.setValueLocked(value, key, id);
    }
}

class LogContainer(memory:MemoryUnit, log:TxLog){
    
}