package x10.util.resilient.localstore.tx;

import x10.util.HashMap;
import x10.util.Set;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.MasterStore;
import x10.xrx.Runtime;

public abstract class TxManager(data:MapData) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static val EARLY_ACQUIRE= 1;
    public static val LATE_ACQUIRE = 2; //CAN NOT BE USED WITH UNDO LOGGING
    
    public static val UNDO_LOGGING= 1;
    public static val WRITE_BUFFERING = 2;
    
    public static val READ_LOCKING= 1;
    public static val READ_VALIDATION = 2;
    
    
    public static val TM_DISABLED = System.getenv("TM_DISABLED") != null && Long.parseLong(System.getenv("TM_DISABLED")) == 1;
    
    //Default TM=RL_EA_UL
    public static val TM_READ = System.getenv("TM") == null || System.getenv("TM").contains("RL") ? READ_LOCKING :  READ_VALIDATION;
    public static val TM_ACQUIRE = System.getenv("TM") == null || System.getenv("TM").contains("EA") ? EARLY_ACQUIRE :  LATE_ACQUIRE;
    public static val TM_RECOVER = System.getenv("TM") == null || System.getenv("TM").contains("UL") ? UNDO_LOGGING :  WRITE_BUFFERING;
    
    public static val TM = System.getenv("TM") == null? "RL_EA_UL":System.getenv("TM");
    
    protected val logsLock = new Lock();
    protected val txLogs = new HashMap[Long,TxLog]();
    protected val abortedTxs = new ArrayList[Long](); //aborted NULL transactions 
    protected val validatedTxLogs = new HashMap[Long,TxLog]();
    
    protected val futures = new HashMap[Long,ArrayList[TxFuture]]();
    protected val futuresLock = new Lock();
    
    protected val stat:TxManagerStatistics = new TxManagerStatistics();
    
    public static def make(name:String) = make(new MapData(name));
    
    public static def make(data:MapData):TxManager {
        if (TM_DISABLED)
            return new TxManager_LockBased(data);
        else if (     TM_READ == READ_LOCKING    &&  TM_ACQUIRE == EARLY_ACQUIRE && TM_RECOVER == UNDO_LOGGING)
            return new TxManager_RL_EA_UL(data);
        else if (TM_READ == READ_LOCKING    &&  TM_ACQUIRE == EARLY_ACQUIRE && TM_RECOVER == WRITE_BUFFERING)
            return new TxManager_RL_EA_WB(data);
        else if (TM_READ == READ_LOCKING    &&  TM_ACQUIRE == LATE_ACQUIRE  && TM_RECOVER == WRITE_BUFFERING)
            return new TxManager_RL_LA_WB(data);
        else if (TM_READ == READ_VALIDATION &&  TM_ACQUIRE == EARLY_ACQUIRE && TM_RECOVER == UNDO_LOGGING)
            return new TxManager_RV_EA_UL(data);
        else if (TM_READ == READ_VALIDATION &&  TM_ACQUIRE == EARLY_ACQUIRE && TM_RECOVER == WRITE_BUFFERING)
            return new TxManager_RV_EA_WB(data);
        else if (TM_READ == READ_VALIDATION &&  TM_ACQUIRE == LATE_ACQUIRE  && TM_RECOVER == WRITE_BUFFERING)
            return new TxManager_RV_LA_WB(data);
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
            if (TM_RECOVER == WRITE_BUFFERING) {
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
    
    public def addFuture(id:Long, future:TxFuture) {
        try {
            futuresLock.lock();
            var list:ArrayList[TxFuture] = futures.getOrElse(id, null);
            if (list == null) {
                list = new ArrayList[TxFuture]();
                futures.put(id, list);
            }
            list.add(future);
        }
        finally{
            futuresLock.unlock();
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] add future ["+future.fid+"] ...");
    }
    
    protected def waitForFutures(id:Long) {
        var list:ArrayList[TxFuture] = null;
        futuresLock.lock();
        list = futures.getOrElse(id, null);
        futuresLock.unlock();
        
        if (list != null) {
            for (future in list) {
                if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] waiting for future["+future.fid+"] from place["+future.targetPlace+"] ...");
                future.waitV();
                if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] waiting for future["+future.fid+"] from place["+future.targetPlace+"] completed ...");
            }
        }
    }
    
    
    public def notifyPlaceDeath() {
        try {
            futuresLock.lock();
            val iter = futures.keySet().iterator();
            while (iter.hasNext()) {
                val txId = iter.next();
                val list = futures.getOrElse(txId, null);
                if (list != null) {
                    for (future in list) {
                        future.notifyPlaceDeath();
                    }
                }
            }
        }finally{
            futuresLock.unlock();
        }
    }
    
    public static def checkDeadCoordinator(txId:Long) {
       val placeId = (txId / MasterStore.TX_FACTOR) -1;
        if (Place(placeId).isDead()) {
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] coordinator place["+Place(placeId)+"] died !!!!");
            throw new DeadPlaceException(Place(placeId));
        }
        else
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] coordinator place["+Place(placeId)+"] is alive");
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
        if (log == null) {
            waitForFutures(id);
            return;
        }
        validate(log);
    }
    
    public def commit(id:Long) {
        logsLock.lock();
        val log = txLogs.getOrElse(id, null);
        logsLock.unlock();
        if (log == null)
            return;
        
        commit(log); 
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
            log.logInitialValue(key, copy1, ver, id);
            
            if (lockRead) {
                log.markAsLocked(key);
            }
            
            return new LogContainer(memory, log);
        }catch(ex:Exception) {
            log.unlock();
            throw ex;
        }
    }
    
    protected def abortAndThrowException(log:TxLog, ex:Exception) {
        if (log != null) {
            val s = System.nanoTime();
            abort(log);
            Console.OUT.println("Tx["+log.id+"] localAbortTime["+ (System.nanoTime()-s)/1e6+"] ");
            if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"]  " + here + "   TxManager.abortAndThrowException   throwing exception["+ex.getMessage()+"] ");
        }
        throw ex;
    }
    
    /************  Common Implementations for Get/Put/Delete/Commit/Abort/Validate ****************/
    protected def put_EA_WB(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_EA_WB started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            memory.lock(id, key); 
            log.markAsLocked(key);
            
            /*** Write Buffering ***/
            val oldValue = log.getValue(key);
            val copy1 = (value == null)? null:value.clone();
            log.logPut(key, copy1);
            
            return oldValue;//return old value
        } catch(ex:AbortedTransactionException) {
            return null;
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
            return null;
        } finally {
            if (log != null)
                log.unlock();
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_EA_WB completed");
        }
    }
    
    protected def put_RL_EA_UL(id:Long, key:String, value:Cloneable):Cloneable {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] put_RL_EA_UL started");
        var log:TxLog = null;
        try {
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** EarlyAcquire ***/
            memory.lock(id, key); 
            log.markAsLocked(key);
            
            /*** Undo Logging ***/
            val atomicV = memory.getAtomicValue(true, key, id);
            val oldValue = atomicV.value;
            val copy1 = (value == null)? null:value.clone();
            memory.setValue(copy1, key, id);
            log.markAsModified(key);
            
            return oldValue;//return old value
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
            memory.lock(id, key); 
            val atomicV = memory.getAtomicValue(true, key, id);
            
            val curVer = atomicV.version;
            val initVer = log.getInitVersion(key);
            
            val wasLocked = log.isLocked(key);
            if (!wasLocked && curVer != initVer) {
               /*another transaction have modified it and committed since we read the initial value*/
                memory.unlock(id, key);
                //don't mark it as locked, because at abort time we return the old value for locked variables. our old value is wrong.
                throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
            }
            
            log.markAsLocked(key);
            
            /*** Undo Logging ***/
            val oldValue = atomicV.value;
            val copy1 = (value == null)? null:value.clone();
            memory.setValue(copy1, key, id);
            log.markAsModified(key);
            
            return oldValue;//return old value
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
            val oldValue = log.getValue(key);
            val copy1 = (value == null)? null:value.clone();
            log.logPut(key, copy1);
            
            return oldValue;//return old value
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
            
            /*** Read Locking ***/
            log.markAsLocked(key);
            
            /*** Write Buffering ***/
            val value = log.getValue(key);
            //return a different copy to use to avoid manipulating the log or the original data
            val copy2 = (value == null)? null:value.clone();
            return copy2; 
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
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** Read Validatoin: DO NOT ACQUIRE READ LOCK HERE ***/
            
            /*** Write Buffering ***/
            val value = log.getValue(key);
            //return a different copy to use to avoid manipulating the log or the original data
            val copy2 = (value == null)? null:value.clone();
            return copy2;
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
            val cont = logInitialIfNotLogged(id, key, false);
            val memory = cont.memory;
            log = cont.log;
            
            /*** ReadValidatoin: DO NOT ACQUIRE READ LOCK HERE ***/
           
            val value =  memory.getAtomicValue(true, key, id).value;
            return value; //send a different copy to use to avoid manipulating the log or the original data
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
    
    protected def validate_RL_EA(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RL_EA started");
        try {
            waitForFutures(id);
            
            //validaiton always true
            //early acuire for write + read locking -> all impacted memory is locked by us
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally{
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RL_EA completed");
        }
    }
    
    protected def validate_RL_LA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RL_LA_WB started");
        try {
            waitForFutures(id);
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                
                //lock it even if it was locked before (it may be locked for read)
                memory.lock(id, key); //trows exception if locked by another transaction
                log.markAsLocked(key);
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
            waitForFutures(id);
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                if (!log.isLocked(key)){ //LATE Acquire
                    memory.lock(id, key); //trows exception if locked by another transaction
                    log.markAsLocked(key);
                }
                /*Read Validation*/
                val initVer = logMap.getOrThrow(key).getInitVersion();
                val curVer = memory.getAtomicValue(false, key, id).version;
                //detect write after read
                if (curVer != initVer) {
                    //Console.OUT.println(here + "   TxManager.validate_RV_LA_WB throwing conflict");
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_LA_WB completed");
        }
    }
    
    protected def validate_RV_EA_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_UL started");
        try {
            waitForFutures(id);
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                if (!log.isLocked(key)){ //lock read only keys
                    memory.lock(id, key); //trows exception if locked by another transaction
                    log.markAsLocked(key);
                }
                /*Read Validation*/
                val initVer = logMap.getOrThrow(key).getInitVersion();
                val curVer = memory.getAtomicValue(false, key, id).version;
                
                //detect write after read
                if (curVer != initVer && memory.getLockedBy() != id) {
                    //Console.OUT.println("Tx["+log.id+"] here["+here + "] TxManager.validate_RV_EA throwing conflict");
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_UL completed");
        }
    }
    
    protected def validate_RV_EA_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_WB started");
        try {
            waitForFutures(id);
            
            val logMap = log.transLog;
            val iter = logMap.keySet().iterator();
            while (iter.hasNext()) {
                val key = iter.next();
                val memory = data.getMemoryUnit(key);
                if (!log.isLocked(key)){ //lock read only keys
                    memory.lock(id, key); //trows exception if locked by another transaction
                    log.markAsLocked(key);
                }
                /*Read Validation*/
                val initVer = logMap.getOrThrow(key).getInitVersion();
                val curVer = memory.getAtomicValue(false, key, id).version;
                //detect write after read
                if (curVer != initVer) {
                    throw new ConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
                }
            }
        } catch(ex:Exception) {
            abortAndThrowException(log, ex);
        } finally {
            if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] validate_RV_EA_WB completed");
        }
    }
    
    protected def commit_WB(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_WB started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val kLog = logMap.getOrThrow(key);
            val memory = data.getMemoryUnit(key);
            if (!kLog.readOnly())
                memory.setValue(kLog.getValue(), key, log.id);
            memory.unlock(log.id, key);
        }
        stat.commitCount++;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_WB completed");
    }
    
    protected def commit_UL(log:TxLog) {
        val id = log.id;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_UL started");
        val logMap = log.transLog;
        val iter = logMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val memory = data.getMemoryUnit(key);
            memory.unlock(log.id, key);
        }
        stat.commitCount++;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit_UL completed");
    }
    
    //log must be locked
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
           
            if (kLog.isLocked()) {
                if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"]  abort_UL key "+key+" is locked readOnly?["+kLog.readOnly()+"]");
                if (kLog.readOnly()) {
                    memory.unlock(log.id, key);
                }
                else {
                    //undo
                    memory.rollbackValue(kLog.getValue(), kLog.getInitVersion(), key, log.id);
                    memory.unlock(log.id, key);
                }
            }
            else {
                if (TM_DEBUG) Console.OUT.println("Tx["+log.id+"]  abort_UL key "+key+" is NOT locked !!!!");
            }
        }
        stat.abortCount++;
        log.aborted = true;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_UL completed");
    }
    
    /** With write buffering: memory is not impacted, just unlock the locked keys*/
    //log must be locked
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
            if (kLog.isLocked())
                memory.unlock(log.id, key);
        }
        stat.abortCount++;
        log.aborted = true;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] abort_WB completed");
    }
    
    public def getState() = stat;
    
    public def resetState() {
        stat.commitCount = 0;
        stat.abortCount = 0;
    }    
    
    
    /*******   Lock based methods *********/
    public def lock_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.lock(id, key);
    }
    public def unlock_LockBased(id:Long, key:String) {
        val memory = data.getMemoryUnit(key);
        memory.unlock(id, key);
    }
    
    public def get_LockBased(id:Long, key:String):Cloneable {
        val memory = data.getMemoryUnit(key);
        return memory.getValueLocked(true, key, id);
    }
    
    public def  put_LockBased(id:Long, key:String, value:Cloneable):Cloneable{
        val memory = data.getMemoryUnit(key);
        val oldValue = memory.getValueLocked(false, key, id);
        memory.setValueLocked(value, key, id);
        return oldValue;
    }
    
    public def delete_LockBased(id:Long, key:String):Cloneable {
        val memory = data.getMemoryUnit(key);
        val oldValue = memory.getValueLocked(false, key, id);
        memory.setValueLocked(null, key, id);
        return oldValue;
    }
}

class LogContainer(memory:MemoryUnit, log:TxLog){
    
}