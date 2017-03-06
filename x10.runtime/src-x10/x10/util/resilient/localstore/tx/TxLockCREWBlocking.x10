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

package x10.util.resilient.localstore.tx;

import x10.util.concurrent.ReadWriteSemaphoreBlocking;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;
/*
 * An blocking concurrent read exclusive write lock based on a Semaphore. 
 * A thread waits until it gets read or write access
 * A runtime-thread can release the semaphore even if it is not the one that locked it.
 * A normal Lock would not allow this relaxed synchronization scheme. 
 * However, we need it to allow a remote place to lock, do some work, and come back to unlock a key.
 * When it comes back, it can use a different thread than the one used while locking the semaphor. 
 * */
public class TxLockCREWBlocking extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val BUSY_WAIT = System.getenv("LOCK_BW") == null? false : Long.parseLong(System.getenv("LOCK_BW")) == 1;
    private static val LOCK_BW_SLEEP = System.getenv("LOCK_BW_SLEEP") == null? 10 : Long.parseLong(System.getenv("LOCK_BW_SLEEP"));
    
    private var sem:ReadWriteSemaphoreBlocking;
    private var readers:Int; // 0 ... N
    private var writer:Int; //0 or 1
    private var lock:Lock;
    
    public def this() {
        if (BUSY_WAIT) {
            lock = new Lock();
            readers = 0n;
            writer = 0n;
        }
        else {
            sem = new ReadWriteSemaphoreBlocking();
        }
    }
    public def lockRead(txId:Long, key:String) {
        if (BUSY_WAIT) 
            lockReadBW(txId, key);
        else
            lockReadSem(txId, key);
    }
    
    public def unlockRead(txId:Long, key:String) {
        if (BUSY_WAIT) 
            unlockReadBW(txId, key);
        else
            unlockReadSem(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
        if (BUSY_WAIT)
            lockWriteBW(txId, key);
        else
            lockWriteSem(txId, key);
    }
  
    public def unlockWrite(txId:Long, key:String) {
        if (BUSY_WAIT)
            unlockWriteBW(txId, key);
        else
            unlockWriteSem(txId, key);
    }

    private def lockReadBW(txId:Long, key:String) {
        Runtime.increaseParallelism();
        lock.lock();
        while (writer > 0n) {
            lock.unlock();
            System.threadSleep(LOCK_BW_SLEEP);
            lock.lock();
        }
        readers++;
        lock.unlock();
        Runtime.decreaseParallelism(1n);
    }
    
    private def unlockReadBW(txId:Long, key:String) {
        lock.lock();
        readers--;
        lock.unlock();
    }
    
    private def lockWriteBW(txId:Long, key:String) {
        Runtime.increaseParallelism();
        lock.lock();
        while (readers > 0n) {
            lock.unlock();
            System.threadSleep(LOCK_BW_SLEEP);
            lock.lock();
        }
        writer++;
        lock.unlock();
        Runtime.decreaseParallelism(1n);
    }
  
    private def unlockWriteBW(txId:Long, key:String) {
        lock.lock();
        writer--;
        lock.unlock();
    }
    
    private def lockReadSem(txId:Long, key:String) {
        Runtime.increaseParallelism();
        sem.acquireRead();
        Runtime.decreaseParallelism(1n);
    }
    
    private def unlockReadSem(txId:Long, key:String) {
        sem.releaseRead();
    }
    
    private def lockWriteSem(txId:Long, key:String) {
        Runtime.increaseParallelism();
        sem.acquireWrite();
        Runtime.decreaseParallelism(1n);
    }
  
    private def unlockWriteSem(txId:Long, key:String) {
        sem.releaseWrite();
    }
    
    public def tryLockRead(txId:Long, key:String):Boolean {
        throw new UnsupportedOperationException("TxLockCREWBlocking.tryLockRead() not supported ...");
    }
    
    public def tryLockWrite(txId:Long, key:String):Boolean {
        throw new UnsupportedOperationException("TxLockCREWBlocking.tryLockWrite() not supported ...");
    }
    
}