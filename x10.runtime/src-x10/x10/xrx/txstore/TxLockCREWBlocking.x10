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

import x10.xrx.txstore.TxConfig;
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
    private var sem:ReadWriteSemaphoreBlocking;
    public def this() {
        sem = new ReadWriteSemaphoreBlocking();
    }
    public def lockRead(txId:Long) {
        sem.acquireRead();
    }
    
    public def unlockRead(txId:Long) {
        sem.releaseRead();
    }
    
    public def lockWrite(txId:Long) {
        sem.acquireWrite();
    }
  
    public def unlockWrite(txId:Long) {
        sem.releaseWrite();
    }
    
    public def tryLockRead(txId:Long):Boolean {
        return sem.tryAcquireRead();
    }
    
    public def tryLockWrite(txId:Long):Boolean {
        return sem.tryAcquireWrite();
    }
    
}