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

import x10.util.concurrent.UnnamedSemaphore;
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
    private val wrt = new UnnamedSemaphore(1n);
    private val mutex = new Lock();
    private var readCount:Int = 0n;
    
    public def lockRead(txId:Long, key:String) {
    	Runtime.increaseParallelism();
    	mutex.lock();
    	readCount ++;
    	if (readCount == 1n) 
    		wrt.acquire();
    	mutex.unlock();
        Runtime.decreaseParallelism(1n);
    }
    
    public def unlockRead(txId:Long, key:String) {
    	mutex.lock();
    	readCount --;
    	if (readCount == 0n) 
    		wrt.release();
    	mutex.lock();
    }
    
    public def lockWrite(txId:Long, key:String) {
        Runtime.increaseParallelism();
        wrt.acquire();
        Runtime.decreaseParallelism(1n);        
    }
  
    public def unlockWrite(txId:Long, key:String) {
    	wrt.release();
    }
}