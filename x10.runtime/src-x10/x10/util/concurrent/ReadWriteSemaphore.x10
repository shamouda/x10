/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

package x10.util.concurrent;

import x10.compiler.NativeClass;
import x10.compiler.Pinned;
import x10.io.Unserializable;

/**
 * <p>A concurrent read exclusive write semaphore</p>
 */
@Pinned public class ReadWriteSemaphore implements Unserializable {
    private val wrt = new Semaphore(1n);
    private val mutex = new Lock();
    private var readCount:Int = 0n;
    
    public def acquireRead() {
        mutex.lock();
        readCount ++;
        if (readCount == 1n) 
            wrt.acquire();
        mutex.unlock();
    }
    
    public def releaseRead() {
        mutex.lock();
        readCount --;
        if (readCount == 0n) 
            wrt.release();
        mutex.unlock();
    }
    
    public def acquireWrite() {
        wrt.acquire();
    }
  
    public def releaseWrite() {
        wrt.release();
    }

    public def tryAcquireRead() {
        if (!mutex.tryLock())
            return false;
        readCount ++;
        var acquired:Boolean = false;
        if (readCount == 1n) 
            acquired = wrt.tryAcquire();
        if (!acquired)
            readCount --;    
        mutex.unlock();
        return acquired;
    }
    
    public def tryAcquireWrite() {
        return wrt.tryAcquire();
    }
}
