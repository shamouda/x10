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
 * <p>A non-blocking concurrent read exclusive write semaphore</p>
 */
@Pinned public class ReadWriteSemaphore implements Unserializable {
    private val wrt = new Semaphore(1n);
    private val mutex = new Lock();
    private var readCount:Int = 0n;
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public def tryAcquireRead(txId:Long) {
    	mutex.lock();
        readCount ++;
        var acquired:Boolean = true;
        if (readCount == 1n) 
            acquired = wrt.tryAcquire();
        if (!acquired)
            readCount --;    
        mutex.unlock();
        return acquired;
    }
    
    public def tryAcquireWrite(txId:Long) {
        val acquired = wrt.tryAcquire();
        return acquired;
    }
    
    public def releaseRead(txId:Long) {
        mutex.lock();
        readCount --;
        if (readCount == 0n) 
            wrt.release();
        mutex.unlock();
    }
    
    public def releaseWrite(txId:Long) {
        wrt.release();
    }
}
