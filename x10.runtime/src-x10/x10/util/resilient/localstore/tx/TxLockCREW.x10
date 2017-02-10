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

import x10.util.concurrent.Lock;
import x10.util.HashSet;

/*
 * A non-blocking concurrent read exclusive write lock for transactional management.
 * Failing to acquire the lock, results in receiving a ConflictException, or a DeadPlaceExeption.
 * A DeadPlaceException is thrown when the lock is being acquired by a dead place's transaction.
 * */
public class TxLockCREW extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val readers = new HashSet[Long]();    
    private var lockedWriter:Long = -1;
       
    private val sem = new Lock();
    
    public def tryLockRead(txId:Long, key:String)  = lockRead(txId, key);
    public def tryLockWrite(txId:Long, key:String)  = lockWrite(txId, key);
    
    public def lockRead(txId:Long, key:String) {
        try {
            lock.lock();
            if (status == UNLOCKED || status == LOCKED_READ) {  
                assert(lockedWriter == -1);
                readers.add(txId);
                status = LOCKED_READ;
            }
            else {
                if (resilient)
                    checkDeadLockers();
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public def unlockRead(txId:Long, key:String) {
        try {
            lock.lock();
            assert(status == LOCKED_READ && readers.contains(txId) && lockedWriter == -1);    
            readers.remove(txId);
            if (readers.size() == 0){
                status = UNLOCKED;
            }
        }
        finally {
            lock.unlock();
        }
    }

    
    public def lockWrite(txId:Long, key:String) {
        try {
            lock.lock();
            if (status == UNLOCKED || (status == LOCKED_WRITE && lockedWriter == txId)) {  
                assert(readers.size() == 0 && (lockedWriter == -1 || lockedWriter == txId));
                lockedWriter = txId;
                status = LOCKED_WRITE;
            }
            else {
                if (resilient)
                    checkDeadLockers();
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public def unlockWrite(txId:Long, key:String) {
    	assert(readers.size() == 0 && lockedWriter == txId);
        try {
            lock.lock();
            lockedWriter = -1;
            status = UNLOCKED;
        }
        finally {
            lock.unlock();
        }
    }
    
    private static def readersAsString(set:HashSet[Long]) {
        var s:String = "";
        val iter = set.iterator();
        while (iter.hasNext()) {
            s += iter.next() + " ";
        }
        return s;
    }
    
    private def checkDeadLockers() {
        val iter = readers.iterator();
        while (iter.hasNext()) {
            val txId = iter.next();
            TxManager.checkDeadCoordinator(txId);
        }
        if (lockedWriter != -1)
            TxManager.checkDeadCoordinator(lockedWriter);
    }
}