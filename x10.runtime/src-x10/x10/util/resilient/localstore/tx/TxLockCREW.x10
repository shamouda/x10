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
import x10.util.concurrent.ReadWriteSemaphore;
import x10.util.HashSet;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.TxConfig;
/*
 * A concurrent read exclusive write lock for transactional management.
 * Each lock records: a set of readers, one writer and one waiting writer.
 * Priority is decided based on the following criteria in order:
 *  - A transaction id is composed of a place id and a sequence, the sequence is used for deciding the priority
 *  1) Readers have the lowest priority: 
 *     a reading transaction fails to acquire the lock if there is a waiting writer, or if the current writer has smaller sequence number.
 *     a reading transaction waits only if the current writer has younger sequence number, and there is no waiting writers.   
 *  2) A writer waits if his sequence number is smaller than the sequence of the current writer, or smaller than current readers. Otherwise, the writer 
 *     fails to acquire the lock.
 * Failing to acquire the lock, results in receiving a ConflictException, or a DeadPlaceExeption.  A DeadPlaceException is thrown 
 * when the lock is being acquired by a dead place's transaction.
 * */
public class TxLockCREW extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val LOCK_WAIT_MS = System.getenv("LOCK_WAIT_MS") == null ? 10 : Long.parseLong(System.getenv("LOCK_WAIT_MS"));
    
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val lock = new Lock();
    private val readers = new HashSet[Long]();
    private var waitingWriter:Long = -1;
    private var writer:Long = -1;
    
    public def lockRead(txId:Long, key:String) {
        try {
            lock.lock();
            var conflict:Boolean = true;
            if (writer == -1 && waitingWriter == -1) {
                assert(!readers.contains(txId)) : "lockRead bug, locking an already locked key";
                readers.add(txId);
                if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockRead done");
                conflict = false;
            }
            else if (waitingWriter == -1 && stronger(txId, writer)) {
                if (waitReaderWriterLocked(txId, key)) {
                    readers.add(txId);
                    conflict = false;
                }
            }
            
            if (conflict) {
                if (resilient)
                    checkDeadLockers();
                assert (writer != txId) : "lockRead bug, downgrade is not supported ";
                if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockRead CONFLICT, writer["+writer+"] waitingWriter["+waitingWriter+"] ");
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId) + " key ["+key+"] ", here);
            }
        }
        finally {
            lock.unlock();
        }
    }
    
    public def lockWrite(txId:Long, key:String) {
        try {
            lock.lock();
            assert (writer != txId) : "lockWrite bug, locking an already locked key";
            
            var conflict:Boolean = true;            
            if (readers.size() == 0 && writer == -1 && waitingWriter == -1) { 
                writer = txId;
                conflict = false;
            }
            else if (readers.size() == 1 && readers.iterator().next() == txId) { 
                readers.remove(txId);
                writer = txId;
                conflict = false;
            }
            else if (readers.size() > 0 && !readers.contains(txId) && stronger(txId, readers)) {  
                if (waitWriterReadersLocked(0, txId, key)) {
                    writer = txId;
                    conflict = false;
                }
            }
            else if (readers.size() > 0 && readers.contains(txId)) {
                if (waitWriterReadersLocked(1, txId, key)) {
                    readers.remove(txId);
                    writer = txId;
                    conflict = false;
                }
                else {
                    readers.remove(txId);
                }
            }           
            else if (writer != -1 && stronger(txId, writer)){ 
                if (waitWriterWriterLocked(txId, key)) {
                    writer = txId;
                    conflict = false;
                }
            }
            else if (waitingWriter != -1 && stronger(txId, waitingWriter)){
                if (waitWriterWriterLocked(txId, key)) {
                    writer = txId;
                    conflict = false;
                }
            }
            
            if (conflict) {
                if (resilient)
                    checkDeadLockers();
                if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockWrite CONFLICT, writer["+writer+"] readers["+readersAsString(readers)+"] ");
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId) + " key ["+key+"] ", here);
            }
            else
                if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockWrite done");
        }
        finally {
            lock.unlock();
        }
    }
    
    public def unlockRead(txId:Long, key:String) {
        lock.lock();
        assert(readers.contains(txId) && writer == -1);
        readers.remove(txId);
        lock.unlock();
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] unlockRead done");
    }
    
    public def unlockWrite(txId:Long, key:String) {
        lock.lock();
        assert(readers.size() == 0 && writer == txId);
        writer = -1;
        lock.unlock();
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] unlockWrite done");
    }
    
    private static def readersAsString(set:HashSet[Long]) {
        var s:String = "";
        val iter = set.iterator();
        while (iter.hasNext()) {
            s += iter.next() + " ";
        }
        return s;
    }
    
    public def tryLockRead(txId:Long, key:String) { 
        lockRead(txId, key); 
        return true;
    }
    
    public def tryLockWrite(txId:Long, key:String) { 
        lockWrite(txId, key); 
        return true; 
    }
    
    /*
     * A reader waiting for readers to unlock
     * */
    private def waitReaderWriterLocked(txId:Long, key:String) {
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitReaderWriterLocked started");
        Runtime.increaseParallelism();

        while (waitingWriter == -1 && writer != -1 && stronger(txId, writer)) {  //waiting writers get access first
            if (resilient)
                checkDeadLockers();
            lock.unlock();
            System.threadSleep(LOCK_WAIT_MS);                   
            lock.lock();
        }
        
        Runtime.decreaseParallelism(1n);
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitReaderWriterLocked completed"); 
        
        if (writer == -1 && waitingWriter == -1)
            return true;
        else
            return false;
    }
    
    /*
     * A writer waiting for readers to unlock
     * minLimit:  pass 0, if you want to wait until all readers unlock
     *            pass 1, if a current reader wants to upgrade
     * */
    private def waitWriterReadersLocked(minLimit:Long, txId:Long, key:String) {
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterReadersLocked started"); 
        
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        
        Runtime.increaseParallelism();
        
        while (readers.size() > minLimit && waitingWriter == txId) {
            if (resilient)
                checkDeadLockers();
            lock.unlock();
            System.threadSleep(LOCK_WAIT_MS);                   
            lock.lock();
        }
        
        Runtime.decreaseParallelism(1n);
        
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterReadersLocked completed");
        
        if (waitingWriter == txId) {
            waitingWriter = -1;
            return true;
        }
        else
            return false;
    }
    
    private def waitWriterWriterLocked(txId:Long, key:String) {
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterWriterLocked started"); 
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        
        Runtime.increaseParallelism();
            
        while (writer != -1 && waitingWriter == txId) {
            if (resilient)
                checkDeadLockers();
            lock.unlock();
            System.threadSleep(LOCK_WAIT_MS);
            lock.lock();
        }
        
        Runtime.decreaseParallelism(1n);
        
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterWriterLocked completed"); 
        if (waitingWriter == txId) {
            waitingWriter = -1;
            return true;
        }
        else
            return false;
        
    }
    
    private def stronger(me:Long, other:Long) {
    	var res:Boolean = true;
    	val seq = (me as Int);
    	val otherSeq = (other as Int);
    	
    	if (seq == otherSeq) {
    		val placeId = (me >> 32) as Int;
    		val otherPlaceId = (other >> 32) as Int;
    		res = placeId < otherPlaceId;
    	}
    	else
    		res = (me as Int) < (other as Int);
        if (TM_DEBUG) Console.OUT.println("Tx[" + me + "] isStronger(other:" + other + ")? [" + res + "]  meSEQ["+ (me as Int) +"] otherSEQ["+ (other as Int) +"] ");
        return res;
    }
    
    private def stronger(me:Long, readers:HashSet[Long]) {
        var res:Boolean = true;
    
        val iter = readers.iterator();
        while (iter.hasNext()) {
            val other = iter.next();
            res = stronger(me, other);
            if (!res)
                break;
        }
        return res;
    }
    
    private def checkDeadLockers() {
        val iter = readers.iterator();
        while (iter.hasNext()) {
            val txId = iter.next();
            TxManager.checkDeadCoordinator(txId);
        }
        if (writer != -1)
            TxManager.checkDeadCoordinator(writer);
    }
}