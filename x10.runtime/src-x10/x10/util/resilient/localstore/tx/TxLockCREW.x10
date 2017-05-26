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
import x10.util.GrowableRail;

/*
 * A concurrent read exclusive write lock for transactional management.
 * Each lock records: a set of readers, one writer and one waiting writer.
 * Priority is decided based on the following criteria in order:
 *  - A transaction id is composed of a place id and a sequence, the sequence is used for deciding the priority
 *  1) Readers have the lowest priority: 
 *     a reading transaction fails to acquire the lock if there is a waiting writer, or if the current writer is older.
 *     a reading transaction waits only if the current writer has younger sequence number, and there is no waiting writers.   
 *  2) A writer waits if his sequence number is smaller than the sequence of the current writer, or smaller than current readers. Otherwise, the writer 
 *     fails to acquire the lock.
 * Failing to acquire the lock, results in receiving a ConflictException, or a DeadPlaceExeption.  A DeadPlaceException is thrown 
 * when the lock is being acquired by a dead place's transaction.
 * */
public class TxLockCREW extends TxLock {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val lock = new Lock();
    private val readers = new ReadersList();
    private var waitingWriter:Long = -1;
    private var writer:Long = -1;

    private static class ReadersList {
        val rdRail = new GrowableRail[Long](5);
        
        public def add(id:Long) {
            rdRail.add(id);
        }
        
        public def remove(id:Long) {
            val last = rdRail.size() -1;
            var indx:Long = -1;
            for (indx = 0 ; indx < rdRail.size(); indx++) {
                if (rdRail(indx) == id)
                    break;
            }
            assert (indx != -1) : "fatal lock bug, removing non-existing reader";
            //swap with last
            val tmp = rdRail(indx);
            rdRail(indx) = rdRail(last);
            rdRail(last) = tmp;
            
            rdRail.removeLast();
        }
        
        public def size() = rdRail.size();
        
        public def contains(id:Long) = rdRail.contains(id);
        
        public def get(indx:Long) = rdRail(indx);
        
    }
    
    public def lockRead(txId:Long, key:String) {
        try {
            lock.lock();
            var conflict:Boolean = true;
            if (writer == -1 && waitingWriter == -1) {
                assert(!readers.contains(txId)) : "lockRead bug, locking an already locked key";
                readers.add(txId);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockRead done");
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
                    checkDeadLockers(key);
                assert (writer != txId) : "lockRead bug, downgrade is not supported ";
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockRead CONFLICT, writer["+writer+"] waitingWriter["+waitingWriter+"] ");
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
            else if (readers.size() == 1 && readers.get(0) == txId) { 
                readers.remove(txId);
                writer = txId;
                conflict = false;
            }
            else if (readers.size() > 0 && !readers.contains(txId) && strongerThanReaders(txId)) {  
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
                    checkDeadLockers(key);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockWrite CONFLICT, writer["+writer+"] readers["+readersAsString()+"] ");
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId) + " key ["+key+"] ", here);
            }
            else
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] lockWrite done");
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] unlockRead done");
    }
    
    public def unlockWrite(txId:Long, key:String) {
        lock.lock();
        assert(readers.size() == 0 && writer == txId);
        writer = -1;
        lock.unlock();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] unlockWrite done");
    }
    
    private def readersAsString() {
        var s:String = "";
        for (var i:Long = 0; i < readers.size(); i++) {
            val rd = readers.get(i);
            s += rd + " ";
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitReaderWriterLocked started");
        try {
            Runtime.increaseParallelism("waitReaderWriterLocked");
            var count:Long = 0;
            while (waitingWriter == -1 && writer != -1 && stronger(txId, writer)) {  //waiting writers get access first
                if (resilient)
                    checkDeadLockers(key);
                lock.unlock();
                TxConfig.waitSleep();                   
                lock.lock();
                count++;
                if (count%1000 == 0) {
                    Console.OUT.println(here + " - waitReaderWriterLocked  Tx["+txId+"]  writer["+writer+"]  waitingWriter["+waitingWriter+"] readers["+readersAsString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " - waitReaderWriterLocked  Tx["+txId+"]  finished wait ...");
            }
        } finally {
            Runtime.decreaseParallelism(1n, "waitReaderWriterLocked");    
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitReaderWriterLocked completed"); 
        
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterReadersLocked started"); 
        
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        
        try {
            Runtime.increaseParallelism("waitWriterReadersLocked");
            var count:Long = 0;
            while (readers.size() > minLimit && waitingWriter == txId) {
                if (resilient)
                    checkDeadLockers(key);
                lock.unlock();
                TxConfig.waitSleep();                     
                lock.lock();
                
                count++;
                if (count%1000 == 0) {
                    Console.OUT.println(here + " - waitWriterReadersLocked  Tx["+txId+"]  writer["+writer+"]  waitingWriter["+waitingWriter+"] readers["+readersAsString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " - waitWriterReadersLocked  Tx["+txId+"]  finished wait ...");
            }
        }finally {
            Runtime.decreaseParallelism(1n, "waitWriterReadersLocked");
        }
        
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterReadersLocked completed");
        
        if (waitingWriter == txId) {
            waitingWriter = -1;
            return true;
        }
        else
            return false;
    }
    
    private def waitWriterWriterLocked(txId:Long, key:String) {        
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterWriterLocked started"); 
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        
        try {
            Runtime.increaseParallelism("waitWriterWriterLocked");
            var count:Long = 0;
            while (writer != -1 && waitingWriter == txId) {
                if (resilient)
                    checkDeadLockers(key);
                lock.unlock();
                TxConfig.waitSleep();   
                lock.lock();
                
                count++;
                if (count%1000 == 0) {
                    Console.OUT.println(here + " - waitWriterWriterLocked  Tx["+txId+"]  writer["+writer+"]  waitingWriter["+waitingWriter+"] readers["+readersAsString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " - waitWriterWriterLocked  Tx["+txId+"]  finished wait ...");
            }
        } finally {
            Runtime.decreaseParallelism(1n, "waitWriterWriterLocked");
        }
        
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK key[" + key + "] waitWriterWriterLocked completed"); 
        if (waitingWriter == txId) {
            waitingWriter = -1;
            return true;
        }
        else
            return false;
        
    }
    
    private def stronger(me:Long, other:Long) {
    	var res:Boolean = true;
    	val seq = TxConfig.getTxSequence(me);
    	val otherSeq = TxConfig.getTxSequence(other);
    	
    	if (seq == otherSeq) {
    		val placeId = TxConfig.getTxPlaceId(me);
    		val otherPlaceId = TxConfig.getTxPlaceId(other);
    		res = placeId < otherPlaceId;
    	}
    	else
    		res = (me as Int) < (other as Int);
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx[" + me + "] isStronger(other:" + other + ")? [" + res + "]  meSEQ["+ (me as Int) +"] otherSEQ["+ (other as Int) +"] ");
        return res;
    }
    
    private def strongerThanReaders(me:Long) {
        var res:Boolean = true;
    
        for (var i:Long = 0; i < readers.size(); i++) {
            val other = readers.get(i);
            res = stronger(me, other);
            if (!res)
                break;
        }
        return res;
    }
    
    private def checkDeadLockers(key:String) {
        for (var i:Long = 0; i < readers.size(); i++) {
            val txId = readers.get(i);
            TxManager.checkDeadCoordinator(txId, key);
        }
        if (writer != -1)
            TxManager.checkDeadCoordinator(writer, key);
    }
}