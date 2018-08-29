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

import x10.util.concurrent.Lock;
import x10.util.concurrent.ReadWriteSemaphore;
import x10.util.HashSet;
import x10.xrx.Runtime;
import x10.xrx.txstore.TxConfig;
import x10.util.GrowableRail;
import x10.xrx.TxStoreConflictException;
import x10.xrx.TxStoreFatalException;

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
        val rdRail = new GrowableRail[Long](TxConfig.PREALLOC_READERS);
        
        public def add(id:Long) {
            if (id < 0) 
                throw new TxStoreFatalException(here + " fatal error, adding reader with id = -1");
            rdRail.add(id);
        }
        
        public def remove(id:Long) {
            val last = rdRail.size() -1;
            var indx:Long = -1;
            for (indx = 0 ; indx < rdRail.size(); indx++) {
                if (rdRail(indx) == id)
                    break;
            }
            if (indx == rdRail.size()) 
                throw new TxStoreFatalException(here + " this["+this+"] fatal lock bug, removing non-existing reader  Tx["+id+"] ");
            //swap with last
            val tmp = rdRail(indx);
            rdRail(indx) = rdRail(last);
            rdRail(last) = tmp;
            
            rdRail.removeLast();
        }
        
        public def size() = rdRail.size();
        
        public def contains(id:Long) = rdRail.contains(id);
        
        public def get(indx:Long) = rdRail(indx);
        
        public def toString() {
            var str:String = "";
            for (var i:Long = 0; i < rdRail.size(); i++) {
                str += rdRail(i) + " ";
            }
            return str;
        }
    }
    
    public def lockRead(txId:Long) {
        try {
            lock.lock();
            var conflict:Boolean = true;
            if (writer == -1 && waitingWriter == -1) {
                if(readers.contains(txId)) 
                    throw new TxStoreFatalException (here + " this["+this+"] lockRead bug, locking an already locked Tx[" + txId + "] writer[" + writer + "] readers ["+readers.toString() + "] ");
                readers.add(txId);
                conflict = false;
            }
            else if (waitingWriter == -1 && stronger(txId, writer)) {
                if (TxConfig.MAX_LOCK_WAIT !=0 && waitReaderWriterLocked(txId)) {
                    readers.add(txId);
                    conflict = false;
                }
            }
            
            if (conflict) {
                if (resilient)
                    checkDeadLockers();
                if (writer == txId) 
                    throw new TxStoreFatalException (here + " this["+this+"] lockRead bug, downgrade is not supported Tx[" + txId + "] writer[" + writer + "] readers ["+readers.toString() + "] ");
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockRead CONFLICT, writer["+writer+"] waitingWriter["+waitingWriter+"] ");
                throw new TxStoreConflictException("ConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId), here);
            }
            else
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockRead done, writer["+writer+"] readers["+readers.toString()+"] ");              
        }
        finally {
            lock.unlock();
        }
    }
    
    public def lockWrite(txId:Long) {
        try {
            lock.lock();
            if (writer == txId)
                throw new TxStoreFatalException (here + " this["+this+"] lockWrite bug, locking an already locked Tx[" + txId + "] writer[" + writer + "] readers ["+readers.toString() + "] ");
            
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
                if (TxConfig.MAX_LOCK_WAIT !=0 && waitWriterReadersLocked(0, txId)) {
                    writer = txId;
                    conflict = false;
                }
            }
            else if (readers.size() > 0 && readers.contains(txId)) {
                if (TxConfig.MAX_LOCK_WAIT !=0 && waitWriterReadersLocked(1, txId)) {
                    readers.remove(txId);
                    writer = txId;
                    conflict = false;
                }
                else {
                    readers.remove(txId);
                }
            }           
            else if (writer != -1 && stronger(txId, writer)){ 
                if (TxConfig.MAX_LOCK_WAIT !=0 && waitWriterWriterLocked(txId)) {
                    writer = txId;
                    conflict = false;
                }
            }
            else if (waitingWriter != -1 && stronger(txId, waitingWriter)){
                if (TxConfig.MAX_LOCK_WAIT !=0 && waitWriterWriterLocked(txId)) {
                    writer = txId;
                    conflict = false;
                }
            }
            
            if (conflict) {
                if (resilient)
                    checkDeadLockers();
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockWrite CONFLICT, writer["+writer+"] readers["+readers.toString()+"] ");
                throw new TxStoreConflictException("ConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId) , here);
            }
            else
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockWrite done, writer["+writer+"] readers["+readers.toString()+"]");
        }
        finally {
            lock.unlock();
        }
    }
    
    public def unlockRead(txId:Long) {
        lock.lock();
        if (! (readers.contains(txId) && writer == -1)) {
            if (TxConfig.TM_DEBUG) Console.OUT.println(here + " this["+this+"] unlockRead bug, unlocking an unlocked Tx[" + txId + "] writer["+writer+"] readers["+readers.toString()+"] ");
            lock.unlock();
            return;
        }
        readers.remove(txId);
        lock.unlock();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK unlockRead done");
    }
    
    public def unlockWrite(txId:Long) {
        lock.lock();
        if (! (readers.size() == 0 && writer == txId) ) {
            if (TxConfig.TM_DEBUG) Console.OUT.println(here + " this["+this+"] unlockWrite bug, unlocking an unlocked Tx[" + txId + "] writer["+writer+"] readers["+readers.toString()+"] ");
            lock.unlock();
            return;
        }
        writer = -1;
        lock.unlock();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK unlockWrite done");
    }
    
    public def tryLockRead(txId:Long) { 
        lockRead(txId); 
        return true;
    }
    
    public def tryLockWrite(txId:Long) { 
        lockWrite(txId); 
        return true; 
    }
    
    /*
     * A reader waiting for readers to unlock
     * */
    private def waitReaderWriterLocked(txId:Long) {
    	if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] waitReaderWriterLocked started - this["+this+"] "); 
    	var failed:Boolean = false;
    	try {
            Runtime.increaseParallelism();
            var count:Long = 0;
            while (waitingWriter == -1 && writer != -1 && stronger(txId, writer)) {  //waiting writers get access first
                if (resilient)
                    checkDeadLockers();
                
                lock.unlock();
                TxConfig.waitSleep(); 
                lock.lock();
                count++;
                if (TxConfig.MAX_LOCK_WAIT > 0 && count > TxConfig.MAX_LOCK_WAIT) {
                	failed = true;
                	break;
                }
                if (count%1000 == 0) {
                    Console.OUT.println(here + " ["+Runtime.activity()+"] - waitReaderWriterLocked  Tx["+txId+"]  writer["+writer+", {"+TxManager.txIdToString(writer)+"} ]  waitingWriter["+waitingWriter+", {"+TxManager.txIdToString(waitingWriter)+"} ] readers["+readers.toString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " ["+Runtime.activity()+"] - waitReaderWriterLocked  Tx["+txId+"]  finished wait failed="+failed+"...");
            }
        } finally {
            Runtime.decreaseParallelism(1n);    
        }
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK waitReaderWriterLocked completed"); 
        
        if (!failed && writer == -1 && waitingWriter == -1)
            return true;
        else
            return false;
    }
    
    /*
     * A writer waiting for readers to unlock
     * minLimit:  pass 0, if you want to wait until all readers unlock
     *            pass 1, if a current reader wants to upgrade
     * */
    private def waitWriterReadersLocked(minLimit:Long, txId:Long) {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] waitWriterReadersLocked started - this["+this+"] "); 
        
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        var failed:Boolean = false;
        try {
            Runtime.increaseParallelism();
            var count:Long = 0;
            while (readers.size() > minLimit && waitingWriter == txId) {
                if (resilient)
                    checkDeadLockers();
                
                lock.unlock();
                TxConfig.waitSleep();
                lock.lock();
                count++;
                if (TxConfig.MAX_LOCK_WAIT > 0 && count > TxConfig.MAX_LOCK_WAIT) {
                	failed = true;
                	break;
                }
                if (count%1000 == 0) {
                    Console.OUT.println(here + " ["+Runtime.activity()+"] - waitWriterReadersLocked upgrade["+(minLimit == 1)+"] Tx["+txId+"]  writer["+writer+", {"+TxManager.txIdToString(writer)+"} ]  waitingWriter["+waitingWriter+", {"+TxManager.txIdToString(waitingWriter)+"} ] readers["+readers.toString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " ["+Runtime.activity()+"] - waitWriterReadersLocked upgrade["+(minLimit == 1)+"] Tx["+txId+"]   failed="+failed+"...");
            }
        }finally {
            Runtime.decreaseParallelism(1n);
        }
        
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " waitWriterReadersLocked completed");
        
        if (!failed && waitingWriter == txId) {
            waitingWriter = -1;
            return true;
        }
        else
            return false;
    }
    
    private def waitWriterWriterLocked(txId:Long) {        
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] waitWriterWriterLocked started - this["+this+"] "); 
        if (waitingWriter == -1 || stronger (txId, waitingWriter))
            waitingWriter = txId;
        else 
            return false;
        var failed:Boolean = false;
        try {
            Runtime.increaseParallelism();
            var count:Long = 0;
            while (writer != -1 && waitingWriter == txId) {
                if (resilient)
                    checkDeadLockers();
                
                lock.unlock();
                TxConfig.waitSleep();   
                lock.lock();
                count++;
                if (TxConfig.MAX_LOCK_WAIT > 0 && count > TxConfig.MAX_LOCK_WAIT) {
                	failed = true;
                	break;
                }
                if (count%1000 == 0) {
                    Console.OUT.println(here + " ["+Runtime.activity()+"] - waitWriterWriterLocked  Tx["+txId+"]  writer["+writer+"]  waitingWriter["+waitingWriter+"] readers["+readers.toString()+"] ...");
                }
            }
            if (count >= 1000) {
                Console.OUT.println(here + " ["+Runtime.activity()+"] - waitWriterWriterLocked  Tx["+txId+"]  finished wait ...");
            }
        } finally {
            Runtime.decreaseParallelism(1n);
        }
        
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK waitWriterWriterLocked completed"); 
        if (!failed && waitingWriter == txId) {
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
        //if (TxConfig.TM_DEBUG) Console.OUT.println("Tx[" + me + "] isStronger(other:" + other + ")? [" + res + "]  meSEQ["+ (me as Int) +"] otherSEQ["+ (other as Int) +"] ");
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
    
    private def checkDeadLockers() {
        for (var i:Long = 0; i < readers.size(); i++) {
            val txId = readers.get(i);
            TxManager.checkDeadCoordinator(txId);
        }
        if (writer != -1)
            TxManager.checkDeadCoordinator(writer);
    }
    
}