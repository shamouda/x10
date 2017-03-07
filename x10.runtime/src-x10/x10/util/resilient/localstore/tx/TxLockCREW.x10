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
 * A non-blocking concurrent read exclusive write lock for transactional management.
 * Failing to acquire the lock, results in receiving a ConflictException, or a DeadPlaceExeption.
 * A DeadPlaceException is thrown when the lock is being acquired by a dead place's transaction.
 * */
public class TxLockCREW extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val WRITER_WAIT_MS = System.getenv("WRITER_WAIT_MS") == null ? 10 : Long.parseLong(System.getenv("WRITER_WAIT_MS"));
    private static val WRITER_WAIT_ITER = System.getenv("WRITER_WAIT_ITER") == null ? 3 : Long.parseLong(System.getenv("WRITER_WAIT_ITER"));
    
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val lock = new Lock();
    private val readers = new HashSet[Long]();
    private var waitingWriters:Long = 0;
    private var lockedWriter:Long = -1;
    
    public def lockRead(txId:Long, key:String) {
    	try {
    		lock.lock();
    		if (lockedWriter == -1 && waitingWriters == 0) {
    			assert(!readers.contains(txId)) : "lockRead bug, locking an already locked key";
    			readers.add(txId);
    			if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockRead done"); 
    		}
    		else {
    			 if (resilient)
   	                checkDeadLockers();
    			assert (lockedWriter != txId) : "lockRead bug, downgrade is not supported ";
   	            if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockRead CONFLICT, lockedWriter["+lockedWriter+"] waitingWriters["+waitingWriters+"] ");
   	            throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
    		}
    			
    	}
    	finally {
    		lock.unlock();
    	}
    }
    
    public def lockWrite(txId:Long, key:String) {
    	try {
    		lock.lock();
    		assert (lockedWriter != txId) : "lockWrite bug, locking an already locked key";
    		
    		var conflict:Boolean = false;
    		if (readers.size() == 0 && lockedWriter == -1) {
    			lockedWriter = txId;
    		}
    		else if (readers.size() == 1 && readers.iterator().next() == txId) { //ready to upgrade
				readers.remove(txId);
    			lockedWriter = txId;
    		}
    		else if (!readers.contains(txId) || lockedWriter != -1) {  //locked by another reader or another writer
    			conflict = true;
    		}
    		else { //locked by me and other readers
 				waitingWriters++;
				
 				if (!TxConfig.getInstance().DISABLE_INCR_PARALLELISM)
 				    Runtime.increaseParallelism();
 				
				for (var i:Int = 0n; i < WRITER_WAIT_ITER && readers.size() > 1; i++) {
					if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockWrite WAIT("+i+") lockedWriter["+lockedWriter+"] readers["+readersAsString(readers)+"] waitingWriters["+waitingWriters+"]");
					lock.unlock();
					System.threadSleep(WRITER_WAIT_MS);   				
					lock.lock();
				}
				if (!TxConfig.getInstance().DISABLE_INCR_PARALLELISM)
				    Runtime.decreaseParallelism(1n);
				
				waitingWriters--;
				
				if (lockedWriter == -1 && readers.size() == 1 && readers.iterator().next() == txId ) { //ready to upgrade
					assert(readers.iterator().next() == txId) : "lockWrite bug, wrong reader found";
					readers.remove(txId);
        			lockedWriter = txId;
				}
				else {
					conflict = true;
					readers.remove(txId);
					if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockWrite CONFLICT AFTERWAIT, lockedWriter["+lockedWriter+"] readers["+readersAsString(readers)+"] waitingWriters["+waitingWriters+"]");
				}
    		}
    		
    		if (conflict) {
    			if (resilient)
                	checkDeadLockers();
            	if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] lockWrite CONFLICT, lockedWriter["+lockedWriter+"] readers["+readersAsString(readers)+"] ");
            	throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
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
        assert(readers.contains(txId) && lockedWriter == -1);
        readers.remove(txId);
        lock.unlock();
        if (TM_DEBUG) Console.OUT.println("Tx["+ txId +"] TXLOCK key[" + key + "] unlockRead done");
    }
    
    public def unlockWrite(txId:Long, key:String) {
    	lock.lock();
        assert(readers.size() == 0 && lockedWriter == txId);
        lockedWriter = -1;
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