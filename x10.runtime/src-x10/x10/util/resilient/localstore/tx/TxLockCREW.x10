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
public class TxLockCREW extends TxLockCREWBlocking {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val readers = new HashSet[Long]();
    private val readersLock = new Lock();
    private var lockedWriter:Long = -1;
    
    public def lockRead(txId:Long, key:String) {
    	val acquired = super.tryLockRead(txId, key);
    	if (acquired) {
    		assert(lockedWriter == -1);
    		readersLock.lock();
            readers.add(txId);
            readersLock.unlock();
    	}
    	else {
    		if (resilient)
                checkDeadLockers();
            throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
    	}
    }
    
    public def unlockRead(txId:Long, key:String) {
        assert(readers.contains(txId) && lockedWriter == -1);
        super.unlockRead(txId, key);
        readersLock.lock();
        readers.remove(txId);
        readersLock.unlock();
    }

    
    public def lockWrite(txId:Long, key:String) {
    	val acquired = super.tryLockWrite(txId, key);
    	if (acquired) {
    		assert(readers.size() == 0 && (lockedWriter == -1 || lockedWriter == txId));
            lockedWriter = txId;
    	}
    	else {
    		if (resilient)
                checkDeadLockers();
            throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] key ["+key+"] ", here);
    	}
    }
    
    public def unlockWrite(txId:Long, key:String) {
    	assert(readers.size() == 0 && lockedWriter == txId);
    	lockedWriter = -1;
    	super.unlockWrite(txId, key);
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