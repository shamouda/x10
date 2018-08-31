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

import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;
import x10.xrx.txstore.TxConfig;
import x10.util.RailUtils;
import x10.util.GrowableRail;
import x10.xrx.Runtime;
import x10.xrx.TxStoreFatalException;

public class TxLogForRail[K] {K haszero} implements x10.io.Unserializable {
    public static val INIT_VERSION_UNKNOWN = -1n;
	/*************************************************************/
	static val LONG_ITEMS_PER_INDEX = 1; //index:
	static val INT_ITEMS_PER_INDEX = 1; //initVersion
	
	static val K_ITEMS_PER_INDEX = 2; //initValue:currValue
	static val K_INIT_VALUE_INDEX = 0;
	static val K_CURRENT_VALUE_INDEX = 1;
	
	static val BOOLEAN_ITEMS_PER_INDEX = 3; //readOnly:lockedRead:lockedWrite
	static val BOOLEAN_READ_ONLY_INDEX = 0;
	static val BOOLEAN_LOCKED_READ_INDEX = 1;
	static val BOOLEAN_LOCKED_WRITE_INDEX = 2;
	
	public val longItems = new GrowableRail[Long](TxConfig.PREALLOC_TXKEYS * LONG_ITEMS_PER_INDEX);
	public val KItems = new GrowableRail[K](TxConfig.PREALLOC_TXKEYS * K_ITEMS_PER_INDEX);
	public val boolItems = new GrowableRail[Boolean](TxConfig.PREALLOC_TXKEYS * BOOLEAN_ITEMS_PER_INDEX);
	public val intItems = new GrowableRail[Int](TxConfig.PREALLOC_TXKEYS * INT_ITEMS_PER_INDEX);
	var size:Long = 0;
	/*************************************************************/
    private var id:Long = -1;
    public var aborted:Boolean = false;
    public var writeValidated:Boolean = false;
    private var busy:Boolean = false;
    private var lock:Lock;
    private var lastUsedLocation:Long;
	/*************************************************************/
    
	public def this() {
        if (!TxConfig.LOCK_FREE)
            lock = new Lock();
        else
            lock = null;
    }
    
    public def id() = id;
    
    public def setId(i:Long) {
    	id = i;
    }
    
	public def isReadOnlyTransaction() {
		for (var i:Long = 0; i < size; i++) {
			if (!boolItems(i*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX))
				return false;
		}
		return true;
    }
	
	public def getLocation(index:Long) {
		for (var i:Long = 0; i < size; i++) {
			if (longItems(i) == index)
				return i;
		}
		return -1;
	}

    public def logPut(location:Long, newValue:K) {
    	KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX) = newValue;
    	boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX) = false;
    }
    
    public def setAllWriteFlags(location:Long, locked:Boolean) {
    	boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX) = false;
    	boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX) = locked;
    }
    
    private def grow() {
        longItems.grow(longItems.capacity()+LONG_ITEMS_PER_INDEX);
        KItems.grow(KItems.capacity()+K_ITEMS_PER_INDEX);
        boolItems.grow(boolItems.capacity()+BOOLEAN_ITEMS_PER_INDEX);
        intItems.grow(intItems.capacity()+INT_ITEMS_PER_INDEX);
    }
    
    public def getOrAddItem(itemIndex:Long):Boolean {
        var added:Boolean = false;
    	var location:Long = getLocation(itemIndex);
    	if (location == -1) {
    	    added = true;
    	    if (size == longItems.capacity()) grow();
    		location = size;
    		longItems(location*LONG_ITEMS_PER_INDEX + 0) = itemIndex;
    		intItems(location*INT_ITEMS_PER_INDEX + 0) = INIT_VERSION_UNKNOWN; //initial version not known
    		boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX) = true;
    		boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX) = false;
        	boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX) = false;
        	KItems(location*K_ITEMS_PER_INDEX + K_INIT_VALUE_INDEX) = Zero.get[K]();
        	KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX) = Zero.get[K]();
    		size++;
    	}
    	lastUsedLocation = location; //check this
    	return added;
    }
    
    public def initialize(location:Long, initVersion:Int, initValue:K) {
        KItems(location*K_ITEMS_PER_INDEX + K_INIT_VALUE_INDEX) = initValue;
        KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX) = initValue;
        intItems(location*INT_ITEMS_PER_INDEX + 0) = initVersion;
    }

    public def reset() {
        id = -1;
        size = 0;
        aborted = false;
        writeValidated = false;
        lastUsedLocation = -1;
    }

    public def getCurrentValue(location:Long) {
        return KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX);
    }
    
    public def getInitValue(location:Long) {
        return KItems(location*K_ITEMS_PER_INDEX + K_INIT_VALUE_INDEX);
    }
    
    public def setLastUsedLocation(l:Long) {
    	lastUsedLocation = l;
    }
    
    public def getLastUsedLocation()  = lastUsedLocation;
    
    public def getInitVersion(location:Long) {
        return intItems(location);
    }

    //*used by Undo Logging*//
    public def getReadOnly(location:Long) {
    	return boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX) ;
    }
    
    // mark as locked for read
    public def setLockedRead(location:Long, lr:Boolean) {
    	boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX) = lr;
    }
    
    public def validateRV_LA_WB(data:TxRail[K]):Boolean {
        var writeTx:Boolean = false;
        for (var location:Long = 0; location < size; location++) {
            val index = longItems(location);
            val ro = boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX);
            val initVersion = intItems(location);
            if (ro) { //read only
                data.lockReadAndValidateVersion(id, index, initVersion);
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX) = true;
            } else {
                if (initVersion != INIT_VERSION_UNKNOWN) {
                    data.lockWriteAndValidateVersion(id, index, initVersion);
                }
                else {
                    data.lockWriteFast(id, index);
                }
                writeTx = true;
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX) = true;
            }
        }
        return writeTx;
    }
    
    
    public def abortRV_LA_WB(data:TxRail[K]) {
        for (var location:Long = 0; location < size; location++) {
            val index = longItems(location);
            if (boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX)) {
                data.unlockReadFast(id, index);
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX) = false;
            } else if (boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX)) {
                data.unlockWriteFast(id, index);
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX) = false;
            }
        }
    }
    
    public def commitRV_LA_WB(data:TxRail[K]) {
        for (var location:Long = 0; location < size; location++) {
            val index = longItems(location);
            if (boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX)) {
                data.unlockReadFast(id, index);
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_READ_INDEX) = false;
            } else if (boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX)) {
                val currValue = KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX);
                data.updateAndunlockWrite(id, index, currValue);
                boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_LOCKED_WRITE_INDEX) = false;
            }
        }
    }
    
    public def getTxCommitLogRV_LA_WB():HashMap[Long,K] {
        var result:HashMap[Long,K] = null;
        for (var location:Long = 0; location < size; location++) {
            val index = longItems(location);
            val ro = boolItems(location*BOOLEAN_ITEMS_PER_INDEX + BOOLEAN_READ_ONLY_INDEX);
            if (!ro) { //read only
                val currValue = KItems(location*K_ITEMS_PER_INDEX + K_CURRENT_VALUE_INDEX);
                if (result == null)
                    result = new HashMap[Long,K]();
                result.put(index,currValue);
            }
        }
        return result;
    }
    
    public def lock(i:Long) {
        if (!TxConfig.LOCK_FREE) {
            if (!TxConfig.BUSY_LOCK)
                lock.lock();
            else
                busyLock();
        }
    }
    
    public def unlock(i:Long) {
        if (!TxConfig.LOCK_FREE) {
            if (!TxConfig.BUSY_LOCK)
                lock.unlock();
            else 
                busyUnlock();
        }
        lastUsedLocation = -1;
    }
    
    public def busyLock() {
        var increased:Boolean = false;
        lock.lock();
        while (busy) {
            lock.unlock();
            
            if (!increased) {
                increased = true;
                Runtime.increaseParallelism();
            }            
            TxConfig.waitSleep();
            lock.lock();
        }
        busy = true;
        lock.unlock();
        if (increased)
            Runtime.decreaseParallelism(1n);
    }
    
    public def busyUnlock() {
        lock.lock();
        busy = false;
        lock.unlock();
    }
    
    public def unlock(i:Long, tmpId:Long) {
        if (!TxConfig.LOCK_FREE) {
            if (!TxConfig.BUSY_LOCK)
                lock.unlock();
            else 
                busyUnlock();
        }
        lastUsedLocation = -1;
    }
}