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

public class TxLockCREWSimple extends TxLock {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private val lock = new Lock();
    //we need to keep track of the lockers for resilience purposes, that is why we are not using a Semaphore.
    private val readers = new HashSet[Long]();
    private var writer:Long = -1;

    def readersAsString():String {
        var str:String = "";
        for (r in readers)
            str += r + ",";
        return str;
    }
    
    public def lockRead(txId:Long) {
        try {
            lock.lock();
            if (writer == -1) {
                readers.add(txId);
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockRead done, writer["+writer+"] readers["+readersAsString()+"] ");
                return;
            }
            //conflict occured
            if (resilient)
                checkDeadLockers();
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockRead CONFLICT, writer["+writer+"] readers["+readersAsString()+"] ");
            throw new TxStoreConflictException("TxStoreConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId), here);
        } finally {
            lock.unlock();
        }
    }
    
    public def lockWrite(txId:Long) {
        try {
            lock.lock();
            var conflict:Boolean = true;            
            if (readers.size() == 0 && writer == -1) { 
            	writer = txId;
                conflict = false;
            } else if (readers.size() == 1 && readers.iterator().next() == txId) { 
                readers.remove(txId);
                writer = txId;
                conflict = false;
            } else if (readers.contains(txId)) {
                readers.remove(txId);
            }
            
            if (conflict) {
                if (resilient)
                    checkDeadLockers();
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockWrite CONFLICT, writer["+writer+"] readers["+readersAsString()+"] ");
                throw new TxStoreConflictException("TxStoreConflictException["+here+"] Tx["+txId+"] " + TxManager.txIdToString(txId) , here);
            }
            else
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK lockWrite done, writer["+writer+"] readers["+readersAsString()+"]");
        } finally {
            lock.unlock();
        }
    }
    
    public def unlockRead(txId:Long) {
        lock.lock();
        readers.remove(txId);
        lock.unlock();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " TXLOCK unlockRead done");
    }
    
    public def unlockWrite(txId:Long) {
        lock.lock();
        writer = -1;
        lock.unlock();
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+ txId +"] " + TxManager.txIdToString(txId) + " here["+here+"] TXLOCK unlockWrite done");
    }
    
    public def tryLockRead(txId:Long) {
        try {
            lockRead(txId);
            return true;
        } catch (ex:Exception) {
            return false;
        }
    }
    
    public def tryLockWrite(txId:Long) {
        try {
            lockWrite(txId); 
            return true;
        } catch (ex:Exception) {
            return false;
        }
    }
    
    private def checkDeadLockers() {
        for (r in readers) {
            TxManager.checkDeadCoordinator(r);
        }
        if (writer != -1)
            TxManager.checkDeadCoordinator(writer);
    }
    
}