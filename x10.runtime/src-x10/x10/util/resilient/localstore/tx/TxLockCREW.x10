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

/* Concurrent Read Exclusive Write Lock
 * To avoid write starvation, when a writer is prevented access, 
 * we stop accepting readers until a writer has successfully acquired the lock
 * */
public class TxLockCREW extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    private val readers = new HashSet[Long]();
    private var locked:Int = -1n; // -1 unlocked, 0 read lock, 1 write lock
    private var lockedWriter:Long = -1;
    private var writePhase:Boolean = false; //gets activated when a writer's request was rejected, until a writer request is accepted
    
    public def lockRead(txId:Long, key:String) {
        try{
            lock.lock();
            if (writePhase && readers.size() > 0 && readers.iterator().next() != txId){
                if (resilient)
                    checkDeadLockers();
                if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockRead conflict writephase["+writePhase+"] readersCount["+readers.size()+"] lockedBy["+readers.iterator().next()+"] allReaders["+readersAsString(readers)+"] ");
                //allow only one reader during the writePhase
                throw new ConflictException("ConflictException - WritePhase", here);
            }
            if (locked == -1n || locked == 0n){
                locked = 0n;
                readers.add(txId);
            }
            else { //writer
               if (lockedWriter != txId) {
                   if (resilient)
                       checkDeadLockers();
                   
                   if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockRead conflict lockedWriter["+lockedWriter+"] != ["+txId+"] ");
                   throw new ConflictException("ConflictException", here);
               }
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockRead succeeded readersCount["+readers.size()+"] allReaders["+readersAsString(readers)+"] ");
        }
        finally {
            lock.unlock();
        }
    }
    
    public def lockWrite(txId:Long, key:String) {
        try{
            lock.lock();
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockWrite started lockedStatus["+locked+"] ...");
            if (locked == -1n){
                locked = 1n;
                lockedWriter = txId;
            }
            else if (locked == 0n) {
                if (readers.size() == 1 && readers.iterator().next() == txId) {
                    locked = 1n;
                    lockedWriter = txId;
                    readers.remove(txId);
                }
                else {// other transactions are reading
                    writePhase = true;
                    if (resilient)
                        checkDeadLockers();
                    if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockWrite conflict set write phase ["+writePhase+"] readersCount["+readers.size()+"] allReaders["+readersAsString(readers)+"] ");
                    throw new ConflictException("ConflictException", here);
                }
            }
            else if (locked == 1n && lockedWriter != txId) {
                if (resilient)
                    checkDeadLockers();
                
                if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockWrite conflict lockedWriter["+lockedWriter+"] != txId["+txId+"] ");
                throw new ConflictException("ConflictException", here);
            }
            writePhase = false;
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] lockWrite succeeded  lockedWriter["+lockedWriter+"] locked["+locked+"] allReaders["+readersAsString(readers)+"] ");
        }
        finally {
            lock.unlock();
        }
    }
  
    //must be called only if locking was successful.
    public def unlock(txId:Long, key:String) {
        try{
            lock.lock();
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] attemp to unlock readersCount["+readers.size()+"] lockedWriter["+lockedWriter+"] locked["+locked+"]");
            if (locked == -1n)
                return;
            else if (locked == 0n) {
                readers.remove(txId);
                if (readers.size() == 0){
                    locked = -1n;
                }
            }
            else {
                //assert(lockedWriter == txId);
                locked = -1n;
                lockedWriter = -1;
            }
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] unlocked readersCount["+readers.size()+"] lockedWriter["+lockedWriter+"]");
        }
        finally {
            lock.unlock();
        }
    }
    
    public def getLockedBy():Long {
        throw new Exception("lock.getLockedBy not supported for LockCREW");
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