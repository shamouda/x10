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

public class TxLockExclusive extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private var locked:Boolean;
    private var lockedBy:Long = -1;

    public def lockRead(txId:Long, key:String) {
        lockWrite(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
        try{
            lock.lock();
            if (locked && lockedBy != txId) {
                if (resilient)
                    TxManager.checkDeadCoordinator(lockedBy);
                //Console.OUT.println(here + "   TxLockExclusive.lockWrite   throwing conflict");
                throw new ConflictException("ConflictException["+here+"] Tx["+txId+"] ", here);
            }
            locked = true;
            lockedBy = txId;
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] locked Ex");
        }
        finally {
            lock.unlock();
        }
    }
  
    public def unlock(txId:Long, key:String) {
        try{
            lock.lock();
            assert (lockedBy == txId);
            locked = false;
            lockedBy = -1;
            if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] unlocked Ex");
        }
        finally {
            lock.unlock();
        }
    }
    
    public def getLockedBy() {
        try{
            lock.lock();
            return lockedBy;
        }
        finally {
            lock.unlock();
        }
    }
    
}