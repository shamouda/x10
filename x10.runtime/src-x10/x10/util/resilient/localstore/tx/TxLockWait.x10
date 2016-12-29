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

public class TxLockWait extends TxLock {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    private var locked:Boolean;

    public def lockRead(txId:Long, key:String) {
        lockWrite(txId, key);
    }
    
    public def lockWrite(txId:Long, key:String) {
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] waiting for lock");
        when (!locked) {
            locked = true;
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] locked");
        
    }
  
    public def unlock(txId:Long, key:String) {
        atomic locked = false;
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] key["+key+"] unlocked");
    }
    
    public def getLockedBy():Long {
        throw new Exception("lock.getLockedBy not supported for TxLockWait");
    }
    
}