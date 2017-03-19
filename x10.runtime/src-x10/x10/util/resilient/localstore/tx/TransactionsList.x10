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

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.LocalTx;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.LockingTx;
import x10.util.resilient.localstore.TxConfig;

public class TransactionsList {
    public val globalTx:ArrayList[Tx];
    public val localTx:ArrayList[LocalTx];
    public val lockingTx:ArrayList[LockingTx];

    private val listLock:Lock;

    public def this(){
        globalTx = new ArrayList[Tx]();
        localTx = new ArrayList[LocalTx]();
        lockingTx = new ArrayList[LockingTx]();
        if (!TxConfig.getInstance().LOCK_FREE)
            listLock = new Lock();
        else
            listLock = null;
    }
    
    public def addLocalTx(tx:LocalTx) {
        lock();
        localTx.add(tx);
        unlock();
    }
    public def addGlobalTx(tx:Tx) {
        lock();
        globalTx.add(tx);
        unlock();
    }
    public def addLockingTx(tx:LockingTx) {
        lock();
        lockingTx.add(tx);
        unlock();
    }
    
    private def lock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            listLock.lock();
    }
    
    private def unlock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            listLock.unlock();
    }
}