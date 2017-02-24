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
import x10.util.resilient.localstore.BlockingTx;

public class TransactionsList {
    public val globalTx:ArrayList[Tx];
	public val localTx:ArrayList[LocalTx];
	public val blockingTx:ArrayList[BlockingTx];

    private val listLock = new Lock();

    public def this(){
        globalTx = new ArrayList[Tx]();
        localTx = new ArrayList[LocalTx]();
        blockingTx = new ArrayList[BlockingTx]();
    }
    
    public def addLocalTx(tx:LocalTx) {
        listLock.lock();
        localTx.add(tx);
        listLock.unlock();
    }
    public def addGlobalTx(tx:Tx) {
        listLock.lock();
        globalTx.add(tx);
        listLock.unlock();
    }
    public def addBlockingTx(tx:BlockingTx) {
        listLock.lock();
        blockingTx.add(tx);
        listLock.unlock();
    }
}