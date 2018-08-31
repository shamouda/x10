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

import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicInteger;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.Cloneable;

public class TxMasterStoreForRail[K] {K haszero} extends TxMasterStore[K] {
    /*Each map has an object of TxManager (same object even after failures)*/
    private val txManagerForRail:TxManagerForRail[K];
    private val size:Long;
    public def this(backupRail:HashMap[Long,K], immediateRecovery:Boolean) {
        super(immediateRecovery);
        this.txManagerForRail = TxManagerForRail.make[K](new TxRail[K](backupRail), immediateRecovery);
        this.size = backupRail.size();
    }
    
    public def this(size:Long, init:(Long)=>K, immediateRecovery:Boolean) {
        super(immediateRecovery);
        this.txManagerForRail = TxManagerForRail.make[K](new TxRail[K](size, init), immediateRecovery);
        this.size = size;
    }
    
    public def getType():Int {
        return TxLocalStore.RAIL_TYPE;
    }
    
    public def getTxCommitLog(id:Long):Any {
        return txManagerForRail.getTxCommitLog(id);
    }
    
    public def getRail(id:Long, index:Long):K {
        return txManagerForRail.get(id, index%size);
    }
    
    public def putRail(id:Long, index:Long, value:K) {
        txManagerForRail.put(id, index%size, value);
    }
    
    public def getRailLocking(id:Long, index:Long):K {
        return txManagerForRail.getLocking(id, index%size);
    }
    
    public def putRailLocking(id:Long, index:Long, value:K) {
        txManagerForRail.putLocking(id, index%size, value);
    }
    
    public def validateRail(id:Long) {
        txManagerForRail.validate(id);
    }
    
    public def commitRail(id:Long) {
        txManagerForRail.commit(id);
    }
    
    public def abortRail(id:Long) {
        txManagerForRail.abort(id);
    }
    
    public def lockAllRail(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]) {
        txManagerForRail.lockAll(id, start, opPerPlace, indices, readFlags);
    }
    
    public def unlockAllRail(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]) {
        txManagerForRail.unlockAll(id, start, opPerPlace, indices, readFlags);
    }
    
    public def isActive() {
        return txManagerForRail.isActive();
    }
    
    public def pausing() {
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.pasuing started");
    	txManagerForRail.pausing();
    	if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.pasuing completed");
    }
    
    public def paused() {
        txManagerForRail.paused();
    }
    
    public def reactivate() {
        txManagerForRail.reactivate();
    }
    
    public def waitUntilPaused() {
    	txManagerForRail.waitUntilPaused();
    }
    
    public def getDataForRecovery():Any {
        return txManagerForRail.data.getRailForRecovery();
    }
    
}
