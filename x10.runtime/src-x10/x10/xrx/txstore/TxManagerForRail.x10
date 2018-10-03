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
import x10.util.Set;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;
import x10.xrx.TxStoreConflictException;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreFatalException;
import x10.xrx.TxStoreAbortedException;
import x10.xrx.TxCommitLog;

public abstract class TxManagerForRail[K] {K haszero} {
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private static val STATUS_ACTIVE = 0;
    private static val STATUS_PAUSING = 1;
    private static val STATUS_PAUSED = 2;

	public val data:TxRail[K];
    public val immediateRecovery:Boolean;
    protected var status:Long; // 0 (not paused), 1 (preparing to pause), 2 (paused)
    private var resilientStatusLock:Lock;
    protected val txLogManager:TxLogManagerForRail[K];
    
    public def this(data:TxRail[K], immediateRecovery:Boolean) {
    	this.data = data;
    	this.immediateRecovery = immediateRecovery;
        txLogManager = new TxLogManagerForRail[K]();
        if (resilient)
        	resilientStatusLock = new Lock();
    }
    
    public static def make[K](data:TxRail[K], immediateRecovery:Boolean){K haszero}:TxManagerForRail[K] {
        if (TxConfig.TM.equals("RV_LA_WB"))
            return new TxManagerForRail_RV_LA_WB[K](data, immediateRecovery);
        else if (TxConfig.TM.equals("RL_EA_UL"))
        	return new TxManagerForRail_RL_EA_UL[K](data, immediateRecovery);
        else
            throw new Exception("Wrong Tx Manager Configuration (undo logging can not be selected with late acquire");
    }
  
    /**************   Pausing for Recovery    ****************/
    public def waitUntilPaused() {
    	if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused started ...");
        try {
            Runtime.increaseParallelism();
            while (true) {
                if (!txLogManager.activeTransactionsExist()) {
                    break;
                }
                System.threadSleep(TxConfig.MASTER_WAIT_MS);
            }
        } finally {
            Runtime.decreaseParallelism(1n);
        }
        paused();
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxMasterStore.waitUntilPaused completed ...");
    }
    
    protected def ensureActiveStatus() {
        try {
            statusLock();
            if (status != STATUS_ACTIVE)
                throw new TxStorePausedException(here + " TxMasterStore paused for recovery");
        } finally {
            statusUnlock();
        }
    }
    
    public def pausing() {
    	try {
    		statusLock();
    		assert(status == STATUS_ACTIVE);
    		status = STATUS_PAUSING;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManagerForRail changed status from STATUS_ACTIVE to STATUS_PAUSING");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def paused() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSING);
    		status = STATUS_PAUSED;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManagerForRail changed status from STATUS_PAUSING to STATUS_PAUSED");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def reactivate() {
    	try {
    		statusLock();
    		assert(status == STATUS_PAUSED);
    		status = STATUS_ACTIVE;
    		if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxManagerForRail changed status from STATUS_PAUSED to STATUS_ACTIVE");
    	} finally {
    		statusUnlock();
    	}
    }
    
    public def isActive() {
    	try {
    		statusLock();
    		return status == STATUS_ACTIVE;
    	} finally {
    		statusUnlock();
    	}
    }
    
    /*************** Abstract Methods ****************/
    public abstract def get(id:Long, index:Long):K;
    public abstract def put(id:Long, index:Long, value:K):void;
    public abstract def getLocking(id:Long, index:Long):K;
    public abstract def putLocking(id:Long, index:Long, value:K):void;
    public abstract def validate(id:Long):void ;
    public abstract def commit(id:Long):void ;
    public abstract def abort(id:Long):void;
    public def getTxCommitLog(id:Long):TxCommitLog[K] { return null; } 
    public abstract def lockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void;
    public abstract def unlockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void;
    
    /*********************************************/
    private def statusLock(){
    	assert(resilient);
        if (!TxConfig.LOCK_FREE)
        	resilientStatusLock.lock();
    }
    
    private def statusUnlock(){
    	assert(resilient);
        if (!TxConfig.LOCK_FREE)
        	resilientStatusLock.unlock();
    }
    
}