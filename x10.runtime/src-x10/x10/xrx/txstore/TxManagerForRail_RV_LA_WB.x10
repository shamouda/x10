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

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;
import x10.util.HashMap;

public class TxManagerForRail_RV_LA_WB[K] {K haszero} extends TxManagerForRail[K] {

    public def this(data:TxRail[K], immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManagerForRail_RL_EA_UL");
    }
    
    public def getLocking(id:Long, index:Long):K {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def putLocking(id:Long, index:Long, value:K) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def get(id:Long, index:Long):K {
        val log = txLogManager.getOrAddTxLog(id);
        try {
            log.lock(1);
            val added = log.getOrAddItem(index);
            val location = log.getLastUsedLocation();
            if (added)
                data.logValueAndVersion(index, location, log);
            return log.getCurrentValue(location);
        }finally {
            log.unlock(1);
        }
    }
    
    public def put(id:Long, index:Long, newValue:K):void {
        val log = txLogManager.getOrAddTxLog(id);
        try {
            log.lock(1);
            if (resilient && immediateRecovery)
                ensureActiveStatus();
            
            log.getOrAddItem(index);
            val location = log.getLastUsedLocation();
            log.logPut(location, newValue);
        }finally {
            log.unlock(1);
        }
    }
    
    public def validate(id:Long):void {
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1)
            return;
        try {
            log.lock(1);
            val writeTx = log.validateRV_LA_WB(data);
            if (writeTx) {
                if (resilient && immediateRecovery)
                    ensureActiveStatus();
                log.writeValidated = true;
            }
        } catch (ex:Exception) {
            log.abortRV_LA_WB(data);
            txLogManager.deleteAbortedTxLog(log);
        } finally {
            log.unlock(1);
        }
    }
    
    public def commit(id:Long):void {
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1)
            return;
        try {
            log.lock(1);
            log.commitRV_LA_WB(data);
        } finally {
            log.unlock(1);
        }
        txLogManager.deleteTxLog(log);
    }
    
    public def abort(id:Long):void {
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1)
            return;
        try {
            log.lock(1);
            log.abortRV_LA_WB(data);
        } finally {
            log.unlock(1);
        }
        txLogManager.deleteAbortedTxLog(log);
    }

    public def getTxCommitLog(id:Long):HashMap[Long,K] {
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1)
            return null;
        return log.getTxCommitLogRV_LA_WB();
    }
    
    public def lockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void {
        throw new Exception("operation not supported for baseline tx manager");
    }
}