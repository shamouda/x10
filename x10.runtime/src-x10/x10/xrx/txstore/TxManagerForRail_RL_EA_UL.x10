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

public class TxManagerForRail_RL_EA_UL[K] {K haszero} extends TxManagerForRail[K] {

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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] get rail["+index+"] ...");
        val log = txLogManager.getOrAddTxLog(id);
        try {
            log.lock(1);
            val added = log.getOrAddItem(index);
            val location = log.getLastUsedLocation();
            if (added)
                data.lockReadFast(id, index);
            val v = data.readLocked(index);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] get rail["+index+"] returns "+v+" ...");
            return v;
        } catch (ex:Exception) {
            if (TxConfig.TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] get rail["+index+"] failed  ex["+ex.getMessage()+"] will call abort...");
                ex.printStackTrace();
            }
            log.abortRL_EA_UL(data);
            txLogManager.deleteAbortedTxLog(log);
            throw ex;
        } finally {
            log.unlock(1);
        }
    }
    
    public def put(id:Long, index:Long, newValue:K):void {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] put rail["+index+"]="+newValue+" ...");
        val log = txLogManager.getOrAddTxLog(id);
        try {
            log.lock(1);
            if (resilient && immediateRecovery)
                ensureActiveStatus();
            val added = log.getOrAddItem(index);
            val location = log.getLastUsedLocation();
            if (!log.isLockedWrite(location)) {
            	log.setLockedRead(location, false);
            	data.lockWriteFast(id, index);
            	log.setLockedWriteAndReadOnly(location, /*locked write*/true, /*read only*/false);
            	data.logValueAndVersionLocked(index, location, log); //for aborting
            }
            data.writeLocked(index, newValue);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] put rail["+index+"]="+newValue+" done successfully...");
        } catch (ex:Exception) {
            if (TxConfig.TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] get put rail["+index+"]="+newValue+" failed  ex["+ex.getMessage()+"] will call abort...");
                ex.printStackTrace();
            }
            log.abortRL_EA_UL(data);
            txLogManager.deleteAbortedTxLog(log);
            throw ex;
        } finally {
            log.unlock(1);
        }
    }
    
    public def validate(id:Long):void {
    	throw new Exception("validate() is not needed for TxManagerForRail_RL_EA_UL");
    }
    
    public def commit(id:Long):void {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit ...");
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit failed, log not found!!!...");
            return;
        }
        try {
            log.lock(1);
            log.commitRV_LA_WB(data);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit succeeded ...");
        } finally {
            log.unlock(1);
            txLogManager.deleteTxLog(log);
        }
    }
    
    public def abort(id:Long):void {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort ...");
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort failed, log not found ...");    
            return;
        }
        try {
            log.lock(1);
            log.abortRV_LA_WB(data);
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort succeeded ...");
        } finally {
            log.unlock(1);
            txLogManager.deleteAbortedTxLog(log);
        }
    }

    public def getTxCommitLog(id:Long):HashMap[Long,K] {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] getTxCommitLog ...");
        val log = txLogManager.searchTxLog(id);
        if (log == null || log.id() == -1) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] getTxCommitLog failed, log not found !!!...");
            return null;
        }
        val l = log.getTxCommitLogRL_EA_UL(data);
        if (TxConfig.TM_DEBUG) {
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] getTxCommitLog succeeded, log = "+getCommitLogAsString(l)+" ...");
        }
        return l;
    }
    
    public def lockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, indices:Rail[Long],readFlags:Rail[Boolean]):void {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    private def getCommitLogAsString(log:HashMap[Long,K]) {
        var str:String = "";
        if (log != null && log.entries() != null) {
            for (e in log.entries()) {
                str += "("+e.getKey() + "," + e.getValue() + "):";
            }
        }
        return str;
    }
}