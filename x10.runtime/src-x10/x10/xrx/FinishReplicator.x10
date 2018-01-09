/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */
package x10.xrx;

import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.compiler.Immediate;
import x10.compiler.Uncounted;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.concurrent.AtomicInteger;

public final class FinishReplicator {
    //the set of all masters
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState]();
    
    //the set of all backups
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState]();
    
    //backup deny list
    private static val backupDeny = new HashSet[FinishResilient.Id]();
    
    //backup place mapping
    private static val backupMap = new HashMap[Int, Int]();
    
    //default next place - may be updated in notifyPlaceDeath
    static val nextPlaceId = new AtomicInteger(((here.id +1)%Place.numPlaces()) as Int);

    //default previous place - may be updated in notifyPlaceDeath
    static val prevPlaceId = new AtomicInteger(((here.id -1 + Place.numPlaces())%Place.numPlaces()) as Int);
    
    static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    
    static val place0Pending = new AtomicInteger(0n);
    
    static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    static val allDead = new HashSet[Int]();
    
    static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    
    static def countBackups(parentId:FinishResilient.Id) {
        var count:Int = 0n;
        try {
            FinishResilient.glock.lock();
            for (e in fbackups.entries()) {
                if (e.getValue().getParentId() == parentId)
                    count++;
            }
            //no more backups under this parent should be created
            backupDeny.add(parentId);
        } finally {
            FinishResilient.glock.unlock();
        }
        return count;
    }
    
    static def getNewDeadPlaces() {
        val newDead = new HashSet[Int]();
        try {
            FinishResilient.glock.lock();
            for (i in 0n..((Place.numPlaces() as Int) - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    allDead.add(i);
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return newDead;
    }
    
    static def lockAndGetImpactedMasters(newDead:HashSet[Int]) {
        val result = new HashSet[FinishMasterState]();
        try {
            FinishResilient.glock.lock();
            for (e in fmasters.entries()) {
                val id = e.getKey();
                val mFin = e.getValue();
                mFin.lock();
                if (mFin.isImpactedByDeadPlaces(newDead)) {
                    result.add(mFin);
                }
                else {
                    mFin.unlock();
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return result;
    }
    
    static def lockAndGetImpactedBackups(newDead:HashSet[Int]) {
        val result = new HashSet[FinishBackupState]();
        try {
            FinishResilient.glock.lock();
            for (e in fbackups.entries()) {
                val id = e.getKey();
                val bFin = e.getValue();
                if (newDead.contains(id.home)) {
                    bFin.lock();
                    result.add(bFin);
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return result;
    }
    
    static def removeMaster(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fmasters.delete(id);
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def removeBackup(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fbackups.delete(id);
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def addMaster(id:FinishResilient.Id, fs:FinishMasterState) {
        if (verbose>=1) debug(">>>> addMaster(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            fmasters.put(id, fs);
            if (verbose>=1) debug("<<<< addMaster(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def findMaster(id:FinishResilient.Id):FinishMasterState {
        if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val fs = fmasters.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return fs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    
    /*static def releaseBackup(id:FinishResilient.Id) {
        if (verbose>=1) debug(">>>> releaseBackup(id="+id+") called ");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrThrow(id);
            bs.release();
            if (verbose>=1) debug("<<<< releaseBackup(id="+id+") returning bs="+bs);
        } finally {
            FinishResilient.glock.unlock();
        }
    }*/
    
    static def findBackupOrThrow(id:FinishResilient.Id):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrThrow(id);
            if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning");
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    /*markAdopted is used in cases when the parent attempts to adopt, before the backup creation*/
    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, markAdopted:Boolean):FinishBackupState {
        if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+", parentId="+parentId+") called ");
        try {
            FinishResilient.glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
                if (backupDeny.contains(parentId)) {
                    if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") failed, BackupCreationDenied");
                    throw new BackupCreationDenied();
                    //no need to handle this exception; the caller has died.
                }
                
                if (OPTIMISTIC)
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId);
                else
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                if (markAdopted)
                    bs.markAsAdopted();
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            }
            else
                if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, found bs="+bs);
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    //used only when master replica is found dead
    public static def getBackupPlace(masterHome:Int) {
        try {
            FinishResilient.glock.lock();
            val backup = backupMap.getOrElse (masterHome, -1n);
            if (backup == -1n) {
                return nextPlaceId.get();
            } 
            else {
                return backup;
            }
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    //FIXME: must give parent Id in all cases, because backupGetAdopter may create the backup
    public static def exec(req:FinishRequest):Boolean {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() called");
        var submit:Boolean = false;
        while (true) {
            try {
                val mresp:MasterResponse = masterExec(req);
                submit = mresp.submit;
                if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() masterDone =>"
                        + " backupPlaceId = " + mresp.backupPlaceId
                        + " submit = " + submit );
                if (mresp.backupPlaceId == -1n) {
                    debug("==== Replicator(id="+req.id+").exec() FATAL ERROR Backup = -1 ");
                    assert false : "fatal error, backup -1 means master had a fatal error before reporting its backup value";
                }
                val backupGo = req.id != FinishResilient.TOP_FINISH && ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
                if (backupGo) {
                    req.backupPlaceId = mresp.backupPlaceId;
                    req.transitSubmitDPE = mresp.transitSubmitDPE;
                    val bresp = backupExec(req);
                    if (bresp.isAdopted) {
                        req.toAdopter = true;
                        req.adopterId = bresp.adopterId;
                    }
                } else {
                    if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() backupGo = false");    
                }
                
                if (verbose>=1) debug("<<<< Replicator(id="+req.id+").exec() returning");
                break;
            } catch (ex:MasterDied) {
                val backupPlaceId = getBackupPlace(req.id.home);
                req.toAdopter = true;
                req.adopterId = backupGetAdopter(backupPlaceId, req.id);
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() forward to adopter " + req.adopterId);
            } catch (ex:BackupDied) {
                debug("<<<< Replicator(id="+req.id+").exec() returning: ignored backup failure exception");
                break; //master should re-replicate
            }
        }
        return submit;
    }
    
    public static def masterExec(req:FinishRequest) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        if (req.isLocal()) {
            val parent = findMaster(req.id);
            assert (parent != null) : "fatal error, parent is null";
            val resp = parent.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
            return resp;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.id.home);
        val rCond = ResilientCondition.make(master);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val parent = findMaster(req.id);
                assert (parent != null) : "fatal error, parent is null";
                val resp = parent.exec(req);
                val r_back = resp.backupPlaceId;
                val r_submit = resp.submit;
                val r_submitDPE = resp.transitSubmitDPE;
                val r_exp = resp.excp;
                at (condGR) @Immediate("master_exec_response") async {
                    val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                    mRes.backupPlaceId = r_back;
                    mRes.submit = r_submit;
                    mRes.transitSubmitDPE = r_submitDPE;
                    mRes.excp = r_exp;
                    condGR().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (rCond.failed()) {
            masterRes().excp = new DeadPlaceException(master);
        }
        rCond.forget();
        val resp = masterRes();
        if (resp.excp != null) { 
            if (resp.excp instanceof DeadPlaceException) {
                throw new MasterDied();
            }
            else {
                throw resp.excp;
            }
        }
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupExec(req:FinishRequest) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").backupExec called [" + req + "]" );
        val createOk = req.reqType == FinishRequest.TRANSIT ||
                       req.reqType == FinishRequest.EXCP ||
                       (req.reqType == FinishRequest.TERM && req.id.home == here.id as Int);
        if (req.backupPlaceId == here.id as Int) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, req.parentId, false);
            else
                bFin = findBackupOrThrow(req.id);
            val resp = bFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
            return resp;
        }
        if (here.id == 0) { //replication requests issued from place0
            place0Pending.incrementAndGet();
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(req.backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
                val bFin:FinishBackupState;
                if (createOk) // termination of remote messages
                    bFin = findBackupOrCreate(req.id, req.parentId, false);                 // must find the backup object
                else
                    bFin = findBackupOrThrow(req.id);
                val resp = bFin.exec(req);
                val r_isAdopt = resp.isAdopted;
                val r_adoptId = resp.adopterId;
                val r_excp = resp.excp;
                at (condGR) @Immediate("backup_exec_response") async {
                    val bRes = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                    bRes.isAdopted = r_isAdopt;
                    bRes.adopterId = r_adoptId;
                    bRes.excp = r_excp;
                    condGR().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (here.id == 0) {
            place0Pending.decrementAndGet();
        }
        
        if (rCond.failed()) {
            backupRes().excp = new DeadPlaceException(backup);
        }
        
        rCond.forget();
        val resp = backupRes();
        if (resp.excp != null) { 
            if (resp.excp instanceof DeadPlaceException)
                throw new BackupDied();
            else 
                throw resp.excp;
        }
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
        return resp;
    }

    public static def backupGetAdopter(backupPlaceId:Int, id:FinishResilient.Id):FinishResilient.Id {
        if (backupPlaceId == here.id as Int) { 
             val bFin = findBackupOrThrow(id); //FIXME: findBackupOrCreate
             return bFin.getAdopter();
        }
        else {
            val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
            val backup = Place(backupPlaceId);
            //we cannot use Immediate activities, because this function is blocking
            val rCond = ResilientCondition.make(backup);
            val condGR = rCond.gr;
            val closure = (gr:GlobalRef[Condition]) => {
                at (backup) @Uncounted async {
                    var adopterIdVar:FinishResilient.Id = FinishResilient.UNASSIGNED;
                    var exVar:Exception = null;
                    val bFin = findBackupOrThrow(id);
                    try {
                        adopterIdVar = bFin.getAdopter();
                    } catch (t:Exception) {
                        exVar = t;
                    }
                    val ex = exVar;
                    val adopterId = adopterIdVar;
                    at (condGR) @Immediate("backup_get_adopter_response") async {
                        val resp = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                        resp.isAdopted = true;
                        resp.adopterId = adopterId;
                        resp.excp = ex;
                        condGR().release();
                    }
                }
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
                backupRes().excp = new MasterAndBackupDied();
            }
            rCond.forget();
            return backupRes().adopterId;
        }
    }
    
    public static def finalizeReplication() {
        var c:Long = 0;
        if (verbose>=1) debug("<<<< Replicator.finalizeReplication called " );
        while (place0Pending.get() != 0n) {
            System.threadSleep(0); // release the CPU to more productive pursuits
            if (c++ % 1000 == 0) {
                if (verbose>=1) debug("<<<< Replicator.finalizeReplication WARNING c="+c + " p0pending="+place0Pending.get());        
            }
        }
        if (verbose>=1) debug("<<<< Replicator.finalizeReplication returning" );
    }
}

class MasterResponse {
    var backupPlaceId:Int;
    var excp:Exception;
    var submit:Boolean = false;
    var transitSubmitDPE:Boolean = false;
}

class BackupResponse {
    var isAdopted:Boolean;
    var adopterId:FinishResilient.Id;
    var excp:Exception;
}

class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterAndBackupDied extends Exception {}
class BackupCreationDenied extends Exception {}
