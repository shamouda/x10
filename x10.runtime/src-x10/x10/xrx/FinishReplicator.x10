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
import x10.util.GrowableRail;

public final class FinishReplicator {
    //the set of all masters
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState]();
    
    //the set of all backups
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState]();
    
    //backup deny list
    private static val backupDeny = new HashSet[FinishResilient.Id]();
    
    private static val allDead = new HashSet[Int]();

    private static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    
    private static val place0Pending = new AtomicInteger(0n);
    
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //backup place mapping
    private static val backupMap = new HashMap[Int, Int]();
    
    //master place mapping
    private static val masterMap = new HashMap[Int, Int]();
    
    //default next place - may be updated in notifyPlaceDeath
    static val nextPlaceId = new AtomicInteger(((here.id +1)%Place.numPlaces()) as Int);

    //default previous place - may be updated in notifyPlaceDeath
    static val prevPlaceId = new AtomicInteger(((here.id -1 + Place.numPlaces())%Place.numPlaces()) as Int);

    /**************** Replication Protocol **********************/
    //FIXME: must give parent Id in all cases, because backupGetAdopter may create the backup
    //FIXME: update the backup mapping if master returned a non-default backup value
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
                
                if (mresp.backupChanged) {
                    updateBackupPlace(req.id.home, mresp.backupPlaceId);
                }
                
                val backupGo = req.id != FinishResilient.TOP_FINISH && ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
                if (backupGo) {
                    req.backupPlaceId = mresp.backupPlaceId;
                    req.transitSubmitDPE = mresp.transitSubmitDPE;
                    backupExec(req);
                } else {
                    if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() backupGo = false");    
                }
                
                if (verbose>=1) debug("<<<< Replicator(id="+req.id+").exec() returning");
                break;
            } catch (ex:MasterDied) {
                prepareRequestForNewMaster(req); // we communicate with backup to ask for the new master location
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterDied exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterChanged) { 
                req.toAdopter = true;
                req.id = ex.newMasterId;
                req.masterPlaceId = ex.newMasterPlace;
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterChanged exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterMigrating) {
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterMigrating exception, retry later");
                //try again later
                System.threadSleep(10);
            } catch (ex:BackupDied) {
                debug("<<<< Replicator(id="+req.id+").exec() returning: ignored backup failure exception");
                break; //master should re-replicate
            }
        }
        return submit;
    }
    
    public static def masterExec(req:FinishRequest) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        if (req.isMasterLocal()) {
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
        val master = Place(req.masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val parent = findMaster(req.id);
                assert (parent != null) : "fatal error, parent is null";
                val resp = parent.exec(req);
                val r_back = resp.backupPlaceId;
                val r_backChg = resp.backupChanged;
                val r_submit = resp.submit;
                val r_submitDPE = resp.transitSubmitDPE;
                val r_exp = resp.excp;
                at (condGR) @Immediate("master_exec_response") async {
                    val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                    mRes.backupPlaceId = r_back;
                    mRes.backupChanged = r_backChg;
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
        if (req.isBackupLocal()) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, req.parentId, false, Place(req.optFinSrc));
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
                    bFin = findBackupOrCreate(req.id, req.parentId, false, Place(req.optFinSrc)); // must find the backup object
                else
                    bFin = findBackupOrThrow(req.id);
                val resp = bFin.exec(req);
                val r_excp = resp.excp;
                at (condGR) @Immediate("backup_exec_response") async {
                    val bRes = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
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

    public static def prepareRequestForNewMaster(req:FinishRequest) {
        val id = req.id;
        val initBackup = getBackupPlace(id.home);
        var curBackup:Int = initBackup;
        do {
            if (verbose>=1) debug(">>>> prepareRequestForNewMaster(id="+req.id+") called, trying curBackup="+curBackup );
            if (curBackup == here.id as Int) { 
                 val bFin = findBackup(id);
                 if (bFin != null) {
                     req.id = bFin.getNewMasterBlocking();
                     req.masterPlaceId = bFin.getPlaceOfMaster();
                     req.toAdopter = true;
                     break;
                 }
            }
            else {
                val reqGR = new GlobalRef[FinishRequest](req);
                val backup = Place(curBackup);
                if (!backup.isDead()) {
                    //we cannot use Immediate activities, because this function is blocking
                    val rCond = ResilientCondition.make(backup);
                    val condGR = rCond.gr;
                    val closure = (gr:GlobalRef[Condition]) => {
                        at (backup) @Uncounted async {
                            var foundVar:Boolean = false;
                            var newMasterIdVar:FinishResilient.Id = FinishResilient.UNASSIGNED;
                            var newMasterPlaceVar:Int = -1n;
                            val bFin = findBackupOrThrow(id);
                            if (bFin != null) {
                                foundVar = true;
                                newMasterIdVar = bFin.getNewMasterBlocking();
                                newMasterPlaceVar = bFin.getPlaceOfMaster();
                            }
                            val found = foundVar;
                            val newMasterId = newMasterIdVar;
                            val newMasterPlace = newMasterPlaceVar;
                            at (condGR) @Immediate("backup_get_new_master_response") async {
                                val req = (reqGR as GlobalRef[FinishRequest]{self.home == here})();
                                if (found) {
                                    req.id = newMasterId;
                                    req.masterPlaceId = newMasterPlace;
                                    req.toAdopter = true;
                                }
                                condGR().release();
                            }
                        }
                    };
                    
                    rCond.run(closure);
                    
                    if (rCond.failed()) {
                        throw new MasterAndBackupDied();
                    }
                    rCond.forget();
                    
                    if (req.toAdopter) {
                        break;
                    }
                }
            }
            curBackup = ((curBackup + 1) % Place.numPlaces()) as Int;
        } while (initBackup != curBackup);
        if (!req.toAdopter)
            throw new Exception("Fatal exception, cannot find backup for id=" + id);
        
        if (verbose>=1) debug("<<<< prepareRequestForNewMaster(id="+id+") returning, curBackup="+curBackup + " newMasterId="+req.id+",newMasterPlace="+req.masterPlaceId+",toAdopter="+req.toAdopter);
        if (curBackup != initBackup)
            updateBackupPlace(id.home, curBackup);
        updateMasterPlace(req.id.home, req.masterPlaceId);
    }
    
    /***************** Utilities *********************************/
    static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    
    static def getMasterPlace(idHome:Int) {
        try {
            FinishResilient.glock.lock();
            val m = masterMap.getOrElse(idHome, -1n);
            if (m == -1n)
                return idHome;
            else
                return m;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    //FIXME: check if the place is alive, if not, search all places for backup
    static def getBackupPlace(idHome:Int) {
        try {
            FinishResilient.glock.lock();
            val b = backupMap.getOrElse(idHome, -1n);
            if (b == -1n)
                return ((idHome+1)%Place.numPlaces()) as Int;
            else
                return b;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def updateBackupPlace(idHome:Int, backupPlaceId:Int) {
        try {
            FinishResilient.glock.lock();
            backupMap.put(idHome, backupPlaceId);
            if (verbose>=1) debug("<<<< updateBackupPlace(idHome="+idHome+",newPlace="+backupPlaceId+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def updateMasterPlace(idHome:Int, masterPlaceId:Int) {
        try {
            FinishResilient.glock.lock();
            masterMap.put(idHome, masterPlaceId);
            if (verbose>=1) debug("<<<< updateMasterPlace(idHome="+idHome+",newPlace="+masterPlaceId+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
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
            if (verbose>=1) debug("<<<< countBackups(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
        } finally {
            FinishResilient.glock.unlock();
        }
        return count;
    }
    
    /************** Adoption Utilities ****************/
    static def getNewDeadPlaces() {
        if (verbose>=1) debug(">>>> getNewDeadPlaces called");
        val newDead = new HashSet[Int]();
        try {
            FinishResilient.glock.lock();
            for (i in 0n..((Place.numPlaces() as Int) - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    allDead.add(i);
                }
            }
            if (verbose>=1) {
                if (newDead.size() > 0) {
                    val s = new x10.util.StringBuilder();
                    for (d in newDead)
                        s.add(d + " ");
                    debug("<<<< getNewDeadPlaces returning, newDead= " + s.toString()); 
                } else {
                    debug("<<<< getNewDeadPlaces returning, newDead is Empty"); 
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return newDead;
    }
    
    static def getImpactedMasters(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> lockAndGetImpactedMasters called");
        val result = new HashSet[FinishMasterState]();
        try {
            FinishResilient.glock.lock();
            for (e in fmasters.entries()) {
                val id = e.getKey();
                val mFin = e.getValue();
                if (mFin.isImpactedByDeadPlaces(newDead)) {
                    result.add(mFin);
                }
            }
            if (verbose>=1) {
                if (result.size() > 0) {
                    val s = new x10.util.StringBuilder();
                    for (m in result)
                        s.add(m.getId() + " ");
                    debug("<<<< lockAndGetImpactedMasters returning, masters= " + s.toString());    
                } else {
                    debug("<<<< lockAndGetImpactedMasters returning, masters is Empty");    
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return result;
    }
    
    static def getImpactedBackups(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> lockAndGetImpactedBackups called");
        val result = new HashSet[FinishBackupState]();
        try {
            FinishResilient.glock.lock();
            for (e in fbackups.entries()) {
                val id = e.getKey();
                val bFin = e.getValue();
                if (newDead.contains(bFin.getPlaceOfMaster())) {
                    result.add(bFin);
                }
            }
            if (verbose>=1) {
                if (result.size() > 0) {
                    val s = new x10.util.StringBuilder();
                    for (b in result)
                        s.add(b.getId() + " ");
                    debug("<<<< lockAndGetImpactedBackups returning, backups= " + s.toString());    
                } else {
                    debug("<<<< lockAndGetImpactedBackups returning, backups is Empty");    
                }
            }
        } finally {
            FinishResilient.glock.unlock();
        }
        return result;
    }
    
    static def nominateMasterPlace(deadHome:Int) {
        var m:Int;
        try {
            FinishResilient.glock.lock();
            m = masterMap.getOrElse(deadHome,-1n);
            if (m == -1n || m == deadHome) {
                m = ((deadHome - 1 + Place.numPlaces())%Place.numPlaces()) as Int;
                masterMap.put(deadHome, m);
            }
            
        } finally {
            FinishResilient.glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateMasterPlace(deadHome="+deadHome+") returning m="+m);
        return m;
    }
    
    static def nominateBackupPlace(deadHome:Int) {
        var b:Int;
        try {
            FinishResilient.glock.lock();
            b = backupMap.getOrElse(deadHome,-1n);
            if (b == -1n || b == deadHome) {
                b = ((deadHome + 1)%Place.numPlaces()) as Int;
                backupMap.put(deadHome, b);
            }
            
        } finally {
            FinishResilient.glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateBackupPlace(deadHome="+deadHome+") returning b="+b);
        return b;
    }
    
    static def removeMaster(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fmasters.delete(id);
            if (verbose>=1) debug("<<<< removeMaster(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def removeBackup(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fbackups.delete(id);
            if (verbose>=1) debug("<<<< removeBackup(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def addMaster(id:FinishResilient.Id, fs:FinishMasterState) {
        if (verbose>=1) debug(">>>> addMaster(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            fmasters.put(id, fs);
            if (verbose>=3) fs.dump();
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
    
    
    static def findBackupOrThrow(id:FinishResilient.Id):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrThrow(id);
            if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning, bs = " + bs);
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def findBackup(id:FinishResilient.Id):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackup(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findBackup(id="+id+") returning, bs = " + bs);
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    /*markAdopted is used in cases when the parent attempts to adopt, before the backup creation*/
    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, markAdopted:Boolean, optSrc:Place):FinishBackupState {
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
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, optSrc);
                else {
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                    if (markAdopted)
                        bs.markAsAdopted();
                }
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
    
    static def createBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, numActive:Long, 
            transit:HashMap[FinishResilient.Edge,Int],
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int, markAdopted:Boolean):FinishBackupState {
        if (verbose>=1) debug(">>>> createBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            FinishResilient.glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                assert !backupDeny.contains(parentId) : "must not be in denyList";
                
                if (OPTIMISTIC)
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, src, numActive, 
                            transit, excs, placeOfMaster);
                else {
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                    if (markAdopted)
                        bs.markAsAdopted();
                }
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                bs.sync(numActive, transit, excs, placeOfMaster);
                if (verbose>=1) debug("<<<< createBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }

    
    //FIXME: doesn't fully fix the final termination problem.
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
    var backupChanged:Boolean = false;
}

class BackupResponse {
    var excp:Exception;
}

class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterMigrating extends Exception {}

class MasterChanged(newMasterId:FinishResilient.Id,newMasterPlace:Int)  extends Exception {}

class MasterAndBackupDied extends Exception {}
class BackupCreationDenied extends Exception {}
