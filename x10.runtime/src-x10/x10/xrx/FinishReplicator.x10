/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Sara Salem Hamouda 2017-2018.
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
import x10.util.resilient.concurrent.ResilientLowLevelFinish;

public final class FinishReplicator {
    //the set of all masters
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState]();
    
    //the set of all backups
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState]();
    
    //backup deny list
    private static val backupDeny = new HashSet[BackupDenyId]();
    
    private static val allDead = new HashSet[Int]();

    private static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    
    private static val pending = new AtomicInteger(0n);
    
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //backup place mapping
    private static val backupMap = new HashMap[Int, Int]();
    
    //master place mapping
    private static val masterMap = new HashMap[Int, Int]();
    
    //places other than the left place can use this place to create backup
    //we must deny new buckup creations from the dead source place
    protected static struct BackupDenyId(parentId:FinishResilient.Id, src:Int) {
        public def toString() = "<backupDenyId parentId=" + parentId + " src=" + src + ">";
        def this(parentId:FinishResilient.Id, src:Int) {
            property(parentId, src);
        }
    }
    
    /**************** Replication Protocol **********************/
    public static def exec(req:FinishRequest):FinishResilient.ReplicatorResponse {
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() called");
        val c = pending.incrementAndGet();
        if (c < 0n)
            throw new Exception("Fatal, new finish request posted after main finish<0,0> termination ...");
        
        var submit:Boolean = false;
        var adopterId:FinishResilient.Id = FinishResilient.UNASSIGNED; //pessimistic only
        while (true) {
            try {
                val mresp:MasterResponse = masterExec(req);
                submit = mresp.submit;
                //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() masterDone =>" + " backupPlaceId = " + mresp.backupPlaceId + " submit = " + submit );
                if (mresp.backupPlaceId == -1n) {
                    debug("==== Replicator(id="+req.id+").exec() FATAL ERROR Backup = -1 ");
                    assert false : "fatal error, backup -1 means master had a fatal error before reporting its backup value";
                }
                
                if (mresp.backupChanged) {
                    updateBackupPlace(req.id.home, mresp.backupPlaceId);
                }
                
                val backupGo = ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
                if (backupGo) {
                    if (req.reqType == FinishRequest.ADD_CHILD)
                        req.parentId = mresp.parentId;
                    req.backupPlaceId = mresp.backupPlaceId;
                    req.transitSubmitDPE = mresp.transitSubmitDPE;
                    backupExec(req);
                } else {
                    //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() backupGo = false");    
                }
                
                //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").exec() returning");
                break;
            } catch (ex:MasterDied) {
                prepareRequestForNewMaster(req); // we communicate with backup to ask for the new master location
                if (!OPTIMISTIC)
                    adopterId = req.id;
                //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterDied exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterChanged) { 
                req.toAdopter = true;
                req.id = ex.newMasterId;
                req.masterPlaceId = ex.newMasterPlace;
                if (!OPTIMISTIC)
                    adopterId = req.id;
                //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterChanged exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterMigrating) {
                //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterMigrating exception, retry later");
                //try again later
                System.threadSleep(10);
            } catch (ex:BackupDied) {
                debug("<<<< Replicator(id="+req.id+").exec() returning: ignored backup failure exception");
                break; //master should re-replicate
            }
        }
        pending.decrementAndGet();
        FinishRequest.deallocReq(req);
        return FinishResilient.ReplicatorResponse(submit, adopterId);
    }
    
    public static def masterExec(req:FinishRequest) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        
        /**AT_FINISH HACK**/
        if (req.id == FinishResilient.Id(0n,0n) && req.toAdopter) {
            return new MasterResponse();
        }
        
        if (req.isMasterLocal()) {
            val parent = findMaster(req.id);
            
            assert (parent != null) : here + " fatal error, master(id="+req.id+") is null";
            val resp = parent.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
            return resp;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val parent = findMaster(req.id);
                assert (parent != null) : here + " fatal error, master(id="+req.id+") is null";
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
        //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupExec(req:FinishRequest) {
        val createOk = req.reqType == FinishRequest.ADD_CHILD || /*in some cases, the parent may be transiting at the same time as its child, the child may not find the parent's backup during globalInit*/
                       req.reqType == FinishRequest.TRANSIT ||
                       req.reqType == FinishRequest.EXCP ||
                       (req.reqType == FinishRequest.TERM && req.id.home == here.id as Int);
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").backupExec called [" + req + "] createOK=" + createOk );
        if (req.isBackupLocal()) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, req.parentId, Place(req.optFinSrc), req.optFinKind);
            else
                bFin = findBackupOrThrow(req.id);
            val resp = bFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
            return resp;
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(req.backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val condGR = rCond.gr;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
                val bFin:FinishBackupState;
                if (createOk)
                    bFin = findBackupOrCreate(req.id, req.parentId, Place(req.optFinSrc), req.optFinKind);
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
        //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
        return resp;
    }

    public static def prepareRequestForNewMaster(req:FinishRequest) {
        val id = req.id;
        val initBackup = getBackupPlace(id.home);
        var curBackup:Int = initBackup;
        do {
            //NOLOG if (verbose>=1) debug(">>>> prepareRequestForNewMaster(id="+req.id+") called, trying curBackup="+curBackup );
            if (curBackup == here.id as Int) { 
                val bFin = findBackup(id);
                if (bFin != null) {
                    req.id = bFin.getNewMasterBlocking();
                    if (req.id == FinishResilient.Id(0n,0n))
                        req.masterPlaceId = 0n;  /**AT_FINISH HACK**/
                    else
                        req.masterPlaceId = bFin.getPlaceOfMaster();
                    req.toAdopter = true;
                    break;
                } else {
                }
            }
            else {
                val reqGR = new GlobalRef[FinishRequest](req);
                val backup = Place(curBackup);
                val me = here;
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
                                if (newMasterIdVar == FinishResilient.Id(0n,0n))
                                    newMasterPlaceVar = 0n; /**AT_FINISH HACK**/
                                else
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
        
        //NOLOG if (verbose>=1) debug("<<<< prepareRequestForNewMaster(id="+id+") returning, curBackup="+curBackup + " newMasterId="+req.id+",newMasterPlace="+req.masterPlaceId+",toAdopter="+req.toAdopter);
        if (curBackup != initBackup)
            updateBackupPlace(id.home, curBackup);
        if (OPTIMISTIC) //master place doesn't change for pessimistic finish
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
    
    static def updateMyBackupIfDead(newDead:HashSet[Int]) {
        try {
            val idHome = here.id as Int;
            FinishResilient.glock.lock();
            var b:Int = backupMap.getOrElse(idHome, -1n);
            if (b == -1n)
                b = ((idHome+1)%Place.numPlaces()) as Int;
            if (newDead.contains(b)) {
                b = ((b+1)%Place.numPlaces()) as Int;
                backupMap.put(idHome, b);
            }
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    static def updateBackupPlace(idHome:Int, backupPlaceId:Int) {
        try {
            FinishResilient.glock.lock();
            backupMap.put(idHome, backupPlaceId);
            //NOLOG if (verbose>=1) debug("<<<< updateBackupPlace(idHome="+idHome+",newPlace="+backupPlaceId+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def updateMasterPlace(idHome:Int, masterPlaceId:Int) {
        try {
            FinishResilient.glock.lock();
            masterMap.put(idHome, masterPlaceId);
            //NOLOG if (verbose>=1) debug("<<<< updateMasterPlace(idHome="+idHome+",newPlace="+masterPlaceId+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def countBackups(parentId:FinishResilient.Id, src:Int) {
        var count:Int = 0n;
        try {
            FinishResilient.glock.lock();
            for (e in fbackups.entries()) {
                if (e.getValue().getParentId() == parentId)
                    count++;
            }
            //no more backups under this parent should be created
            backupDeny.add(BackupDenyId(parentId, src));
            //NOLOG if (verbose>=1) debug("<<<< countBackups(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
        } finally {
            FinishResilient.glock.unlock();
        }
        return count;
    }
    
    /************** Adoption Utilities ****************/
    static def getNewDeadPlaces() {
        //NOLOG if (verbose>=1) debug(">>>> getNewDeadPlaces called");
        val newDead = new HashSet[Int]();
        if (pending.get() < 0n) {
            return newDead;
        }
        try {
            FinishResilient.glock.lock();
            for (i in 0n..((Place.numPlaces() as Int) - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    allDead.add(i);
                }
            }
          //NOLOG if (newDead.size() > 0) {
          //NOLOG         val s = new x10.util.StringBuilder();
          //NOLOG   for (d in newDead)
          //NOLOG       s.add(d + " ");
          //NOLOG   debug("<<<< getNewDeadPlaces returning, newDead= " + s.toString()); 
          //NOLOG } else {
          //NOLOG   debug("<<<< getNewDeadPlaces returning, newDead is Empty"); 
          //NOLOG }
          //NOLOG }
        } finally {
            FinishResilient.glock.unlock();
        }
        return newDead;
    }
    
    static def getImpactedMasters(newDead:HashSet[Int]) {
        //NOLOG if (verbose>=1) debug(">>>> lockAndGetImpactedMasters called");
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
          //NOLOG if (verbose>=1) {
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
        //NOLOG if (verbose>=1) debug(">>>> lockAndGetImpactedBackups called");
        val result = new HashSet[FinishBackupState]();
        try {
            FinishResilient.glock.lock();
            for (e in fbackups.entries()) {
                val id = e.getKey();
                val bFin = e.getValue();
                if ( OPTIMISTIC && newDead.contains(bFin.getPlaceOfMaster()) || 
                        !OPTIMISTIC && newDead.contains(bFin.getId().home) ) {
                    result.add(bFin);
                }
            }
            
          //NOLOG if (verbose>=1) {
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
    
    static def nominateMasterPlaceIfDead(idHome:Int) {
        var m:Int;
        var maxIter:Int = Place.numPlaces() as Int;
        var i:Int = 0n;
        try {
            FinishResilient.glock.lock();
            m = masterMap.getOrElse(idHome,-1n);
            do {
                if (m == -1n)
                    m = ((idHome - 1 + Place.numPlaces())%Place.numPlaces()) as Int;
                else
                    m = ((m - 1 + Place.numPlaces())%Place.numPlaces()) as Int;
                i++;
            } while (allDead.contains(m) && i < maxIter);
            
            if (allDead.contains(m) || i == maxIter)
                throw new Exception(here + " Fatal, couldn't nominate a new master place for idHome=" + idHome);
            masterMap.put(idHome, m);
        } finally {
            FinishResilient.glock.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< nominateMasterPlaceIfDead(idHome="+idHome+") returning m="+m);
        return m;
    }
    
    static def nominateBackupPlaceIfDead(idHome:Int) {
        var b:Int;
        var maxIter:Int = Place.numPlaces() as Int;
        var i:Int = 0n;
        try {
            FinishResilient.glock.lock();
            b = backupMap.getOrElse(idHome,-1n);
            do {
                if (b == -1n)
                    b = ((idHome + 1)%Place.numPlaces()) as Int;
                else
                    b = ((b + 1)%Place.numPlaces()) as Int;
                i++;
            } while (allDead.contains(b) && i < maxIter);
            
            if (allDead.contains(b) || i == maxIter)
                throw new Exception(here + " Fatal, couldn't nominate a new backup place for idHome=" + idHome);
            backupMap.put(idHome, b);
        } finally {
            FinishResilient.glock.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< nominateBackupPlace(idHome="+idHome+") returning b="+b);
        return b;
    }
    
    static def removeMaster(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fmasters.delete(id);
            //NOLOG if (verbose>=1) debug("<<<< removeMaster(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def removeBackup(id:FinishResilient.Id) {
        try {
            FinishResilient.glock.lock();
            fbackups.delete(id);
            //NOLOG if (verbose>=1) debug("<<<< removeBackup(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def addMaster(id:FinishResilient.Id, fs:FinishMasterState) {
        //NOLOG if (verbose>=1) debug(">>>> addMaster(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            fmasters.put(id, fs);
            //NOLOG if (verbose>=3) fs.dump();
            //NOLOG if (verbose>=1) debug("<<<< addMaster(id="+id+") returning");
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def findMaster(id:FinishResilient.Id):FinishMasterState {
        //NOLOG if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val fs = fmasters.getOrElse(id, null);
            //NOLOG if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return fs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    
    static def findBackupOrThrow(id:FinishResilient.Id):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (bs == null) {
                throw new Exception(here + "Fatal error: backup(id="+id+" not found here");
            }
            else {
                //NOLOG if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning, bs = " + bs);
                return bs;
            }
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def findBackup(id:FinishResilient.Id):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findBackup(id="+id+") called");
        try {
            FinishResilient.glock.lock();
            val bs = fbackups.getOrElse(id, null);
            //NOLOG if (verbose>=1) debug("<<<< findBackup(id="+id+") returning, bs = " + bs);
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    /*markAdopted is used in cases when the parent attempts to adopt, before the backup creation*/
    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, optSrc:Place, optKind:Int):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+", parentId="+parentId+") called ");
        try {
            FinishResilient.glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
                if (backupDeny.contains(BackupDenyId(parentId, id.home))) {
                    //NOLOG if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") failed, BackupCreationDenied");
                    throw new BackupCreationDenied();
                    //no need to handle this exception; the caller has died.
                }
                
                if (OPTIMISTIC)
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, optSrc, optKind);
                else
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                fbackups.put(id, bs);
                //NOLOG if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            }
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    static def createOptimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, optKind:Int, numActive:Long, 
            /*transit:HashMap[FinishResilient.Edge,Int],*/
            sent:HashMap[FinishResilient.Edge,Int], 
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            FinishResilient.glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                assert !backupDeny.contains(BackupDenyId(parentId,id.home)) : "must not be in denyList";
                bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, src, optKind, numActive, 
                            /*transit,*/ sent, excs, placeOfMaster);
                fbackups.put(id, bs);
                //NOLOG if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientOptimistic.OptimisticBackupState).sync(numActive, /*transit,*/ sent, excs, placeOfMaster);
                //NOLOG if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    
    static def createPessimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, numActive:Long, 
            live:HashMap[FinishResilient.Task,Int], transit:HashMap[FinishResilient.Edge,Int], 
            liveAdopted:HashMap[FinishResilient.Task,Int], transitAdopted:HashMap[FinishResilient.Edge,Int], 
            children:HashSet[FinishResilient.Id],
            excs:GrowableRail[CheckedThrowable]):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            FinishResilient.glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId, numActive, live,
                            transit, liveAdopted, transitAdopted, children, excs);
                fbackups.put(id, bs);
                //NOLOG if (verbose>=1) debug("<<<< createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientPessimistic.PessimisticBackupState).sync(numActive, live,
                            transit, liveAdopted, transitAdopted, children, excs);
                //NOLOG if (verbose>=1) debug("<<<< createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    public static def finalizeReplication() {
        //NOLOG if (verbose>=1) debug("<<<< Replicator.finalizeReplication called " );
        val numP = Place.numPlaces();
        val places = new Rail[Int](numP, -1n);
        var i:Long = 0;
        for (pl in Place.places())
            places(i++) = pl.id as Int;
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                //NOLOG if (verbose>=1) debug("==== Replicator.finalizeReplication  moving from " + here + " to " + Place(p));
                if (Place(p).isDead() || p == -1n) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Uncounted async {
                        //NOLOG if (verbose>=1) debug("==== Replicator.finalizeReplication  reached from " + gr.home + " to " + here);
                        waitForZeroPending();
                        val me = here.id as Int;
                        //NOLOG if (verbose>=1) debug("==== Replicator.finalizeReplication  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("wait_for_zero_pending_done") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        //NOLOG if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        //NOLOG if (verbose>=1) debug("<<<< Replicator.finalizeReplication returning" );
    }
    
    static def waitForZeroPending() {
        //NOLOG if (verbose>=1) debug(">>>> waitForZeroPending called" );
        while (pending.get() != 0n) {
            System.threadSleep(10); // release the CPU to more productive pursuits
        }
        pending.set(-2000n);
        //NOLOG if (verbose>=1) debug("<<<< waitForZeroPending returning" );
    }
}

class MasterDied extends Exception {}
class BackupDied extends Exception {}
class MasterMigrating extends Exception {}
class MasterChanged(newMasterId:FinishResilient.Id,newMasterPlace:Int)  extends Exception {}
class MasterAndBackupDied extends Exception {}
class BackupCreationDenied extends Exception {}
