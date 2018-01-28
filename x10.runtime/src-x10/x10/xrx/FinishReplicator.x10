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
import x10.util.concurrent.Lock;
import x10.io.Deserializer;
import x10.compiler.AsyncClosure;

public final class FinishReplicator {
    
    public static glock = new Lock(); //global lock for accessing all the static values below
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState](); //the set of all masters
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState](); //the set of all backups
    private static val backupDeny = new HashSet[BackupDenyId](); //backup deny list
    private static val allDead = new HashSet[Int](); //all discovered dead places
    private static val pending = new AtomicInteger(0n); //pending replication requests, used for final program termination
    private static val backupMap = new HashMap[Int, Int](); //backup place mapping
    private static val masterMap = new HashMap[Int, Int](); //master place mapping

    private static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //non-blocking transit
    private static val pendingMaster = new HashMap[Long,FinishRequest]();
    private static val pendingBackup = new HashMap[Long,FinishRequest]();
    private static val transitPendingAct = new HashMap[Long,PendingActivity]();
    private static val postActions = new HashMap[Long,()=>void]();
    
    protected static struct PendingActivity(dst:Long, fs:FinishState, bodyBytes:Rail[Byte], prof:x10.xrx.Runtime.Profile) {}
    private static val NULL_PENDING_ACT = PendingActivity(-1, null, null, null);
    
    //we must deny new buckup creations from the dead source place
    //places other than the given src can use this place to create a backup
    protected static struct BackupDenyId(parentId:FinishResilient.Id, src:Int) {
        public def toString() = "<backupDenyId parentId=" + parentId + " src=" + src + ">";
    }
    
    //raises a fatal error if new replication requests were received after main finish terminated
    static def checkMainTermination() {
        val c = pending.incrementAndGet();
        if (c < 0n)
            throw new Exception("Fatal, new finish request posted after main finish<0,0> termination ...");
    }
    
    /************ Non-blocking Replication Protocol ***********************/
    static def addPendingAct(id:FinishResilient.Id, num:Long, dstId:Long, fs:FinishState, bodyBytes:Rail[Byte], prof:x10.xrx.Runtime.Profile) {
        try {
            glock.lock();
            transitPendingAct.put(num, PendingActivity(dstId, fs, bodyBytes, prof));
            if (verbose>=1) debug("<<<< Replicator.addPendingAct(id="+id+", num="+num+", fs="+fs+",bytes="+bodyBytes.size+") returned");
        } finally {
            glock.unlock();
        }
    }
    
    static def sendPendingAct(id:FinishResilient.Id, num:Long) {
        try {
            glock.lock();
            val pendingAct = transitPendingAct.getOrElse(num, NULL_PENDING_ACT);
            assert pendingAct != NULL_PENDING_ACT : here + " FATAL ERROR req["+num+"] lost its pending activity";
            transitPendingAct.remove(num);
            val preSendAction = ()=>{ };
            if (verbose>=1) debug("<<<< Replicator.sendPendingAct(id="+id+", num="+num+", dst="+pendingAct.dst+", fs="+pendingAct.fs+") returned");
            val bytes = pendingAct.bodyBytes;
            val wrappedBody = ()=> @AsyncClosure {
                val deser = new Deserializer(bytes);
                val bodyPrime = deser.readAny() as ()=>void;
                bodyPrime();
            };
            x10.xrx.Runtime.x10rtSendAsync(pendingAct.dst, wrappedBody, pendingAct.fs, pendingAct.prof, preSendAction);
        } finally {
            glock.unlock();
        }
    }
    
    private static def addMasterPending(req:FinishRequest, postSendAction:()=>void) {
        try {
            glock.lock();
            pendingMaster.put(req.num, req);
            if (postSendAction != null)
                postActions.put(req.num, postSendAction);
            if (verbose>=1) debug("<<<< Replicator.addMasterPending(id="+req.id+", num="+req.num+") returned");
        } finally {
            glock.unlock();
        }
    }
    
    private static def removeMasterPending(num:Long) {
        try {
            glock.lock();
            val req = pendingMaster.remove(num);
            if (verbose>=1) debug("<<<< Replicator.removeMasterPending(id="+req.id+", num="+num+") returned");
        } finally {
            glock.unlock();
        }
    }
    
    
    private static def masterToBackupPending(num:Long, masterPlaceId:Long, submit:Boolean) {
        try {
            glock.lock();
            val req = pendingMaster.remove(num);
            req.submit = submit;
            if (req != null) {
                pendingBackup.put(num, req);
                if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+", submit="+submit+") returned");
            } else {
                if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+", submit="+submit+") req not found, masterDead? " + Place(masterPlaceId).isDead());
                assert Place(masterPlaceId).isDead() : here + " FATAL ERROR, pending master request not found although master is alive"; 
            }
        } finally {
            glock.unlock();
        } 
    }
    
    private static def getPendingBackupRequest(num:Long) {
        try {
            glock.lock();
            return pendingBackup.getOrThrow(num);
        } finally {
            glock.unlock();
        } 
    }
    
    private static def finalizeAsyncExec(num:Long) {
        try {
            glock.lock();
            val req = pendingBackup.remove(num);
            if (verbose>=1) debug(">>>> Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+", submit="+req.submit+", backupPlace="+req.backupPlaceId+") called");
            if (req == null) {
                if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+",submit="+req.submit+") req not found, backupDead? " + Place(req.backupPlaceId).isDead());
                assert Place(req.backupPlaceId).isDead() : here + " FATAL ERROR, pending backup request not found although backup is alive";
            }
            val postSendAction = postActions.remove(req.num); 
            if (req.submit && postSendAction != null) {
            	if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+req.id+") executing postSendAction()");
                postSendAction();
            }
            FinishRequest.deallocReq(req);
            if (verbose>=1) debug("<<<< Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+", submit="+req.submit+", backupPlace="+req.backupPlaceId+") returned");
        } finally {
            glock.unlock();
        }
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState):void {
        addMasterPending(req, null);
        asyncExecInternal(req, localMaster); //we retry this line when needed
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState, preSendAction:()=>void, postSendAction:()=>void):void {
        addMasterPending(req, postSendAction);
        preSendAction();
        asyncExecInternal(req, localMaster); //we retry this line when needed
    }
    
    //FIXME: return adoptedId for PESSIMISTIC
    //FIXME: reduce serialization
    private static def asyncExecInternal(req:FinishRequest, localMaster:FinishMasterState) {
        val caller = here;
        val master = Place(req.masterPlaceId);
        if (master.id == here.id) {
            val mFin = localMaster != null ? findMasterOrAdd(req.id, localMaster) : findMaster(req.id);
            assert (mFin != null) : here + " fatal error, master(id="+req.id+") is null";
            val mresp = mFin.exec(req);
            if (mresp.excp instanceof MasterMigrating) {
                System.threadSleep(10);
                asyncExecInternal(req, null);
            }
            else {
                asyncMasterToBackup(caller, req, mresp);
            }
        } else {
            at (master) @Immediate("async_master_exec") async {
                val mFin = findMaster(req.id);
                assert (mFin != null) : here + " fatal error, master(id="+req.id+") is null";
                val mresp = mFin.exec(req);
                at (caller) @Immediate("async_master_exec_response") async {
                    if (mresp.excp instanceof MasterMigrating) {
                        System.threadSleep(10);
                        asyncExecInternal(req, null);
                    }
                    else {
                        asyncMasterToBackup(caller, req, mresp);
                    }
                }
            }
        }
    }
    
    static def asyncMasterToBackup(caller:Place, req:FinishRequest, mresp:MasterResponse) {
        val submit = mresp.submit;
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncMasterToBackup => backupPlaceId = " + mresp.backupPlaceId + " submit = " + submit );
        assert mresp.backupPlaceId != -1n : here + " fatal error ["+req+"], backup -1 means master had a fatal error before reporting its backup value";
        if (mresp.backupChanged) {
            updateBackupPlace(req.id.home, mresp.backupPlaceId);
        }
        val backupGo = ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
        if (backupGo) {
            if (req.reqType == FinishRequest.ADD_CHILD) /*in some cases, the parent may be transiting at the same time as its child, */ 
                req.parentId = mresp.parentId;          /*the child may not find the parent's backup during globalInit, so it needs to create it*/
            req.backupPlaceId = mresp.backupPlaceId;
            req.transitSubmitDPE = mresp.transitSubmitDPE;
            
            masterToBackupPending(req.num, req.masterPlaceId, submit);
            
            val backup = Place(req.backupPlaceId);
            val createOk = req.reqType == FinishRequest.ADD_CHILD || 
                    /*in some cases, the parent may be transiting at the same time as its child, 
                      the child may not find the parent's backup during globalInit, so it needs to create it*/
                    req.reqType == FinishRequest.TRANSIT ||
                    req.reqType == FinishRequest.EXCP ||
                    (req.reqType == FinishRequest.TERM && req.id.home == here.id as Int);
            
            if (backup.id == here.id) {
                if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup backup local");
                val bFin:FinishBackupState;
                if (createOk)
                    bFin = findBackupOrCreate(req.id, req.parentId, Place(req.finSrc), req.finKind);
                else
                    bFin = findBackupOrThrow(req.id);
                val bresp = bFin.exec(req);
                processBackupResponse(bresp, req.num);
            }
            else {
                if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup moving to backup " + backup);
                at (backup) @Immediate("async_backup_exec") async {
                    if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup reached backup ");
                    val bFin:FinishBackupState;
                    if (createOk)
                        bFin = findBackupOrCreate(req.id, req.parentId, Place(req.finSrc), req.finKind);
                    else
                        bFin = findBackupOrThrow(req.id);
                    val bresp = bFin.exec(req);
                    if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup moving to caller " + caller);
                    val num = req.num;
                    at (caller) @Immediate("async_backup_exec_response") async {
                        processBackupResponse(bresp, num);
                    }
                }
            }
        } else {
            if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup() backupGo = false");
            removeMasterPending(req.num);
        }
    }
    
    static def processBackupResponse(bresp:BackupResponse, num:Long) {
        if (verbose>=1) debug(">>>> Replicator(num="+num+").processBackupResponse called" );
        if (bresp.excp instanceof MasterChanged) {
            val req = FinishReplicator.getPendingBackupRequest(num);
            val ex = bresp.excp as MasterChanged;
            req.toAdopter = true;
            req.id = ex.newMasterId;
            req.masterPlaceId = ex.newMasterPlace;
            /*if (!OPTIMISTIC)
                adopterId = req.id;*/
            if (verbose>=1) debug("==== Replicator(id="+req.id+").processBackupResponse MasterChanged exception caught, newMasterId["+ex.newMasterId+"] newMasterPlace["+ex.newMasterPlace+"]" );
            asyncExecInternal(req, null);
        } else {
            finalizeAsyncExec(num);
        }
        if (verbose>=1) debug("<<<< Replicator(num="+num+").processBackupResponse returned" );
    }
    
    static def submitDeadBackupPendingRequests(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> Replicator.submitDeadBackupPendingRequests called");
        val set = new HashSet[FinishRequest]();
        try {
            glock.lock();
            for (entry in pendingBackup.entries()) {
                val req = entry.getValue();
                if (newDead.contains(req.backupPlaceId)) {
                    set.add(req);
                }
            }
            for (r in set) {
                pendingBackup.delete(r.num);
            }
        } finally {
            glock.unlock();
        }
        if (verbose>=1) debug("==== Replicator.submitDeadBackupPendingRequests found "+set.size()+" requests");
        for (req in set) {
            if (verbose>=1) debug("==== Replicator.submitDeadBackupPendingRequests processing=> " + req);
            val postSendAction = postActions.remove(req.num);
            if (req.submit && postSendAction != null) {
                postSendAction();
            }
            FinishRequest.deallocReq(req);
        }
        if (verbose>=1) debug("<<<< Replicator.submitDeadBackupPendingRequests returning");
    }
    
    static def submitDeadMasterPendingRequests(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> Replicator.submitDeadMasterPendingRequests called");
        val set = new HashSet[FinishRequest]();
        try {
            glock.lock();
            for (entry in pendingMaster.entries()) {
                val req = entry.getValue();
                if (newDead.contains(req.masterPlaceId)) {
                    set.add(req);
                }
            }
            for (r in set) {
                pendingMaster.delete(r.num);
            }
        } finally {
            glock.unlock();
        }
        if (verbose>=1) debug("==== Replicator.submitDeadMasterPendingRequests found "+set.size()+" requests");
        for (req in set) {
            prepareRequestForNewMaster(req);
            asyncExecInternal(req, null);
        }
        if (verbose>=1) debug("<<<< Replicator.submitDeadMasterPendingRequests returning");
    }
    
    /**************** Blocking Replication Protocol **********************/
    public static def exec(req:FinishRequest):FinishResilient.ReplicatorResponse {
        return exec(req, null);
    }
    public static def exec(req:FinishRequest, localMaster:FinishMasterState):FinishResilient.ReplicatorResponse {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() called");
        checkMainTermination(); 
        
        var submit:Boolean = false;
        var adopterId:FinishResilient.Id = FinishResilient.UNASSIGNED; //pessimistic only
        while (true) {
            try {
                val mresp:MasterResponse = masterExec(req, localMaster);
                submit = mresp.submit;
                if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() masterDone =>" + " backupPlaceId = " + mresp.backupPlaceId + " submit = " + submit );
                
                assert mresp.backupPlaceId != -1n : here + " fatal error ["+req+"], backup -1 means master had a fatal error before reporting its backup value";
                
                if (mresp.backupChanged) {
                    updateBackupPlace(req.id.home, mresp.backupPlaceId);
                }
                
                val backupGo = ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
                if (backupGo) {
                    if (req.reqType == FinishRequest.ADD_CHILD) /*in some cases, the parent may be transiting at the same time as its child, */ 
                        req.parentId = mresp.parentId;          /*the child may not find the parent's backup during globalInit, so it needs to create it*/
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
                if (!OPTIMISTIC)
                    adopterId = req.id;
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterDied exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterChanged) { 
                req.toAdopter = true;
                req.id = ex.newMasterId;
                req.masterPlaceId = ex.newMasterPlace;
                if (!OPTIMISTIC)
                    adopterId = req.id;
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
        pending.decrementAndGet();
        FinishRequest.deallocReq(req); //reuse the finish request object
        return FinishResilient.ReplicatorResponse(submit, adopterId);
    }
    
    public static def masterExec(req:FinishRequest, localMaster:FinishMasterState) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        
        /**AT_FINISH HACK**/
        if (req.id == FinishResilient.Id(0n,0n) && req.toAdopter) {
            return new MasterResponse();
        }
        
        if (req.isMasterLocal()) {
            val mFin = findMasterOrAdd(req.id, localMaster);
            assert (mFin != null) : here + " fatal error, master(id="+req.id+") is null";
            val resp = mFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
            return resp;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val mFin = findMaster(req.id);
                assert (mFin != null) : here + " fatal error, master(id="+req.id+") is null";
                val resp = mFin.exec(req);
                val r_back = resp.backupPlaceId;
                val r_backChg = resp.backupChanged;
                val r_submit = resp.submit;
                val r_submitDPE = resp.transitSubmitDPE;
                val r_exp = resp.excp;
                val r_parentId = resp.parentId;
                at (gr) @Immediate("master_exec_response") async {
                    val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                    mRes.backupPlaceId = r_back;
                    mRes.backupChanged = r_backChg;
                    mRes.submit = r_submit;
                    mRes.transitSubmitDPE = r_submitDPE;
                    mRes.excp = r_exp;
                    mRes.parentId = r_parentId;
                    gr().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (rCond.failed()) {
            masterRes().excp = new DeadPlaceException(master);
        }
        
        val resp = masterRes();
        
        rCond.forget();
        masterRes.forget();
        
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
        val createOk = req.reqType == FinishRequest.ADD_CHILD || /*in some cases, the parent may be transiting at the same time as its child, 
                                                                   the child may not find the parent's backup during globalInit, so it needs to create it*/
                       req.reqType == FinishRequest.TRANSIT ||
                       req.reqType == FinishRequest.EXCP ||
                       (req.reqType == FinishRequest.TERM && req.id.home == here.id as Int);
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").backupExec called [" + req + "] createOK=" + createOk );
        if (req.isBackupLocal()) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, req.parentId, Place(req.finSrc), req.finKind);
            else
                bFin = findBackupOrThrow(req.id);
            val resp = bFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
            return resp;
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(req.backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
                val bFin:FinishBackupState;
                if (createOk)
                    bFin = findBackupOrCreate(req.id, req.parentId, Place(req.finSrc), req.finKind);
                else
                    bFin = findBackupOrThrow(req.id);
                val resp = bFin.exec(req);
                val r_excp = resp.excp;
                at (gr) @Immediate("backup_exec_response") async {
                    val bRes = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                    bRes.excp = r_excp;
                    gr().release();
                }
            }
        };
        
        rCond.run(closure);
        
        if (rCond.failed()) {
            backupRes().excp = new DeadPlaceException(backup);
        }
        val resp = backupRes();
        
        rCond.forget();
        backupRes.forget();
        
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
                            at (gr) @Immediate("backup_get_new_master_response") async {
                                val req = (reqGR as GlobalRef[FinishRequest]{self.home == here})();
                                if (found) {
                                    req.id = newMasterId;
                                    req.masterPlaceId = newMasterPlace;
                                    req.toAdopter = true;
                                }
                                gr().release();
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
            glock.lock();
            val m = masterMap.getOrElse(idHome, -1n);
            if (m == -1n)
                return idHome;
            else
                return m;
        } finally {
            glock.unlock();
        }
    }
    
    static def getBackupPlace(idHome:Int) {
        try {
            glock.lock();
            val b = backupMap.getOrElse(idHome, -1n);
            if (b == -1n)
                return ((idHome+1)%Place.numPlaces()) as Int;
            else
                return b;
        } finally {
            glock.unlock();
        }
    }
    
    static def updateMyBackupIfDead(newDead:HashSet[Int]) {
        try {
            val idHome = here.id as Int;
            glock.lock();
            var b:Int = backupMap.getOrElse(idHome, -1n);
            if (b == -1n)
                b = ((idHome+1)%Place.numPlaces()) as Int;
            if (newDead.contains(b)) {
                b = ((b+1)%Place.numPlaces()) as Int;
                backupMap.put(idHome, b);
            }
        } finally {
            glock.unlock();
        }
    }
    static def updateBackupPlace(idHome:Int, backupPlaceId:Int) {
        try {
            glock.lock();
            backupMap.put(idHome, backupPlaceId);
            if (verbose>=1) debug("<<<< updateBackupPlace(idHome="+idHome+",newPlace="+backupPlaceId+") returning");
        } finally {
            glock.unlock();
        }
    }
    
    static def updateMasterPlace(idHome:Int, masterPlaceId:Int) {
        try {
            glock.lock();
            masterMap.put(idHome, masterPlaceId);
            if (verbose>=1) debug("<<<< updateMasterPlace(idHome="+idHome+",newPlace="+masterPlaceId+") returning");
        } finally {
            glock.unlock();
        }
    }
    
    static def countBackups(parentId:FinishResilient.Id, src:Int) {
        var count:Int = 0n;
        try {
            glock.lock();
            for (e in fbackups.entries()) {
                if (e.getValue().getParentId() == parentId)
                    count++;
            }
            //no more backups under this parent from that src place should be created
            backupDeny.add(BackupDenyId(parentId, src));
            if (verbose>=1) debug("<<<< countBackups(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
        } finally {
            glock.unlock();
        }
        return count;
    }
    
    /************** Adoption Utilities ****************/
    static def getNewDeadPlaces() {
        if (verbose>=1) debug(">>>> getNewDeadPlaces called");
        val newDead = new HashSet[Int]();
        if (pending.get() < 0n) {
            return newDead;
        }
        try {
            glock.lock();
            for (i in 0n..((Place.numPlaces() as Int) - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    allDead.add(i);
                }
            }
        } finally {
            glock.unlock();
        }
        return newDead;
    }
    
    static def getImpactedMasters(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> lockAndGetImpactedMasters called");
        val result = new HashSet[FinishMasterState]();
        try {
            glock.lock();
            for (e in fmasters.entries()) {
                val id = e.getKey();
                val mFin = e.getValue();
                if (mFin.isImpactedByDeadPlaces(newDead)) {
                    result.add(mFin);
                }
            }
        } finally {
            glock.unlock();
        }
        return result;
    }
    
    static def getImpactedBackups(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> lockAndGetImpactedBackups called");
        val result = new HashSet[FinishBackupState]();
        try {
            glock.lock();
            for (e in fbackups.entries()) {
                val id = e.getKey();
                val bFin = e.getValue();
                if ( OPTIMISTIC && newDead.contains(bFin.getPlaceOfMaster()) || 
                        !OPTIMISTIC && newDead.contains(bFin.getId().home) ) {
                    result.add(bFin);
                }
            }
        } finally {
            glock.unlock();
        }
        return result;
    }
    
    static def nominateMasterPlaceIfDead(idHome:Int) {
        var m:Int;
        var maxIter:Int = Place.numPlaces() as Int;
        var i:Int = 0n;
        try {
            glock.lock();
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
            glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateMasterPlaceIfDead(idHome="+idHome+") returning m="+m);
        return m;
    }
    
    static def nominateBackupPlaceIfDead(idHome:Int) {
        var b:Int;
        var maxIter:Int = Place.numPlaces() as Int;
        var i:Int = 0n;
        try {
            glock.lock();
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
            glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateBackupPlace(idHome="+idHome+") returning b="+b);
        return b;
    }
    
    static def removeMaster(id:FinishResilient.Id) {
        try {
            glock.lock();
            fmasters.delete(id);
            if (verbose>=1) debug("<<<< removeMaster(id="+id+") returning");
        } finally {
            glock.unlock();
        }
    }
    
    static def removeBackup(id:FinishResilient.Id) {
        try {
            glock.lock();
            fbackups.delete(id);
            if (verbose>=1) debug("<<<< removeBackup(id="+id+") returning");
        } finally {
            glock.unlock();
        }
    }
    
    static def addMaster(id:FinishResilient.Id, fs:FinishMasterState) {
        if (verbose>=1) debug(">>>> addMaster(id="+id+") called");
        try {
            glock.lock();
            fmasters.put(id, fs);
            if (verbose>=3) fs.dump();
            if (verbose>=1) debug("<<<< addMaster(id="+id+") returning");
        } finally {
            glock.unlock();
        }
    }
    
    static def findMaster(id:FinishResilient.Id):FinishMasterState {
        if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            glock.lock();
            val fs = fmasters.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return fs;
        } finally {
            glock.unlock();
        }
    }
    
    static def findMasterOrAdd(id:FinishResilient.Id, mFin:FinishMasterState):FinishMasterState {
        if (verbose>=1) debug(">>>> findMasterOrAdd(id="+id+") called");
        try {
            glock.lock();
            var fs:FinishMasterState = fmasters.getOrElse(id, null);
            if (fs == null) {
                fs = mFin;
                fmasters.put(id, mFin);
            }
            if (verbose>=1) debug("<<<< findMasterOrAdd(id="+id+") returning");
            return fs;
        } finally {
            glock.unlock();
        }
    }

    
    static def findBackupOrThrow(id:FinishResilient.Id):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            glock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (bs == null) {
                throw new Exception(here + "Fatal error: backup(id="+id+" not found here");
            }
            else {
                if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning, bs = " + bs);
                return bs;
            }
        } finally {
            glock.unlock();
        }
    }
    
    static def findBackup(id:FinishResilient.Id):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackup(id="+id+") called");
        try {
            glock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findBackup(id="+id+") returning, bs = " + bs);
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, finSrc:Place, finKind:Int):FinishBackupState {
        if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+", parentId="+parentId+") called ");
        try {
            glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
                if (backupDeny.contains(BackupDenyId(parentId, id.home))) {
                    if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") failed, BackupCreationDenied");
                    throw new BackupCreationDenied();
                    //no need to handle this exception; the caller has died.
                }
                
                if (OPTIMISTIC)
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, finSrc, finKind);
                else
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            }
            else {
            	if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, found bs="+bs); 
            }
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    static def createOptimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, finKind:Int, numActive:Long, 
            /*transit:HashMap[FinishResilient.Edge,Int],*/
            sent:HashMap[FinishResilient.Edge,Int], 
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int):FinishBackupState {
        if (verbose>=1) debug(">>>> createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                assert !backupDeny.contains(BackupDenyId(parentId,id.home)) : "must not be in denyList";
                bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, src, finKind, numActive, 
                            /*transit,*/ sent, excs, placeOfMaster);
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientOptimistic.OptimisticBackupState).sync(numActive, /*transit,*/ sent, excs, placeOfMaster);
                if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    
    static def createPessimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, numActive:Long, 
            live:HashMap[FinishResilient.Task,Int], transit:HashMap[FinishResilient.Edge,Int], 
            liveAdopted:HashMap[FinishResilient.Task,Int], transitAdopted:HashMap[FinishResilient.Edge,Int], 
            children:HashSet[FinishResilient.Id],
            excs:GrowableRail[CheckedThrowable]):FinishBackupState {
        if (verbose>=1) debug(">>>> createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId, numActive, live,
                            transit, liveAdopted, transitAdopted, children, excs);
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientPessimistic.PessimisticBackupState).sync(numActive, live,
                            transit, liveAdopted, transitAdopted, children, excs);
                if (verbose>=1) debug("<<<< createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            glock.unlock();
        }
    }
    
    //a very slow hack to terminate replication at all places before shutting down
    public static def finalizeReplication() {
        if (verbose>=1) debug("<<<< Replicator.finalizeReplication called " );
        val numP = Place.numPlaces();
        val places = new Rail[Int](numP, -1n);
        var i:Long = 0;
        for (pl in Place.places())
            places(i++) = pl.id as Int;
        val fin = ResilientLowLevelFinish.make(places);
        val gr = fin.getGr();
        val closure = (gr:GlobalRef[ResilientLowLevelFinish]) => {
            for (p in places) {
                if (verbose>=1) debug("==== Replicator.finalizeReplication  moving from " + here + " to " + Place(p));
                if (Place(p).isDead() || p == -1n) {
                    (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Uncounted async {
                        if (verbose>=1) debug("==== Replicator.finalizeReplication  reached from " + gr.home + " to " + here);
                        waitForZeroPending();
                        val me = here.id as Int;
                        if (verbose>=1) debug("==== Replicator.finalizeReplication  reporting termination to " + gr.home + " from " + here);
                        at (gr) @Immediate("wait_for_zero_pending_done") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting started");
        fin.run(closure);
        if (verbose>=1) debug("LOW_LEVEL_FINISH.waiting ended");
        if (verbose>=1) debug("<<<< Replicator.finalizeReplication returning" );
    }
    
    static def waitForZeroPending() {
        if (verbose>=1) debug(">>>> waitForZeroPending called" );
        while (pending.get() != 0n) {
            System.threadSleep(10); // release the CPU to more productive pursuits
        }
        pending.set(-2000n); //this means the main finish has completed its work
        if (verbose>=1) debug("<<<< waitForZeroPending returning" );
    }
}
