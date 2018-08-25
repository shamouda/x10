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
import x10.util.ArrayList;
import x10.util.concurrent.AtomicInteger;
import x10.util.GrowableRail;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.util.concurrent.Lock;
import x10.io.Deserializer;
import x10.compiler.AsyncClosure;
import x10.xrx.freq.FinishRequest;
import x10.xrx.freq.RemoveGhostChildRequestOpt;
import x10.xrx.freq.MergeSubTxRequestOpt;

public final class FinishReplicator {
    
    public static glock = new Lock(); //global lock for accessing all the static values below\\
    private static val NUM_PLACES = Place.numPlaces() as Int;
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState](); //the set of all masters
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState](); //the set of all backups
    private static val backupDeny = new HashSet[BackupDenyId](); //backup deny list
    private static val allDead = new HashSet[Int](); //all discovered dead places
    private static val pending = new AtomicInteger(0n); //pending replication requests, used for final program termination
    private static val backupMap = new HashMap[Int, Int](); //backup place mapping
    private static val masterMap = new HashMap[Int, Int](); //master place mapping

    private static val verbose = FinishResilient.verbose;
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //non-blocking transit
    private static val pendingMaster = new HashMap[Long,FinishRequest]();
    private static val pendingBackup = new HashMap[Long,FinishRequest]();
    private static val reqRecoveringMaster = new HashSet[Long]();//to avoid recovering the same request twice
    private static val transitPendingAct = new HashMap[Long,PendingActivity]();
    private static val postActions = new HashMap[Long,(Boolean, FinishResilient.Id)=>void]();
    
    protected static struct PendingActivity(dst:Long, fs:FinishState, bodyBytes:Rail[Byte], prof:x10.xrx.Runtime.Profile) {}
    private static val NULL_PENDING_ACT = PendingActivity(-1, null, null, null);
    
    private static val SUCCESS = 0n;
    private static val TARGET_DEAD = 1n;
    private static val LEGAL_ABSENCE = 2n;

    //we don't backup place0 finish by default
    public static val PLACE0_BACKUP = System.getenv("PLACE0_BACKUP") != null && System.getenv("PLACE0_BACKUP").equals("1");
    
    //we must deny new buckup creations from the dead source place
    //places other than the given src can use this place to create a backup
    protected static struct BackupDenyId(parentId:FinishResilient.Id, src:Int) {
        public def toString() = "<backupDenyId parentId=" + parentId + " src=" + src + ">";
    }

    //raises a fatal error if new replication requests were received after main finish terminated
    static def checkMainTermination() {
        val c = pending.incrementAndGet();
        if (c < 0n)
            throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR new finish request posted after main finish<0,0> termination ...");
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
    
    static def removePendingAct(num:Long) {
        try {
            glock.lock();
            val pendingAct = transitPendingAct.getOrElse(num, NULL_PENDING_ACT);
            if (pendingAct == NULL_PENDING_ACT) {
                Console.OUT.println(here + " FATAL ERROR req["+num+"] lost its pending activity");
                System.killHere();
            }
            transitPendingAct.remove(num);
            return pendingAct;
        } finally {
            glock.unlock();
        }
        
    }
    static def sendPendingAct(id:FinishResilient.Id, num:Long) {
        val pendingAct = removePendingAct(num);
        val preSendAction = ()=>{ };
        if (verbose>=1) debug("<<<< Replicator.sendPendingAct(id="+id+", num="+num+", dst="+pendingAct.dst+", fs="+pendingAct.fs+") returned");
        val bytes = pendingAct.bodyBytes;
        val wrappedBody = ()=> @AsyncClosure {
            val deser = new Deserializer(bytes);
            val bodyPrime = deser.readAny() as ()=>void;
            bodyPrime();
        };
        x10.xrx.Runtime.x10rtSendAsync(pendingAct.dst, wrappedBody, pendingAct.fs, pendingAct.prof, preSendAction);
    }
    
    //we don't insert a master request if master is dead
    private static def addMasterPending(req:FinishRequest, postSendAction:(Boolean, FinishResilient.Id)=>void) {
        try {
            glock.lock();
            pendingMaster.put(req.num, req);
            if (postSendAction != null) {
                postActions.put(req.num, postSendAction);
                if (verbose>=1) debug("==== Replicator.addMasterPending(id="+req.id+", num="+req.num+") postAction ADDED");
            }
            if (Place(req.masterPlaceId).isDead()) {
                reqRecoveringMaster.add(req.num); // to prevent notifyPlaceDeath from double-recovering this request
                if (verbose>=1) debug("<<<< Replicator.addMasterPending(id="+req.id+", num="+req.num+",master="+req.masterPlaceId+") returned TARGET_DEAD");
                return TARGET_DEAD;
            }
            if (verbose>=1) debug("<<<< Replicator.addMasterPending(id="+req.id+", num="+req.num+") returned SUCCESS");
            return SUCCESS;
        } finally {
            glock.unlock();
        }
    }
    
    //we don't insert a backup request if backup is dead
    private static def masterToBackupPending(inReq:FinishRequest, submit:Boolean, backupPlaceId:Int, transitSubmitDPE:Boolean) {
        try {
            glock.lock();
            val num = inReq.num;
            val masterPlaceId = inReq.masterPlaceId;
            val req = pendingMaster.remove(num);
            if (req == null)
                throw new Exception(here + " FATAL ERROR masterToBackupPending  req["+inReq.id+"] not found in pendingMaster num="+num);
            
            req.setOutSubmit(submit);   //in
            inReq.backupPlaceId = backupPlaceId; //in
            req.backupPlaceId = backupPlaceId;  //in
            inReq.setSubmitDPE(transitSubmitDPE); //in
            req.setSubmitDPE(transitSubmitDPE); //in
            req.parentId = inReq.parentId; //in
            
            inReq.setOutAdopterId(req.getOutAdopterId()); //out
            inReq.setOutSubmit(submit); //out
            
            if (req != null) {
                if (verbose>=1) debug(">>>> Replicator.masterToBackupPending(id="+req.id+", num="+num+", submit="+submit+") called, req found");
                if (Place(backupPlaceId).isDead()) {
                    if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+", submit="+submit+") returned TARGET_DEAD");
                    return TARGET_DEAD;
                }
                pendingBackup.put(num, req);
                if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+", submit="+submit+") returned SUCCESS");
                return SUCCESS;
            } else {
                if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(num="+num+", submit="+submit+") req not found, masterDead? " + Place(masterPlaceId).isDead());
                if (!Place(masterPlaceId).isDead())
                    throw new Exception(here + " FATAL ERROR, pending master request not found although master is alive");
            }
            return LEGAL_ABSENCE;
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
    
    private static def getPendingBackupRequest(num:Long) {
        try {
            glock.lock();
            return pendingBackup.getOrThrow(num);
        } finally {
            glock.unlock();
        } 
    }
    
    private static def finalizeAsyncExec(num:Long, backupPlaceId:Long) {
        glock.lock();
        val req = pendingBackup.remove(num);
        glock.unlock();
        if (req == null) {
            if (!Place(backupPlaceId).isDead()) {
                Console.OUT.println(here + " FATAL ERROR, pending backup request not found although backup is alive");
                System.killHere();
            }
        }
        else {
            if (verbose>=1) debug(">>>> Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") called");    
            glock.lock();
            val postSendAction = postActions.remove(req.num);
            glock.unlock();
            
            if (postSendAction != null) {
                if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+req.id+") executing postSendAction(submit="+req.getOutSubmit()+",adopterId="+req.getOutAdopterId()+")");
                postSendAction(req.getOutSubmit(), req.getOutAdopterId());
            } else {
                if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") NO_POST_ACTION_FOUND");
            }
            if (verbose>=1) debug("<<<< Replicator.finalizeAsyncExec(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") returned");
        }
    }
    
    private static def finalizeAsyncExecForP0Finish(num:Long, submit:Boolean) {
        glock.lock();
        val req = pendingMaster.remove(num);
        glock.unlock();
        
        if (req == null) {
            Console.OUT.println(here + " FATAL ERROR in finalizeAsyncExecForP0Finish, pending backup request num="+num+" not found ");
            System.killHere();
        }
        else {
            if (verbose>=1) debug(">>>> Replicator.finalizeAsyncExecForP0Finish(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") called");
            req.setOutSubmit(submit);
            glock.lock();
            val postSendAction = postActions.remove(req.num);
            glock.unlock();
            
            if (postSendAction != null) {
                if (verbose>=1) debug("==== Replicator.finalizeAsyncExecForP0Finish(id="+req.id+") executing postSendAction(submit="+req.getOutSubmit()+",adopterId="+req.getOutAdopterId()+")");
                postSendAction(req.getOutSubmit(), req.getOutAdopterId());
            } else {
                if (verbose>=1) debug("==== Replicator.finalizeAsyncExecForP0Finish(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") NO_POST_ACTION_FOUND");
            }
            if (verbose>=1) debug("<<<< Replicator.finalizeAsyncExecForP0Finish(id="+req.id+", num="+req.num+", submit="+req.getOutSubmit()+", backupPlace="+req.backupPlaceId+") returned");
        }
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState):void {
        val rc = addMasterPending(req, null);
        if (rc == SUCCESS)
            asyncExecInternal(req, localMaster); //we retry this line when needed
        else 
            handleMasterDied(req);
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState, preSendAction:()=>void, postSendAction:(Boolean, FinishResilient.Id)=>void):void {
        preSendAction();
        val rc = addMasterPending(req, postSendAction);
        if (rc == SUCCESS)
            asyncExecInternal(req, localMaster); //we retry this line when needed
        else
            handleMasterDied(req);
    }
    
    private static def asyncExecInternal(req:FinishRequest, localMaster:FinishMasterState) {
        val caller = here;
        val master = Place(req.masterPlaceId);
        if (master.id == here.id) {
            if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncExecInternal local" );
            val mFin = localMaster != null ? findMasterOrAdd(req.id, localMaster) : findMaster(req.id);
            if (mFin == null)
                throw new Exception (here + " fatal error, master(id="+req.id+") is null1 while processing req["+req+"]");
            req.isLocal = true;
            val mresp = mFin.exec(req);
            req.isLocal = false;
            if (mresp.errMasterMigrating) {
                if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncExecInternal MasterMigrating1, try again after 10ms" );
                Runtime.submitUncounted( ()=>{
                    System.threadSleep(10);
                    asyncExecInternal(req, null);
                });
            }
            else {
                asyncMasterToBackup(caller, req, mresp);
            }
        } else {
            if (verbose>=1) debug("==== Replicator(id="+req.id+",num="+req.num+").asyncExecInternal remote moving to master " + master);
            at (master) @Immediate("async_master_exec") async {
                if (req == null)
                    throw new Exception(here + " SER_FATAL at master => req is null");
                if (verbose>=1) debug("==== Replicator(id="+req.id+",num="+req.num+").asyncExecInternal remote reached master ");
                val mFin = findMaster(req.id);
                if (mFin == null)  {
                    Console.OUT.println(here + " FATAL ERROR, master(id="+req.id+",num="+req.num+") is null2 while processing req["+req+"]");
                    System.killHere();
                }
                val mresp = mFin.exec(req);
                if (verbose>=1) debug("==== Replicator(id="+req.id+",num="+req.num+").asyncExecInternal remote master moving to caller " + caller);
                at (caller) @Immediate("async_master_exec_response") async {
                    if (mresp == null || req == null)
                        throw new Exception(here + " SER_FATAL at caller => mresp is null? "+(mresp == null)+", req is null? " + (req==null));
                    if (mresp.errMasterMigrating) {
                        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncExecInternal MasterMigrating2, try again after 10ms" );
                        //we cannot block within an immediate thread
                        Runtime.submitUncounted( ()=>{
                            System.threadSleep(10); 
                            asyncExecInternal(req, null);
                        });
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
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+",num="+req.num+").asyncMasterToBackup => backupPlaceId = " + mresp.backupPlaceId + " submit = " + submit + " parentId="+req.parentId + " req="+req);
        if (mresp.backupPlaceId == -1n)
            throw new Exception (here + " FATAL ERROR ["+req+"], backup -1 means master had a fatal error before reporting its backup value");
        if (mresp.backupChanged) {
            updateBackupPlace(req.id.home, mresp.backupPlaceId);
        }
        val backupGo = ( submit || (mresp.transitSubmitDPE && req.isTransitRequest()) );
        if (backupGo) {
            if (req.isAddChildRequest()){ /*in some cases, the parent may be transiting at the same time as its child, */
                /*the child may not find the parent's backup during globalInit, so it needs to create it*/
                req.parentId = FinishResilient.Id(mresp.parentIdHome, mresp.parentIdSeq);          
            }
            val p0Cond = req.id.home != 0n || PLACE0_BACKUP; 
            if (p0Cond)  {
                val rc = masterToBackupPending(req, submit, mresp.backupPlaceId, mresp.transitSubmitDPE);
                if (rc == TARGET_DEAD) {
                    //ignore backup and go ahead with post processing
                    handleBackupDied(req);
                } else if (rc == SUCCESS){ //normal path
                    val backupPlaceId = req.backupPlaceId;
                    val backup = Place(backupPlaceId);
                    val createOk = req.createOK();
                    if (backup.id == here.id) {
                        if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup backup local");
                        val bFin:FinishBackupState;
                        if (createOk)
                            bFin = findBackupOrCreate(req.id, req.parentId, req.getTx(), req.isRootTx());
                        else if (req instanceof RemoveGhostChildRequestOpt ||
                                req instanceof MergeSubTxRequestOpt)
                            bFin = findBackup(req.id);
                        else
                            bFin = findBackupOrThrow(req.id);
                        req.isLocal = true;
                        var bresp:BackupResponse = null;
                        if (bFin != null)
                            bresp = bFin.exec(req);
                        else
                            bresp = new BackupResponse(); //allowed only for RemoveGhost
                        req.isLocal = false;
                        processBackupResponse(bresp, req.num, backupPlaceId);
                    } else {
                        if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup moving to backup " + backup);
                        at (backup) @Immediate("async_backup_exec") async {
                            if (req == null)
                                throw new Exception(here + " SER_FATAL at backup => req is null");
                            if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup reached backup ");
                            val bFin:FinishBackupState;
                            if (createOk)
                                bFin = findBackupOrCreate(req.id, req.parentId, req.getTx(), req.isRootTx());
                            else if (req instanceof RemoveGhostChildRequestOpt ||
                                    req instanceof MergeSubTxRequestOpt)
                                bFin = findBackup(req.id);
                            else
                                bFin = findBackupOrThrow(req.id);
                            var resp:BackupResponse = null;
                            if (bFin != null)
                                resp = bFin.exec(req);
                            else
                                resp = new BackupResponse(); //allowed only for RemoveGhost
                            val bresp = resp;
                            if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup backup moving to caller " + caller);
                            val num = req.num;
                            at (caller) @Immediate("async_backup_exec_response") async {
                                if (bresp == null)
                                    throw new Exception(here + " SER_FATAL at caller => bresp is null");
                                processBackupResponse(bresp, num, backupPlaceId);
                            }
                        }
                    }                
                } //else is LEGAL ABSENCE => backup death is being handled by notifyPlaceDeath
            } else { //don't backup place0 finishes
                finalizeAsyncExecForP0Finish(req.num, submit);
            }
        } else {
            if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup() backupGo = false");
            removeMasterPending(req.num);
        }
    }
    
    static def processBackupResponse(bresp:BackupResponse, num:Long, backupPlaceId:Long) {
        if (verbose>=1) debug(">>>> Replicator(num="+num+").processBackupResponse called" );
        if (bresp.errMasterChanged) {
            val req = FinishReplicator.getPendingBackupRequest(num);
            req.masterPlaceId = bresp.newMasterPlace;
            if (verbose>=1) debug("==== Replicator(id="+req.id+").processBackupResponse MasterChanged exception caught, newMasterPlace["+bresp.newMasterPlace+"]" );
            asyncExecInternal(req, null);
        } else if (bresp.errMasterDied) {
            val req = FinishReplicator.getPendingBackupRequest(num);
            if (verbose>=1) debug("==== Replicator(id="+req.id+").processBackupResponse MasterDied exception caught" );
            handleMasterDied(req);
        } else {
            finalizeAsyncExec(num, backupPlaceId);
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
            handleBackupDied(req);   
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
                if (newDead.contains(req.masterPlaceId) && !reqRecoveringMaster.contains(req.num)) {
                    set.add(req);
                    reqRecoveringMaster.add(req.num);
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
            handleMasterDied(req);
        }
        if (verbose>=1) debug("<<<< Replicator.submitDeadMasterPendingRequests returning");
    }
    
    static def handleMasterDied(req:FinishRequest) {
        if (Runtime.activity() != null) {
            val ignore = backupGetNewMaster(req);
            if (ignore)//this is true only in remove ghost when the master and backup objects are not found
                return;
            if (!OPTIMISTIC) {
                req.setOutAdopterId(req.id);
                req.setOutSubmit(false);
            }
            asyncExec(req, null);
        } else {
            Runtime.submitUncounted( ()=>{
                val ignore = backupGetNewMaster(req);
                if (ignore)//this is true only in remove ghost when the master and backup objects are not found
                    return;
                if (!OPTIMISTIC) {
                    req.setOutAdopterId(req.id);
                    req.setOutSubmit(false);
                }
                asyncExec(req, null);
            });
        }
    }
    
    static def handleBackupDied(req:FinishRequest) {
        if (verbose>=1) debug(">>>> Replicator.handleBackupDied(id="+req.id+") called");
        val postSendAction:(Boolean, FinishResilient.Id)=>void;
        try {
            glock.lock();
            postSendAction = postActions.remove(req.num);
        } finally {
            glock.unlock();
        }
        if (postSendAction != null) {
            if (verbose>=1) debug("==== Replicator.handleBackupDied(id="+req.id+") calling postSendAction(submit="+req.getOutSubmit()+",adopterId="+req.getOutAdopterId()+")");
            postSendAction(req.getOutSubmit(), req.getOutAdopterId());
        }
        if (verbose>=1) debug("<<<< Replicator.handleBackupDied(id="+req.id+") returning");
        /////FinishRequest.deallocReq(req);
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
                
                if (mresp.backupPlaceId == -1n)
                    throw new Exception (here + " fatal error ["+req+"], backup -1 means master had a fatal error before reporting its backup value");
                
                if (mresp.backupChanged) {
                    updateBackupPlace(req.id.home, mresp.backupPlaceId);
                }
                
                val p0Cond = req.id.home != 0n || PLACE0_BACKUP;
                val backupGo = p0Cond && ( submit || (mresp.transitSubmitDPE && req.isTransitRequest()));
                if (backupGo) {
                    if (req.isAddChildRequest()) { /*in some cases, the parent may be transiting at the same time as its child, */
                                                   /*the child may not find the parent's backup during globalInit, so it needs to create it*/
                        req.parentId = FinishResilient.Id(mresp.parentIdHome, mresp.parentIdSeq);
                    }
                    req.backupPlaceId = mresp.backupPlaceId;
                    req.setSubmitDPE(mresp.transitSubmitDPE);
                    backupExec(req);
                } else {
                    if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() backupGo = false");    
                }
                
                if (verbose>=1) debug("<<<< Replicator(id="+req.id+").exec() returning");
                break;
            } catch (ex:MasterDied) {
                val ignore = backupGetNewMaster(req); // we communicate with backup to ask for the new master location
                if (ignore) //this is true only in remove ghost when the master and backup objects are not found
                    break;
                if (!OPTIMISTIC)
                    adopterId = req.id;
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterDied exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterChanged) { //optimistic only
                req.masterPlaceId = ex.newMasterPlace;
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterChanged exception, forward to newMaster " + req.id + " @ " + req.masterPlaceId);
            } catch (ex:MasterMigrating) {
                if (verbose>=1) debug("==== Replicator(id="+req.id+").exec() threw MasterMigrating exception, retry later");
                //try again later
                System.threadSleep(10);
            } catch (ex:BackupDied) {
                try {
                    val newBackupId = masterGetNewBackup(req, localMaster);
                    updateBackupPlace(req.id.home, newBackupId);
                } catch (mdied:MasterDied) {
                    throw new Exception (here + " FATAL error, backup died, as well as original master");
                }
                debug("<<<< Replicator(id="+req.id+").exec() returning: ignored backup failure exception");
                break; //master should re-replicate
            }
        }
        pending.decrementAndGet();
        //////FinishRequest.deallocReq(req); //reuse the finish request object
        return FinishResilient.ReplicatorResponse(submit, adopterId);
    }
    
    public static def masterExec(req:FinishRequest, localMaster:FinishMasterState) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        
        /**AT_FINISH HACK**/
        if (req.id == FinishResilient.Id(0n,0n) && req.isToAdopter()) {
            return new MasterResponse();
        }
        
        if (req.masterPlaceId == here.id as Int) { /*Local master*/ 
            val mFin = findMasterOrAdd(req.id, localMaster);
            if (mFin == null)
                throw new Exception (here + " fatal error, master(id="+req.id+") is null");
            req.isLocal = true;
            val resp = mFin.exec(req);
            req.isLocal = false;
            if (resp.errMasterMigrating) { 
                throw new MasterMigrating();
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
                if (mFin == null)
                    throw new Exception (here + " fatal error, master(id="+req.id+") is null");
                val resp = mFin.exec(req);
                val r_back = resp.backupPlaceId;
                val r_backChg = resp.backupChanged;
                val r_submit = resp.submit;
                val r_submitDPE = resp.transitSubmitDPE;
                val r_errMasterMigrating = resp.errMasterMigrating;
                val r_parentId_home = resp.parentIdHome;
                val r_parentId_seq = resp.parentIdSeq;
                at (gr) @Immediate("master_exec_response") async {
                    val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                    mRes.backupPlaceId = r_back;
                    mRes.backupChanged = r_backChg;
                    mRes.submit = r_submit;
                    mRes.transitSubmitDPE = r_submitDPE;
                    mRes.errMasterMigrating = r_errMasterMigrating;
                    mRes.parentIdHome = r_parentId_home;
                    mRes.parentIdSeq = r_parentId_seq;
                    gr().release();
                }
            }
        };
        
        rCond.run(closure, false);
        
        if (rCond.failed()) {
            rCond.forget();
            masterRes.forget();
            throw new MasterDied();
        }
        
        val resp = masterRes();
        rCond.forget();
        masterRes.forget();
        
        if (resp.errMasterMigrating) { 
            throw new MasterMigrating();
        }
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupExec(req:FinishRequest) {
        val createOk = req.createOK();        
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").backupExec called [" + req + "] createOK=" + createOk );
        if (req.backupPlaceId == here.id as Int) { //Local Backup
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, req.parentId, req.getTx(), req.isRootTx());
            else if (req instanceof RemoveGhostChildRequestOpt ||
                    req instanceof MergeSubTxRequestOpt)
                bFin = findBackup(req.id);
            else
                bFin = findBackupOrThrow(req.id);
            req.isLocal = true;
            
            var resp:BackupResponse = null;
            if (bFin != null)
                resp = bFin.exec(req);
            else
                resp = new BackupResponse(); //allowed only for RemoveGhost
            
            req.isLocal = false;
            if (resp.errMasterDied) { 
                throw new MasterDied();
            }
            if (resp.errMasterChanged) { //optimistic only
                throw new MasterChanged(resp.newMasterPlace);
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
                    bFin = findBackupOrCreate(req.id, req.parentId, req.getTx(), req.isRootTx());
                else if (req instanceof RemoveGhostChildRequestOpt ||
                        req instanceof MergeSubTxRequestOpt)
                    bFin = findBackup(req.id);
                else
                    bFin = findBackupOrThrow(req.id);
                var resp:BackupResponse = null;
                if (bFin != null)
                    resp = bFin.exec(req);
                else
                    resp = new BackupResponse(); //allowed only for RemoveGhost
                val r_errMasterDied = resp.errMasterDied;
                val r_errMasterChanged = resp.errMasterChanged;
                val r_newMasterPlace = resp.newMasterPlace;
                at (gr) @Immediate("backup_exec_response") async {
                    val bRes = (backupRes as GlobalRef[BackupResponse]{self.home == here})();
                    bRes.errMasterDied = r_errMasterDied;
                    bRes.errMasterChanged = r_errMasterChanged;
                    bRes.newMasterPlace = r_newMasterPlace;
                    gr().release();
                }
            }
        };
        
        rCond.run(closure, false);
        
        if (rCond.failed()) {
            rCond.forget();
            backupRes.forget();
            throw new BackupDied();
        }
        val resp = backupRes();
        
        rCond.forget();
        backupRes.forget();
        
        if (resp.errMasterDied) { 
            throw new MasterDied();
        }
        if (resp.errMasterChanged) {
            throw new MasterChanged(resp.newMasterPlace);
        }
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+").backupExec returning [" + req + "]" );
        return resp;
    }
    
    //prepares request for new master - returns true only if backup was not found for a RemoveGhost request
    public static def backupGetNewMaster(req:FinishRequest):Boolean {
        val id = req.id;
        val initBackup = getBackupPlace(id.home);
        var curBackup:Int = initBackup;
        
        val resp = new GetNewMasterResponse();
        val respGR = new GlobalRef[GetNewMasterResponse](resp);
        do {
            if (verbose>=1) debug(">>>> backupGetNewMaster(id="+id+") called, trying curBackup="+curBackup );
            if (curBackup == here.id as Int) { 
                val bFin = findBackup(id);
                if (bFin != null) {
                    resp.found = true;
                    resp.newMasterId = bFin.getNewMasterBlocking();
                    if (req.id == FinishResilient.Id(0n,0n))
                        resp.newMasterPlace = 0n;  /**AT_FINISH HACK**/
                    else
                        resp.newMasterPlace = bFin.getPlaceOfMaster();
                    break;
                }
            } else {
                val backup = Place(curBackup);
                if (verbose>=1) debug("==== backupGetNewMaster(id="+id+") going to backup="+backup );
                val me = here;
                if (!backup.isDead()) {
                    //we cannot use Immediate activities, because this function is blocking
                    val rCond = ResilientCondition.make(backup);
                    val closure = (gr:GlobalRef[Condition]) => {
                        at (backup) @Immediate("backup_get_new_master_request") async {
                            Runtime.submitUncounted( ()=>{
                                var foundVar:Boolean = false;
                                var newMasterIdVar:FinishResilient.Id = FinishResilient.UNASSIGNED;
                                var newMasterPlaceVar:Int = -1n;
                                val bFin = findBackup(id);
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
                                    val backResp = (respGR as GlobalRef[GetNewMasterResponse]{self.home == here})();
                                    backResp.found = found;
                                    backResp.newMasterId = newMasterId;
                                    backResp.newMasterPlace = newMasterPlace;
                                    gr().release();
                                }
                            });
                        }
                    };
                    
                    rCond.run(closure, true);
                    if (rCond.failed()) {
                        Console.OUT.println("FATAL: backupGetNewMaster(id="+id+") backup="+backup  + "  failed: MasterAndBackupDied");
                        //throw new MasterAndBackupDied();
                        System.killHere();
                    }
                    rCond.forget();
                    if (respGR().found) {
                        break;
                    }
                }
            }
            curBackup = ((curBackup + 1) % NUM_PLACES) as Int;
        } while (initBackup != curBackup);
        
        if (!resp.found && !(req instanceof RemoveGhostChildRequestOpt || req instanceof MergeSubTxRequestOpt)) {
            if (verbose>=1) debug("==== backupGetNewMaster(id="+id+") failed: FATAL exception, cannot find backup for id=" + id);
            Console.OUT.println("BackupMap = " + getBackupMapAsString());
            Console.OUT.println(here + " ["+Runtime.activity()+"] FATAL exception, cannot find backup for id=" + id + "   initBackup=" + initBackup + "  curBackup=" + curBackup + " req="+req);
            System.killHere();
        }
        
        if (!resp.found && (req instanceof RemoveGhostChildRequestOpt || req instanceof MergeSubTxRequestOpt)) {
            respGR.forget();
            return true; //ignore
        }
        
        req.id = resp.newMasterId;
        req.masterPlaceId = resp.newMasterPlace;
        req.setToAdopter(true);
        
        respGR.forget();
        
        if (verbose>=1) debug("<<<< backupGetNewMaster(id="+id+") returning, curBackup="+curBackup + " newMasterId="+req.id+",newMasterPlace="+req.masterPlaceId+",toAdopter="+req.isToAdopter());
        if (curBackup != initBackup)
            updateBackupPlace(id.home, curBackup);
        if (OPTIMISTIC) //master place doesn't change for pessimistic finish
            updateMasterPlace(req.id.home, req.masterPlaceId);
        
        glock.lock();
        reqRecoveringMaster.remove(req.num);
        glock.unlock();
        return false; //don't ignore
    }
    
    //dummy communication with the master to ensure it is still alive
    //if it died, fatal error must be raised even if a new master was created by backup
    //because the new master's state may be incorrect
    public static def masterGetNewBackup(req:FinishRequest, localMaster:FinishMasterState) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterGetNewBackup called [" + req + "]" );
        
        if (req.masterPlaceId == here.id as Int) { /*Local master*/
            val mFin = findMasterOrAdd(req.id, localMaster);
            if (mFin == null)
                throw new Exception (here + " fatal error, master(id="+req.id+") is null");
            val backupId = mFin.getBackupId();
            if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterGetNewBackup returning [" + req + "]" );
            return backupId;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_get_backup_id") async {
                val mFin = findMaster(req.id);
                if (mFin == null)
                    throw new Exception (here + " fatal error, master(id="+req.id+") is null");
                val r_back = mFin.getBackupId();
                at (gr) @Immediate("master_get_backup_id_response") async {
                    val mRes = (masterRes as GlobalRef[MasterResponse]{self.home == here})();
                    mRes.backupPlaceId = r_back;
                    gr().release();
                }
            }
        };
        
        rCond.run(closure, true);
        
        if (rCond.failed()) {
            rCond.forget();
            masterRes.forget();
            throw new MasterDied();
        }
        
        val resp = masterRes();
        rCond.forget();
        masterRes.forget();
        
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
        return resp.backupPlaceId;
    }
    
    
    /***************** Utilities *********************************/
    static def debug(msg:String) {
        val nsec = System.nanoTime();
        val output = "[nsec=" + nsec + " place=" + here.id + " " + Runtime.activity() + "] " + msg;
        Console.OUT.println(output); Console.OUT.flush();
    }
    
    public static def getMasterPlace(idHome:Int) {
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
    
    static def getBackupMapAsString() {
        var str:String = "";
        try {
            glock.lock();
            for (e in backupMap.entries()) {
                str += "("+e.getKey()+","+e.getValue()+") " ;
            }
            return str;
        } finally {
            glock.unlock();
        }
    }
    
    
    static def isBackupOf(idHome:Int) {
        try {
            glock.lock();
            for ( e in fbackups.entries()) {
                if (e.getKey().home == idHome)
                    return true;
            }
            return false;
        } finally {
            glock.unlock();
        }
    }
    
    static def searchBackup(idHome:Int, deadBackup:Int) {
        if (verbose>=1) debug(">>>> searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") called ");
        var b:Int = deadBackup;
        for (var c:Int = 1n; c < NUM_PLACES; c++) {
            val nextPlace = Place((deadBackup + c) % NUM_PLACES);
            if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") nextPlace is " + nextPlace + "  dead?" + nextPlace.isDead());
            if (nextPlace.isDead())
                continue;
            val searchResp = new GlobalRef[SearchBackupResponse](new SearchBackupResponse());
            val rCond = ResilientCondition.make(nextPlace);
            val closure = (gr:GlobalRef[Condition]) => {
                at (nextPlace) @Immediate("search_backup") async {
                    val found = FinishReplicator.isBackupOf(idHome);
                    if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") found="+found);
                    at (gr) @Immediate("search_backup_response") async {
                        val resp = (searchResp as GlobalRef[SearchBackupResponse]{self.home == here})();
                        resp.found = found;
                        gr().release();
                    }
                }
            };
            rCond.run(closure, true);
            
            if (rCond.failed()) {
                Console.OUT.println(here + " WARNING - another place["+nextPlace+"] failed while searching for backup");
                rCond.forget();
                searchResp.forget();
            }
            else {
                val resp = searchResp();
                rCond.forget();
                searchResp.forget();
                if (resp.found) {
                    b = nextPlace.id as Int;
                    if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") result b="+b);
                    updateBackupPlace(idHome, b);
                    break;
                }
            }
        }
        if (Place(b).isDead())
            throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR could not find backup for idHome = " + idHome);
        if (verbose>=1) debug("<<<< searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") returning b=" + b);
        return b;
    }
    
    
    
    static def updateMyBackupIfDead(newDead:HashSet[Int]) {
        try {
            val idHome = here.id as Int;
            if (verbose>=1) debug(">>>> updateMyBackupIfDead(idHome="+idHome+") called");
            glock.lock();
            var b:Int = backupMap.getOrElse(idHome, -1n);
            if (b == -1n) {
                b = ((idHome+1)%NUM_PLACES) as Int;
                if (verbose>=1) debug("==== updateMyBackupIfDead(idHome="+idHome+") not in backupMap, b="+b);
            }
            if (newDead.contains(b)) {
                b = ((b+1)%NUM_PLACES) as Int;
                backupMap.put(idHome, b);
                if (verbose>=1) debug("==== updateMyBackupIfDead(idHome="+idHome+") b died, newb="+b);
            }
            if (verbose>=1) debug("<<<< updateMyBackupIfDead(idHome="+idHome+") returning myBackup=" + b);
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
    
    static def countChildrenBackups(parentId:FinishResilient.Id, deadDst:Int, src:Int) {
        val set = new HashSet[FinishResilient.Id]();
        try {
            glock.lock();
            for (e in fbackups.entries()) {
                val backup = e.getValue() as FinishResilientOptimistic.OptimisticBackupState;
                if (backup.getParentId() == parentId && /* backup.finSrc.id as Int == src &&*/ backup.getId().home == deadDst) {
                    if (verbose>=1) debug("== countChildrenBackups(parentId="+parentId+", src="+src+") found state " + backup.id);
                    set.add(backup.getId());
                }
            }
            //no more backups under this parent from that src place should be created
            backupDeny.add(BackupDenyId(parentId, deadDst));
            if (verbose>=1) debug("<<<< countChildrenBackups(parentId="+parentId+") returning, count = " + set.size() + " and parentId added to denyList");
        } finally {
            glock.unlock();
        }
        return set;
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
            for (i in 0n..(NUM_PLACES - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    if (verbose>=1) debug("==== getNewDeadPlaces newDead=Place("+i+")");
                    allDead.add(i);
                }
            }
        } finally {
            glock.unlock();
        }
        if (newDead.contains(0n)){
            Console.OUT.println(here + " detected the failure of place0 -- terminating!!");
            System.killHere();
        }
        if (verbose>=1) debug("<<<< getNewDeadPlaces returning");
        return newDead;
    }
    
    static def getImpactedMasters(newDead:HashSet[Int]) {
        if (verbose>=1) debug(">>>> lockAndGetImpactedMasters called");
        val result = new HashSet[FinishMasterState]();
        val tmp = new HashSet[FinishMasterState]();
        try {
            glock.lock();
            for (e in fmasters.entries()) { //copy to another list to avoid locking glock while calling isImpactedBy...
                val mFin = e.getValue();
                tmp.add(mFin);
            }
        } finally {
            glock.unlock();
        }
        
        for (mFin in tmp) {
            var im:Boolean = false;
            if (mFin.isImpactedByDeadPlaces(newDead)) {
                result.add(mFin);
                im = true;
            }
            if (verbose>=1) debug("==== lockAndGetImpactedMasters id=" +  mFin.getId() + " isImpacted? " + im);
        }
        if (verbose>=1) {
            var s:String = "";
            for (e in result)
                s += e + " ";
            debug("<<<< lockAndGetImpactedMasters returning ["+s+"]");
        }
        return result;
    }
    
    static def isLeaf(id:FinishResilient.Id, set:HashSet[FinishBackupState]) {
        if (verbose>=1) debug(">>>> isLeaf("+id+") called");
        for (e in set) {
            if (e.getParentId() == id) {
                if (verbose>=1) debug("<<<< isLeaf("+id+") returning false, the child is " + e.getId());
                return false;
            }
        }
        if (verbose>=1) debug("<<<< isLeaf("+id+") returning true");
        return true;
    }
    
    static def getImpactedBackups(newDead:HashSet[Int]):ArrayList[FinishBackupState] {
        if (verbose>=1) debug(">>>> lockAndGetImpactedBackups called");
        val tmp2 = new HashSet[FinishBackupState]();
        val unordered = new HashSet[FinishBackupState]();
        val list = new ArrayList[FinishBackupState]();
        
        try {
            glock.lock();
            for (e in fbackups.entries()) {
                val bFin = e.getValue();
                tmp2.add(bFin);
            } 
        } finally {
            glock.unlock();
        }
            
        if (verbose>=1) debug(">>>> lockAndGetImpactedBackups obtained lock");
        for (bFin in tmp2) {
            if ( bFin.getId().id != -5555n && ( 
                 OPTIMISTIC && (newDead.contains(bFin.getPlaceOfMaster()) || bFin.getTxStarted() )  || 
                !OPTIMISTIC && newDead.contains(bFin.getId().home) ) ) {
                unordered.add(bFin);
            }
        }
        if (verbose>=1) {
            var str:String = "";
            for (k in unordered)
                str += k.getId() + " , ";
            debug("==== lockAndGetImpactedBackups unordered ["+str+"]");
        }
        while (unordered.size() > 0) {
            var tmp:FinishBackupState = null;
            var rm:Boolean = false;
            for (node in unordered) {
                if (isLeaf(node.getId(), unordered)) {
                    rm = true;
                    tmp = node;
                    break;
                }
            }
            if (rm) {
                unordered.remove(tmp);
                list.add(tmp);
                if (unordered.size() == 0)
                    break;
            }
        }
        if (verbose>=1) {
            var str:String = "";
            for (k in list)
                str += k.getId() + " , ";
            debug("<<<< lockAndGetImpactedBackups returning ["+str+"]");
        }
        return list;
    }
    
    static def nominateMasterPlaceIfDead(idHome:Int) {
        var m:Int;
        var maxIter:Int = NUM_PLACES;
        var i:Int = 0n;
        try {
            glock.lock();
            m = masterMap.getOrElse(idHome,-1n);
            if (m == -1n)
                m = ((idHome - 1 + NUM_PLACES)%NUM_PLACES) as Int;
            while (allDead.contains(m) && i < maxIter) {
                m = ((m - 1 + NUM_PLACES)%NUM_PLACES) as Int;
                i++;
            }
            if (allDead.contains(m) || i == maxIter) {
                Console.OUT.println(here + " ["+Runtime.activity()+"] FATAL ERROR couldn't nominate a new master place for idHome=" + idHome);
                System.killHere();
            }
        } finally {
            glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateMasterPlaceIfDead(idHome="+idHome+") returning m="+m);
        return m;
    }
    
    static def applyNominatedMasterPlaces(map:HashMap[Int,Int]) {
        if (map == null)
            return;
        try {
            glock.lock();
            for (e in map.entries()) {
                masterMap.put(e.getKey(), e.getValue());
                if (verbose>=1) debug("<<<< applyNominatedMasterPlaces(idHome="+e.getKey()+",masterPlace"+e.getValue()+")");
            }
        } finally {
            glock.unlock();
        }
    }
    
    static def updateBackupPlaceIfDead(idHome:Int) {
        var b:Int;
        var maxIter:Int = NUM_PLACES;
        var i:Int = 0n;
        try {
            glock.lock();
            b = backupMap.getOrElse(idHome,-1n);
            if (b == -1n)
                b = ((idHome + 1)%NUM_PLACES) as Int;
            while(allDead.contains(b) && i < maxIter) {
                b = ((b + 1)%NUM_PLACES) as Int;
                i++;
            }
            if (allDead.contains(b) || i == maxIter)
                throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR couldn't nominate a new backup place for idHome=" + idHome);
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
    
    static def removeBackupOrMarkToDelete(id:FinishResilient.Id) {
        try {
            glock.lock();
            val bFin = fbackups.getOrElse(id, null);
            if (bFin != null) {
                bFin.removeBackupOrMarkToDelete();
            }
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
    
    static def findMaster(id:FinishResilient.Id) {
        if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            glock.lock();
            val master = fmasters.getOrElse(id, null);
            if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return master;
        } finally {
            glock.unlock();
        }
    }
    
    static def findMasterOrAdd(id:FinishResilient.Id, mFin:FinishMasterState) {
        if (verbose>=1) debug(">>>> findMasterOrAdd(id="+id+") called");
        try {
            glock.lock();
            val master = fmasters.getOrElse(id, null);
            if (master == null) {
                fmasters.put(id, mFin);
            }
            if (verbose>=1) debug("<<<< findMasterOrAdd(id="+id+") returning");
            return master;
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
                if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning with FATAL ERROR backup(id="+id+") not found here");
                throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR backup(id="+id+") not found here");
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
    
    

    static def pesFindBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, children:HashSet[FinishResilient.Id]):FinishBackupState {
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
                bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId, children);
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

    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, tx:Tx, rootTx:Boolean):FinishBackupState {
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
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, tx, rootTx);
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
    
    //added to mark a place as a new backup when no backups are there yet
    static def createDummyBackup(idHome:Int) {
        if (verbose>=1) debug(">>>> createDummyBackup called");
        try {
            glock.lock();
            val id = FinishResilient.Id(idHome, -5555n);
            if (OPTIMISTIC)
                fbackups.put(id, new FinishResilientOptimistic.OptimisticBackupState(id, id, null, false));
            else
                fbackups.put(id, new FinishResilientPessimistic.PessimisticBackupState(id, id));
            
            if (verbose>=1) debug("<<<< createDummyBackup returning");
        } finally {
            glock.unlock();
        }
    }
    
    
    static def createOptimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, numActive:Long, 
            sent:HashMap[FinishResilient.Edge,Int], 
            transit:HashMap[FinishResilient.Edge,Int],
            ghostChildren:HashSet[FinishResilient.Id],
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int, tx:Tx, rootTx:Boolean):FinishBackupState {
        if (verbose>=1) debug(">>>> createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                if (backupDeny.contains(BackupDenyId(parentId,id.home)))
                    throw new Exception (here + " FATAL: " + id + " must not be in denyList");
                bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, numActive, 
                        sent, transit, ghostChildren, excs, placeOfMaster, tx, rootTx);
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientOptimistic.OptimisticBackupState).sync(numActive, sent, transit, ghostChildren, excs, placeOfMaster);
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
        
        val numP = NUM_PLACES;
        val places = new Rail[Int](numP, -1n);
        var i:Long = 0;
        for (pl in Place.places())
            places(i++) = pl.id as Int;
        val fin = LowLevelFinish.make(places);
        val gr = fin.getGr();
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                if (verbose>=1) debug("==== Replicator.finalizeReplication  moving from " + here + " to " + Place(p));
                if (Place(p).isDead() || p == -1n) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
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
    
    static def pendingAsyncRequestsExists() {
        try {
            glock.lock();
            return pendingMaster.size() != 0 || pendingBackup.size() != 0;
        } finally {
            glock.unlock();
        }
    }
    
    static def waitForZeroPending() {
        var count:Long = 0;
        if (verbose>=1) debug(">>>> waitForZeroPending called" );
        while (pending.get() != 0n || pendingAsyncRequestsExists() ) {
            System.threadSleep(100); // release the CPU to more productive pursuits
            count++;
            if (count%10000 == 0) {
                glock.lock();
                var str:String = "pendingMaster = ";
                for (e in pendingMaster.entries()) {
                    str += e.getKey() + ":" + e.getValue().id + " , ";
                }
                var str2:String = "pendingBackup = ";
                for (e in pendingBackup.entries()) {
                    str2 += e.getKey() + ":" + e.getValue().id + " , ";
                }
                glock.unlock();
                if (verbose>=1) debug("==== waitForZeroPending waiting for: " + str + " : " + str2 );
            }
        }
        pending.set(-2000n); //this means the main finish has completed its work
        if (verbose>=1) debug("<<<< waitForZeroPending returning" );
    }
    
}