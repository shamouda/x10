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
import x10.util.resilient.concurrent.ResilientLowLevelFinish;
import x10.util.concurrent.Lock;
import x10.io.Deserializer;
import x10.compiler.AsyncClosure;

public final class FinishReplicator {
    
    private static val NUM_PLACES = Place.numPlaces() as Int;
    
    private static fmastersLock = new Lock(); 
    private static val fmasters = new HashMap[FinishResilient.Id, FinishMasterState](); //the set of all masters
    
    private static fbackupsLock = new Lock();
    private static val fbackups = new HashMap[FinishResilient.Id, FinishBackupState](); //the set of all backups
    private static val backupDeny = new HashSet[BackupDenyId](); //backup deny list

    private static allDeadLock = new Lock();
    private static val allDead = new HashSet[Int](); //all discovered dead places
    
    private static backupMapLock = new Lock();
    private static val backupMap = new HashMap[Int, Int](); //backup place mapping
    
    private static masterMapLock = new Lock();
    private static val masterMap = new HashMap[Int, Int](); //master place mapping

    private static val verbose = System.getenv("X10_RESILIENT_VERBOSE") == null? 0 : Long.parseLong(System.getenv("X10_RESILIENT_VERBOSE"));
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //non-blocking transit
    private static pendingMasterLock = new Lock();
    private static val pendingMaster = new HashMap[Long,FinishRequest]();
    
    private static pendingBackupLock = new Lock();
    private static val pendingBackup = new HashMap[Long,FinishRequest]();
    private static val pendingBackupIndx = new HashMap[Int/*place*/,ArrayList[Long]/*req.num*/]();
    private static val postActions = new HashMap[Long,(Boolean, FinishResilient.Id)=>void]();
    private static val reqOutputs = new HashMap[Long,ReqOutput]();
    
    private static transitPendingActLock = new Lock();
    private static val transitPendingAct = new HashMap[Long,PendingActivity]();
    

    protected static struct PendingActivity(dst:Long, fs:FinishState, bodyBytes:Rail[Byte], prof:x10.xrx.Runtime.Profile) {}
    private static val NULL_PENDING_ACT = PendingActivity(-1, null, null, null);
    
    private static val SUCCESS = 0n;
    private static val TARGET_DEAD = 1n;
    private static val LEGAL_ABSENCE = 2n;

    private static val pending = new AtomicInteger(0n); //pending replication requests, used for final program termination
    
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
            transitPendingActLock.lock();
            transitPendingAct.put(num, PendingActivity(dstId, fs, bodyBytes, prof));
            //NOLOG if (verbose>=1) debug("<<<< Replicator.addPendingAct(id="+id+", num="+num+", fs="+fs+",bytes="+bodyBytes.size+") returned");
        } finally {
            transitPendingActLock.unlock();
        }
    }
    
    static def sendPendingAct(id:FinishResilient.Id, num:Long) {
        try {
            transitPendingActLock.lock();
            val pendingAct = transitPendingAct.getOrElse(num, NULL_PENDING_ACT);
            if( pendingAct == NULL_PENDING_ACT)
                throw new Exception (here + "["+Runtime.activity()+"] FATAL ERROR req[id="+id+", num="+num+"] lost its pending activity");
            transitPendingAct.remove(num);
            val preSendAction = ()=>{ };
            //NOLOG if (verbose>=1) debug("<<<< Replicator.sendPendingAct(id="+id+", num="+num+", dst="+pendingAct.dst+", fs="+pendingAct.fs+") returned");
            val bytes = pendingAct.bodyBytes;
            val wrappedBody = ()=> @AsyncClosure {
                val deser = new Deserializer(bytes);
                val bodyPrime = deser.readAny() as ()=>void;
                bodyPrime();
            };
            x10.xrx.Runtime.x10rtSendAsync(pendingAct.dst, wrappedBody, pendingAct.fs, pendingAct.prof, preSendAction);
        } finally {
            transitPendingActLock.unlock();
        }
    }
    
    //we don't insert a master request if master is dead
    private static def addMasterPending(req:FinishRequest, postSendAction:(Boolean, FinishResilient.Id)=>void,
    		reqOutput:ReqOutput) {
        if (Place(req.masterPlaceId).isDead())
            return TARGET_DEAD;
        
        pendingMasterLock.lock();
        pendingMaster.put(req.num, req);
        pendingMasterLock.unlock();
        
        pendingBackupLock.lock();
        if (postSendAction != null) {
            postActions.put(req.num, postSendAction);
        }
        reqOutputs.put(req.num, reqOutput);
        pendingBackupLock.unlock();
        //NOLOG if (verbose>=1) debug("<<<< Replicator.addMasterPending(id="+req.id+", num="+req.num+") returned SUCCESS");
        return SUCCESS;
    }
    
    private static def updateOutput(num:Long, submit:Boolean) {
        pendingBackupLock.lock();
        reqOutputs.getOrThrow(num).submit = submit;
        pendingBackupLock.unlock();
    }
    
    private static def updateOutput(num:Long, adopterId:FinishResilient.Id) {
        pendingBackupLock.lock();
        reqOutputs.getOrThrow(num).adopterId = adopterId;
        pendingBackupLock.unlock();
    }
    
    private static def updateOutput(num:Long, submit:Boolean, adopterId:FinishResilient.Id) {
        pendingBackupLock.lock();
        reqOutputs.getOrThrow(num).submit = submit;
        reqOutputs.getOrThrow(num).adopterId = adopterId;
        pendingBackupLock.unlock();
    }
    
    //we don't insert a backup request if backup is dead
    private static def masterToBackupPending(num:Long, masterPlaceId:Int, backupPlaceId:Int) {
        try {
            pendingMasterLock.lock();
            pendingBackupLock.lock();
            val req = pendingMaster.remove(num);
            if (req != null) {
                //NOLOG if (verbose>=1) debug(">>>> Replicator.masterToBackupPending(id="+req.id+", num="+num+") called, req found");
                if (Place(backupPlaceId).isDead()) {
                    //NOLOG if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+") returned TARGET_DEAD");
                    return TARGET_DEAD;
                }
                pendingBackup.put(num, req);
                var list:ArrayList[Long] = pendingBackupIndx.getOrElse(backupPlaceId, null);
                if (list == null) {
                    list = new ArrayList[Long]();
                    pendingBackupIndx.put(backupPlaceId, list);
                }
                list.add(num);
                //NOLOG if (verbose>=1) debug("==== Replicator.masterToBackupPending(id="+req.id+", num="+num+") pendingBackupIndx.add("+num+") successful");
                //NOLOG if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(id="+req.id+", num="+num+") returned SUCCESS");
                return SUCCESS;
            } else {
                //NOLOG if (verbose>=1) debug("<<<< Replicator.masterToBackupPending(num="+num+") req not found, masterDead? " + Place(masterPlaceId).isDead());
                if (!Place(masterPlaceId).isDead())
                    throw new Exception (here + " FATAL ERROR, pending master request not found although master is alive");
            }
            return LEGAL_ABSENCE;
        } finally {
            pendingMasterLock.unlock();
            pendingBackupLock.unlock();
        } 
    }
    
    private static def removeMasterPending(num:Long) {
        try {
            pendingMasterLock.lock();
            val req = pendingMaster.remove(num);
            //NOLOG if (verbose>=1) debug("<<<< Replicator.removeMasterPending(id="+req.id+", num="+num+") returned");
        } finally {
            pendingMasterLock.unlock();
        }
    }
    
    private static def getPendingBackupRequest(num:Long) {
        try {
            pendingBackupLock.lock();
            val b = pendingBackup.getOrElse(num, null);
            if (b == null)
                throw new Exception (here + " FATAL ERROR getPendingBackupRequest(num="+num+") returned null");
            return b;
        } finally {
            pendingBackupLock.unlock();
        } 
    }
    
    private static def getPendingMasterRequest(num:Long) {
        try {
            pendingMasterLock.lock();
            val m = pendingMaster.getOrElse(num, null);
            if (m == null)
                throw new Exception (here + " FATAL ERROR getPendingMasterRequest(num="+num+") returned null");
            return m;
        } finally {
            pendingMasterLock.unlock();
        } 
    }

    private static def finalizeAsyncExec(num:Long, backupPlaceId:Int) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator.finalizeAsyncExec(num="+num+", backupPlace="+backupPlaceId+") called");
        var req:FinishRequest = null;
        pendingBackupLock.lock();
        
        req = pendingBackup.remove(num);
        val list = pendingBackupIndx.getOrElse(backupPlaceId, null);
        if (list == null) {
            pendingBackupLock.unlock();
            throw new Exception (here + " FATAL ERROR finalizeAsyncExec(num="+num+", backupPlace="+backupPlaceId+") pendingBackupIndx has null list ");
        }
        if (!list.contains(num)) {
            pendingBackupLock.unlock();
            throw new Exception (here + " FATAL ERROR finalizeAsyncExec(num="+num+", backupPlace="+backupPlaceId+") list does not include num");
        }
        list.remove(num);
        //NOLOG if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(num="+num+", backupPlace="+backupPlaceId+") pendingBackupIndx.remove("+num+") successful");
        
        if (req == null) {
            try {
                if (!Place(backupPlaceId).isDead()) {
                    throw new Exception (here + " FATAL ERROR, finalizeAsyncExec(num="+num+") pending backup request not found although backup is alive");
                }
            } finally {
                pendingBackupLock.unlock();
            }
            return;
        }
        
        val postSendAction = postActions.remove(num);
        val output = reqOutputs.remove(num);
        pendingBackupLock.unlock();
        if (postSendAction != null) {
            //NOLOG if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+req.id+") executing postSendAction(submit="+output.submit+",adopterId="+output.adopterId+")");
            postSendAction(output.submit, output.adopterId);
        }
        FinishRequest.deallocReq(req);
        //NOLOG if (verbose>=1) debug("<<<< Replicator.finalizeAsyncExec(id="+req.id+", num="+num+", backupPlace="+backupPlaceId+") returning");
    }
    
    static def restartExec(req:FinishRequest, localMaster:FinishMasterState, reqOutput:ReqOutput):void {
        val rc = addMasterPending(req, null, reqOutput);
        if (rc == SUCCESS)
            asyncExecInternal(req, localMaster); //we retry this line when needed
        else 
            handleMasterDied(req);
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState):void {
        val rc = addMasterPending(req, null, new ReqOutput());
        if (rc == SUCCESS)
            asyncExecInternal(req, localMaster); //we retry this line when needed
        else 
            handleMasterDied(req);
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState, 
            preSendAction:()=>void, postSendAction:(Boolean, FinishResilient.Id)=>void):void {
        preSendAction();
        val rc = addMasterPending(req, postSendAction, new ReqOutput());
        if (rc == SUCCESS)
            asyncExecInternal(req, localMaster); //we retry this line when needed
        else
            handleMasterDied(req);
    }
    
    private static def asyncExecInternal(req:FinishRequest, localMaster:FinishMasterState) {
        val caller = here;
        val master = Place(req.masterPlaceId);
        val num = req.num;
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncExecInternal going to master " + master );
        if (master.id == here.id) {
            val mFin = localMaster != null ? findMasterOrAdd(req.id, localMaster) : findMaster(req.id);
            if (mFin == null)
                throw new Exception (here + " FATAL ERROR, master(id="+req.id+") is null");
            val mresp = mFin.exec(req);
            if (mresp.excp != null && mresp.excp instanceof MasterMigrating) {
                //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").asyncExecInternal MasterMigrating1, try again after 10ms" );
                Runtime.submitUncounted( ()=>{
                    val reqx = getPendingMasterRequest(num);
                    System.threadSleep(10);
                    asyncExecInternal(reqx, null);
                });
            }
            else {
                asyncMasterToBackup(caller, req, mresp);
            }
        } else {
            val bytes = req.bytes;
            if (bytes == null)
                throw new Exception (here + " FATAL ERROR, bytes null before moving to master " + master);
            at (master) @Immediate("async_master_exec") async {
                if (bytes == null)
                    throw new Exception (here + " FATAL ERROR, at master null bytes");
                val newReq = FinishRequest.make(bytes);
                val mFin = findMaster(newReq.id);
                if (mFin == null)
                    throw new Exception (here + " FATAL ERROR, master(id="+newReq.id+") is null while processing req["+newReq+"]");
                val mresp = mFin.exec(newReq);
                val mresp_backupPlaceId = mresp.backupPlaceId;
                val mresp_excp = mresp.excp;
                val mresp_submit = mresp.submit;
                val mresp_transitSubmitDPE = mresp.transitSubmitDPE;
                val mresp_backupChanged = mresp.backupChanged;
                val mresp_parentId = mresp.parentId;
                at (caller) @Immediate("async_master_exec_response") async {
                    val mresp2 = new MasterResponse(mresp_backupPlaceId, mresp_excp, mresp_submit, mresp_transitSubmitDPE, mresp_backupChanged, mresp_parentId);
                    if (mresp2.excp != null && mresp2.excp instanceof MasterMigrating) {
                        //NOLOG if (verbose>=1) debug(">>>> Replicator(num="+num+").asyncExecInternal MasterMigrating2, try again after 10ms" );
                        //we cannot block within an immediate thread
                        Runtime.submitUncounted( ()=>{
                            val reqx = getPendingMasterRequest(num);
                            System.threadSleep(10); 
                            asyncExecInternal(reqx, null);
                        });
                    }
                    else {
                        val reqx = getPendingMasterRequest(num);
                        asyncMasterToBackup(caller, reqx, mresp2);
                    }
                }
            }
        }
    }
    
    static def asyncMasterToBackup(caller:Place, req:FinishRequest, mresp:MasterResponse) {
        val num = req.num;
        val transitSubmitDPE = mresp.transitSubmitDPE;
        val parentId = (req.reqType == FinishRequest.ADD_CHILD) ? mresp.parentId : req.parentId ;
        /*in some cases, the parent may be transiting at the same time as its child, */ 
        /*the child may not find the parent's backup during globalInit, so it needs to create it*/
        
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+", num="+num+").asyncMasterToBackup => backupPlaceId = " + mresp.backupPlaceId + " submit = " + mresp.submit );
        if ( mresp.backupPlaceId == -1n)
            throw new Exception (here + " fatal error [id="+req.id+"], backup -1 means master had a fatal error before reporting its backup value");
        
        updateOutput(num, mresp.submit);
        if (mresp.backupChanged) {
            updateBackupPlace(req.id.home, mresp.backupPlaceId);
        }
        val backupGo = ( mresp.submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
        if (backupGo) {
            val backupPlaceId = mresp.backupPlaceId;
            val rc = masterToBackupPending(num, req.masterPlaceId, mresp.backupPlaceId);
            if (rc == TARGET_DEAD) {
                //ignore backup and go ahead with post processing
                handleBackupDied(req);
            } else if (rc == SUCCESS){ //normal path
                val backup = Place(backupPlaceId);
                val createOk = req.reqType == FinishRequest.ADD_CHILD || 
                        /*in some cases, the parent may be transiting at the same time as its child, 
                          the child may not find the parent's backup during globalInit, so it needs to create it*/
                        req.reqType == FinishRequest.TRANSIT ||
                        req.reqType == FinishRequest.EXCP ||
                        (req.reqType == FinishRequest.TERM && req.id.home == here.id as Int);
                
                if (backup.id == here.id) {
                    //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup backup local");
                    val bFin:FinishBackupState;
                    if (createOk)
                        bFin = findBackupOrCreate(req.id, parentId, Place(req.finSrc), req.finKind);
                    else
                        bFin = findBackupOrThrow(req.id, "asyncMasterToBackup local");
                    val bexcp = bFin.exec(req, transitSubmitDPE);
                    processBackupResponse(bexcp, num, backupPlaceId);
                }
                else {
                    //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup moving to backup " + backup);
                    val bytes = req.bytes;
                    if (bytes == null)
                        throw new Exception (here + " FATAL ERROR, bytes null before moving to backup " + backup);
                    at (backup) @Immediate("async_backup_exec") async {
                        if (bytes == null)
                            throw new Exception (here + " FATAL ERROR, at backup null bytes");
                        val newReq = FinishRequest.make(bytes);
                        //NOLOG if (verbose>=1) debug("==== Replicator(id="+newReq.id+").asyncMasterToBackup reached backup ");
                        val bFin:FinishBackupState;
                        if (createOk)
                            bFin = findBackupOrCreate(newReq.id, parentId, Place(newReq.finSrc), newReq.finKind);
                        else
                            bFin = findBackupOrThrow(newReq.id, "asyncMasterToBackup remote");
                        val bexcp = bFin.exec(newReq, transitSubmitDPE);
                        //NOLOG if (verbose>=1) debug("==== Replicator(id="+newReq.id+").asyncMasterToBackup moving to caller " + caller);
                        at (caller) @Immediate("async_backup_exec_response") async {
                            processBackupResponse(bexcp, num, backupPlaceId);
                        }
                    }
                }                
            } //else is LEGAL ABSENCE => backup death is being handled by notifyPlaceDeath
        } else {
            //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+").asyncMasterToBackup() backupGo = false");
            removeMasterPending(num);
        }
    }
    
    static def processBackupResponse(bexcp:Exception, num:Long, backupPlaceId:Int) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator(num="+num+").processBackupResponse called" );
        if (bexcp != null && bexcp instanceof MasterChanged) {
            val ex = bexcp as MasterChanged;
            Runtime.submitUncounted( ()=>{
                val req = getPendingMasterRequest(num);
                req.masterPlaceId = ex.newMasterPlace;
                req.toAdopter = true;
                req.id = ex.newMasterId;
                req.updateBytes();
                if (!OPTIMISTIC) {
                	updateOutput(num, ex.newMasterId);
                }
                //NOLOG if (verbose>=1) debug("==== Replicator(id="+req.id+",num="+req.num+").processBackupResponse MasterChanged exception caught, newMasterId["+ex.newMasterId+"] newMasterPlace["+ex.newMasterPlace+"]" );
                System.threadSleep(10); 
                asyncExecInternal(req, null);
            });
        } else {
            finalizeAsyncExec(num, backupPlaceId);
        }
        //NOLOG if (verbose>=1) debug("<<<< Replicator(num="+num+").processBackupResponse returned" );
    }
    
    static def submitDeadBackupPendingRequests(newDead:HashSet[Int]) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator.submitDeadBackupPendingRequests called");
        val set = new HashSet[FinishRequest]();
        try {
            pendingBackupLock.lock();
            for (dead in newDead) {
                val list = pendingBackupIndx.remove(dead);
                if (list != null) {
                    for (num in list) {
                        val req = pendingBackup.remove(num);
                        set.add(req);
                    }
                }
            }            
        } finally {
            pendingBackupLock.unlock();
        }
        //NOLOG if (verbose>=1) debug("==== Replicator.submitDeadBackupPendingRequests found "+set.size()+" requests");
        for (req in set) {
            //NOLOG if (verbose>=1) debug("==== Replicator.submitDeadBackupPendingRequests processing=> " + req);
            handleBackupDied(req);   
        }
        //NOLOG if (verbose>=1) debug("<<<< Replicator.submitDeadBackupPendingRequests returning");
    }
    
    static def submitDeadMasterPendingRequests(newDead:HashSet[Int]) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator.submitDeadMasterPendingRequests called");
        val set = new HashSet[FinishRequest]();
        try {
            pendingMasterLock.lock();
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
            pendingMasterLock.unlock();
        }
        //NOLOG if (verbose>=1) debug("==== Replicator.submitDeadMasterPendingRequests found "+set.size()+" requests");
        for (req in set) {
            handleMasterDied(req);
        }
        //NOLOG if (verbose>=1) debug("<<<< Replicator.submitDeadMasterPendingRequests returning");
    }
    
    static def handleMasterDied(req:FinishRequest) {
        prepareRequestForNewMaster(req);
        req.updateBytes();
        restartExec(req, null, new ReqOutput(false, req.id));
    }
    
    static def handleBackupDied(req:FinishRequest) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator.handleBackupDied(id="+req.id+") called");
        pendingBackupLock.lock();
        val postSendAction = postActions.remove(req.num);
        val output = reqOutputs.remove(req.num);
        pendingBackupLock.unlock();
        if (postSendAction != null) {
            //NOLOG if (verbose>=1) debug("==== Replicator.handleBackupDied(id="+req.id+") calling postSendAction(submit="+output.submit+",adopterId="+output.adopterId+")");
            postSendAction(output.submit, output.adopterId);
        }
        //NOLOG if (verbose>=1) debug("<<<< Replicator.handleBackupDied(id="+req.id+") returning");
        FinishRequest.deallocReq(req);
    }
    
    /**************** Blocking Replication Protocol **********************/
    public static def exec(req:FinishRequest):FinishResilient.ReplicatorResponse {
        return exec(req, null);
    }
    public static def exec(req:FinishRequest, localMaster:FinishMasterState):FinishResilient.ReplicatorResponse {
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() called");
        checkMainTermination(); 
        
        var submit:Boolean = false;
        var adopterId:FinishResilient.Id = FinishResilient.UNASSIGNED; //pessimistic only
        while (true) {
            try {
                val mresp:MasterResponse = masterExec(req, localMaster);
                submit = mresp.submit;
                //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").exec() masterDone =>" + " backupPlaceId = " + mresp.backupPlaceId + " submit = " + submit );
                
                if (mresp.backupPlaceId == -1n)
                    throw new Exception (here + " fatal error ["+req+"], backup -1 means master had a fatal error before reporting its backup value");
                
                if (mresp.backupChanged) {
                    updateBackupPlace(req.id.home, mresp.backupPlaceId);
                }
                
                val backupGo = ( submit || (mresp.transitSubmitDPE && req.reqType == FinishRequest.TRANSIT));
                if (backupGo) {
                    backupExec(req, mresp);
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
        FinishRequest.deallocReq(req); //reuse the finish request object
        return FinishResilient.ReplicatorResponse(submit, adopterId);
    }
    
    public static def masterExec(req:FinishRequest, localMaster:FinishMasterState) {
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+req.id+").masterExec called [" + req + "]" );
        
        /**AT_FINISH HACK**/
        if (req.id == FinishResilient.Id(0n,0n) && req.toAdopter) {
            return new MasterResponse();
        }
        if (req.masterPlaceId == here.id as Int) {
            val mFin = findMasterOrAdd(req.id, localMaster);
            if (mFin == null)
                throw new Exception (here + " FATAL ERROR, master(id="+req.id+") is null");
            val resp = mFin.exec(req);
            if (resp.excp != null) { 
                throw resp.excp;
            }
            //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec req["+req+"]" );
            return resp;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(req.masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val bytes = req.bytes;
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
            	val newReq = FinishRequest.make(bytes);
                val mFin = findMaster(newReq.id);
                if (mFin == null)
                    throw new Exception (here + " FATAL ERROR, master(id="+newReq.id+") is null");
                val resp = mFin.exec(newReq);
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
        //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+req.id+").masterExec returning [" + req + "]" );
        return resp;
    }
    
    public static def backupExec(req:FinishRequest, mresp:MasterResponse) {
        val id = req.id;
        val transitSubmitDPE = mresp.transitSubmitDPE;
        val parentId = (req.reqType == FinishRequest.ADD_CHILD) ? mresp.parentId : req.parentId ;
        val backupPlaceId = mresp.backupPlaceId;
        
        val createOk = req.reqType == FinishRequest.ADD_CHILD || /*in some cases, the parent may be transiting at the same time as its child, 
                                                                   the child may not find the parent's backup during globalInit, so it needs to create it*/
        		        req.reqType == FinishRequest.TRANSIT ||
        				req.reqType == FinishRequest.EXCP ||
                       (req.reqType == FinishRequest.TERM && id.home == here.id as Int);
        //NOLOG if (verbose>=1) debug(">>>> Replicator(id="+id+").backupExec called createOK=" + createOk );
        if (backupPlaceId == here.id as Int) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(req.id, parentId, Place(req.finSrc), req.finKind);
            else
                bFin = findBackupOrThrow(id, "backupExec local");
            val bexcp = bFin.exec(req, transitSubmitDPE);
            if (bexcp != null) { 
                throw bexcp;
            }
            //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+id+").backupExec returning" );
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val bytes = req.bytes;
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
            	val newReq = FinishRequest.make(bytes);
                val bFin:FinishBackupState;
                if (createOk)
                    bFin = findBackupOrCreate(newReq.id, parentId, Place(newReq.finSrc), newReq.finKind);
                else
                    bFin = findBackupOrThrow(newReq.id, "backupExec remote");
                val r_excp = bFin.exec(newReq, transitSubmitDPE);
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
        //NOLOG if (verbose>=1) debug("<<<< Replicator(id="+id+").backupExec returning " );
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
                    val closure = (gr:GlobalRef[Condition]) => {
                        at (backup) @Uncounted async {
                            var foundVar:Boolean = false;
                            var newMasterIdVar:FinishResilient.Id = FinishResilient.UNASSIGNED;
                            var newMasterPlaceVar:Int = -1n;
                            val bFin = findBackupOrThrow(id, "prepareRequestForNewMaster");
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
            curBackup = ((curBackup + 1) % NUM_PLACES) as Int;
        } while (initBackup != curBackup);
        if (!req.toAdopter)
            throw new Exception(here + " ["+Runtime.activity()+"] FATAL exception, cannot find backup for id=" + id);
        
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
            masterMapLock.lock();
            val m = masterMap.getOrElse(idHome, -1n);
            if (m == -1n)
                return idHome;
            else
                return m;
        } finally {
            masterMapLock.unlock();
        }
    }
    
    static def getBackupPlace(idHome:Int) {
        try {
            backupMapLock.lock();
            val b = backupMap.getOrElse(idHome, -1n);
            if (b == -1n)
                return ((idHome+1)%Place.numPlaces()) as Int;
            else
                return b;
        } finally {
            backupMapLock.unlock();
        }
    }
    
    
    static def isBackupOf(idHome:Int) {
        try {
            fbackupsLock.lock();
            for ( e in fbackups.entries()) {
                if (e.getKey().home == idHome)
                    return true;
            }
            return false;
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    static def searchBackup(idHome:Int, deadBackup:Int) {
        //NOLOG if (verbose>=1) debug(">>>> searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") called ");
        var b:Int = deadBackup;
        for (var c:Int = 1n; c < NUM_PLACES; c++) {
            val nextPlace = Place((deadBackup + c) % NUM_PLACES);
            //NOLOG if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") nextPlace is " + nextPlace + "  dead?" + nextPlace.isDead());
            if (nextPlace.isDead())
                continue;
            val searchResp = new GlobalRef[SearchBackupResponse](new SearchBackupResponse());
            val rCond = ResilientCondition.make(nextPlace);
            val closure = (gr:GlobalRef[Condition]) => {
                at (nextPlace) @Immediate("search_backup") async {
                    val found = FinishReplicator.isBackupOf(idHome);
                    //NOLOG if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") found="+found);
                    at (gr) @Immediate("search_backup_response") async {
                        val resp = (searchResp as GlobalRef[SearchBackupResponse]{self.home == here})();
                        resp.found = found;
                        gr().release();
                    }
                }
            };
            rCond.run(closure);
            
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
                    //NOLOG if (verbose>=1) debug("==== searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") result b="+b);
                    updateBackupPlace(idHome, b);
                    break;
                }
            }
        }
        if (Place(b).isDead())
            throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR could not find backup for idHome = " + idHome);
        //NOLOG if (verbose>=1) debug("<<<< searchBackup(idHome="+idHome+", deadBackup="+deadBackup+") returning b=" + b);
        return b;
    }
    
    
    
    static def updateMyBackupIfDead(newDead:HashSet[Int]) {
        try {
            val idHome = here.id as Int;
            //NOLOG if (verbose>=1) debug(">>>> updateMyBackupIfDead(idHome="+idHome+") called");
            backupMapLock.lock();
            var b:Int = backupMap.getOrElse(idHome, -1n);
            if (b == -1n) {
                b = ((idHome+1)%NUM_PLACES) as Int;
                //NOLOG if (verbose>=1) debug("==== updateMyBackupIfDead(idHome="+idHome+") not in backupMap, b="+b);
            }
            if (newDead.contains(b)) {
                b = ((b+1)%NUM_PLACES) as Int;
                backupMap.put(idHome, b);
                //NOLOG if (verbose>=1) debug("==== updateMyBackupIfDead(idHome="+idHome+") b died, newb="+b);
            }
            //NOLOG if (verbose>=1) debug("<<<< updateMyBackupIfDead(idHome="+idHome+") returning myBackup=" + b);
        } finally {
            backupMapLock.unlock();
        }
    }
    static def updateBackupPlace(idHome:Int, backupPlaceId:Int) {
        try {
            backupMapLock.lock();
            backupMap.put(idHome, backupPlaceId);
            //NOLOG if (verbose>=1) debug("<<<< updateBackupPlace(idHome="+idHome+",newPlace="+backupPlaceId+") returning");
        } finally {
            backupMapLock.unlock();
        }
    }
    
    static def updateMasterPlace(idHome:Int, masterPlaceId:Int) {
        try {
            masterMapLock.lock();
            masterMap.put(idHome, masterPlaceId);
            //NOLOG if (verbose>=1) debug("<<<< updateMasterPlace(idHome="+idHome+",newPlace="+masterPlaceId+") returning");
        } finally {
            masterMapLock.unlock();
        }
    }
    
    static def countChildrenBackups(parentId:FinishResilient.Id, deadDst:Int, src:Int) {
        var count:Int = 0n;
        try {
            fbackupsLock.lock();
            for (e in fbackups.entries()) {
                val backup = e.getValue() as FinishResilientOptimistic.OptimisticBackupState;
                if (backup.getParentId() == parentId && backup.finSrc.id as Int == src && backup.getId().home == deadDst) {
                    //NOLOG if (verbose>=1) debug("== countChildrenBackups(parentId="+parentId+", src="+src+") found state " + backup.id);
                    count++;
                }
            }
            //no more backups under this parent from that src place should be created
            backupDeny.add(BackupDenyId(parentId, deadDst));
            //NOLOG if (verbose>=1) debug("<<<< countChildrenBackups(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
        } finally {
            fbackupsLock.unlock();
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
            allDeadLock.lock();
            for (i in 0n..(NUM_PLACES - 1n)) {
                if (Place.isDead(i) && !allDead.contains(i)) {
                    newDead.add(i);
                    //NOLOG if (verbose>=1) debug("==== getNewDeadPlaces newDead=Place("+i+")");
                    allDead.add(i);
                }
            }
        } finally {
            allDeadLock.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< getNewDeadPlaces returning");
        return newDead;
    }
    
    static def getImpactedMasters(newDead:HashSet[Int]) {
        //NOLOG if (verbose>=1) debug(">>>> lockAndGetImpactedMasters called");
        val result = new HashSet[FinishMasterState]();
        try {
            fmastersLock.lock();
            for (e in fmasters.entries()) {
                val id = e.getKey();
                val mFin = e.getValue();
                if (mFin.isImpactedByDeadPlaces(newDead)) {
                    result.add(mFin);
                }
            }
        } finally {
            fmastersLock.unlock();
        }
        return result;
    }
    
    static def getImpactedBackups(newDead:HashSet[Int]) {
        //NOLOG if (verbose>=1) debug(">>>> lockAndGetImpactedBackups called");
        val result = new HashSet[FinishBackupState]();
        try {
            fbackupsLock.lock();
            for (e in fbackups.entries()) {
                val id = e.getKey();
                val bFin = e.getValue();
                if ( bFin.getId().id != -5555n && ( OPTIMISTIC && newDead.contains(bFin.getPlaceOfMaster()) || 
                    !OPTIMISTIC && newDead.contains(bFin.getId().home) ) ) {
                    result.add(bFin);
                }
            }
        } finally {
            fbackupsLock.unlock();
        }
        return result;
    }
    
    static def nominateMasterPlaceIfDead(idHome:Int) {
        var m:Int;
        var maxIter:Int = NUM_PLACES;
        var i:Int = 0n;
        try {
            masterMapLock.lock();
            allDeadLock.lock();
            m = masterMap.getOrElse(idHome,-1n);
            do {
                if (m == -1n)
                    m = ((idHome - 1 + NUM_PLACES)%NUM_PLACES) as Int;
                else
                    m = ((m - 1 + NUM_PLACES)%NUM_PLACES) as Int;
                i++;
            } while (allDead.contains(m) && i < maxIter);
            
            if (allDead.contains(m) || i == maxIter)
                throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR couldn't nominate a new master place for idHome=" + idHome);
            masterMap.put(idHome, m);
        } finally {
            masterMapLock.unlock();
            allDeadLock.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< nominateMasterPlaceIfDead(idHome="+idHome+") returning m="+m);
        return m;
    }
    
    static def nominateBackupPlaceIfDead(idHome:Int) {
        var b:Int;
        var maxIter:Int = NUM_PLACES;
        var i:Int = 0n;
        try {
            backupMapLock.lock();
            allDeadLock.lock();
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
            backupMapLock.unlock();
            allDeadLock.unlock();
        }
        //NOLOG if (verbose>=1) debug("<<<< nominateBackupPlace(idHome="+idHome+") returning b="+b);
        return b;
    }
    
    static def removeMaster(id:FinishResilient.Id) {
        try {
            fmastersLock.lock();
            fmasters.delete(id);
            //NOLOG if (verbose>=1) debug("<<<< removeMaster(id="+id+") returning");
        } finally {
            fmastersLock.unlock();
        }
    }
    
    static def removeBackup(id:FinishResilient.Id) {
        try {
            fbackupsLock.lock();
            fbackups.delete(id);
            //NOLOG if (verbose>=1) debug("<<<< removeBackup(id="+id+") returning");
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    static def addMaster(id:FinishResilient.Id, fs:FinishMasterState) {
        //NOLOG if (verbose>=1) debug(">>>> addMaster(id="+id+") called");
        try {
            fmastersLock.lock();
            fmasters.put(id, fs);
            //NOLOG if (verbose>=3) fs.dump();
            //NOLOG if (verbose>=1) debug("<<<< addMaster(id="+id+") returning");
        } finally {
            fmastersLock.unlock();
        }
    }
    
    static def findMaster(id:FinishResilient.Id):FinishMasterState {
        //NOLOG if (verbose>=1) debug(">>>> findMaster(id="+id+") called");
        try {
            fmastersLock.lock();
            val fs = fmasters.getOrElse(id, null);
            //NOLOG if (verbose>=1) debug("<<<< findMaster(id="+id+") returning");
            return fs;
        } finally {
            fmastersLock.unlock();
        }
    }
    
    static def findMasterOrAdd(id:FinishResilient.Id, mFin:FinishMasterState):FinishMasterState {
        //NOLOG if (verbose>=1) debug(">>>> findMasterOrAdd(id="+id+") called");
        try {
            fmastersLock.lock();
            var fs:FinishMasterState = fmasters.getOrElse(id, null);
            if (fs == null) {
                fs = mFin;
                fmasters.put(id, mFin);
            }
            //NOLOG if (verbose>=1) debug("<<<< findMasterOrAdd(id="+id+") returning");
            return fs;
        } finally {
            fmastersLock.unlock();
        }
    }

    
    static def findBackupOrThrow(id:FinishResilient.Id, tag:String):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            fbackupsLock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (bs == null) {
                throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR backup(id="+id+") not found here tag["+tag+"]");
            }
            else {
                //NOLOG if (verbose>=1) debug("<<<< findBackupOrThrow(id="+id+") returning, bs = " + bs);
                return bs;
            }
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    static def findBackup(id:FinishResilient.Id):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findBackup(id="+id+") called");
        try {
            fbackupsLock.lock();
            val bs = fbackups.getOrElse(id, null);
            //NOLOG if (verbose>=1) debug("<<<< findBackup(id="+id+") returning, bs = " + bs);
            return bs;
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    static def findBackupOrCreate(id:FinishResilient.Id, parentId:FinishResilient.Id, finSrc:Place, finKind:Int):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> findOrCreateBackup(id="+id+", parentId="+parentId+") called ");
        try {
            fbackupsLock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            if (bs == null) {
                if (backupDeny.contains(BackupDenyId(parentId, id.home))) {
                    //NOLOG if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") failed, BackupCreationDenied");
                    throw new BackupCreationDenied();
                    //no need to handle this exception; the caller has died.
                }
                
                if (OPTIMISTIC)
                    bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, finSrc, finKind);
                else
                    bs = new FinishResilientPessimistic.PessimisticBackupState(id, parentId);
                fbackups.put(id, bs);
                //NOLOG if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            }
            else {
                //NOLOG if (verbose>=1) debug("<<<< findOrCreateBackup(id="+id+", parentId="+parentId+") returning, found bs="+bs); 
            }
            return bs;
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    //added to mark a place as a new backup when no backups are there yet
    static def createDummyBackup(idHome:Int) {
        //NOLOG if (verbose>=1) debug(">>>> createDummyBackup called");
        try {
            fbackupsLock.lock();
            val id = FinishResilient.Id(idHome, -5555n);
            if (OPTIMISTIC)
                fbackups.put(id, new FinishResilientOptimistic.OptimisticBackupState(id, id, Place(-1), -1n));
            else
                fbackups.put(id, new FinishResilientPessimistic.PessimisticBackupState(id, id));
            
            //NOLOG if (verbose>=1) debug("<<<< createDummyBackup returning");
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    static def createOptimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, finKind:Int, numActive:Long, 
            sent:HashMap[FinishResilient.Edge,Int], 
            transit:HashMap[FinishResilient.Edge,Int],
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            fbackupsLock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                if ( backupDeny.contains(BackupDenyId(parentId,id.home)) )
                    throw new Exception (here + " FATAL ERROR must not be in denyList");
                
                bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, src, finKind, numActive, 
                		sent, transit, excs, placeOfMaster);
                fbackups.put(id, bs);
                //NOLOG if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientOptimistic.OptimisticBackupState).sync(numActive, sent, transit, excs, placeOfMaster);
                //NOLOG if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning from sync");
            }
            return bs;
        } finally {
            fbackupsLock.unlock();
        }
    }
    
    
    static def createPessimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, numActive:Long, 
            live:HashMap[FinishResilient.Task,Int], transit:HashMap[FinishResilient.Edge,Int], 
            liveAdopted:HashMap[FinishResilient.Task,Int], transitAdopted:HashMap[FinishResilient.Edge,Int], 
            children:HashSet[FinishResilient.Id],
            excs:GrowableRail[CheckedThrowable]):FinishBackupState {
        //NOLOG if (verbose>=1) debug(">>>> createPessimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            fbackupsLock.lock();
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
            fbackupsLock.unlock();
        }
    }
    
    //a very slow hack to terminate replication at all places before shutting down
    public static def finalizeReplication() {
        //NOLOG if (verbose>=1) debug("<<<< Replicator.finalizeReplication called " );
        
        val numP = NUM_PLACES;
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
    
    static def pendingAsyncRequestsExists() {
        try {
            pendingMasterLock.lock();
            pendingBackupLock.lock();
            return pendingMaster.size() != 0 || pendingBackup.size() != 0;
        } finally {
            pendingMasterLock.unlock();
            pendingBackupLock.unlock();
        }
    }
    
    static def waitForZeroPending() {
        //NOLOG if (verbose>=1) debug(">>>> waitForZeroPending called" );
        while (pending.get() != 0n || pendingAsyncRequestsExists() ) {
            System.threadSleep(100); // release the CPU to more productive pursuits
        }
        pending.set(-2000n); //this means the main finish has completed its work
        //NOLOG if (verbose>=1) debug("<<<< waitForZeroPending returning" );
    }
}