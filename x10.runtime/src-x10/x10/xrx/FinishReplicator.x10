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
    
    public static glock = new Lock(); //global lock for accessing all the static values below\\
    private static val NUM_PLACES = Place.numPlaces() as Int;
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
    private static val postActions = new HashMap[Long,(Boolean, FinishResilient.Id)=>void]();
    
    protected static struct PendingActivity(dst:Long, fs:FinishState, bodyBytes:Rail[Byte], prof:x10.xrx.Runtime.Profile) {}
    private static val NULL_PENDING_ACT = PendingActivity(-1, null, null, null);
    
    private static val SUCCESS = 0n;
    private static val TARGET_DEAD = 1n;
    private static val LEGAL_ABSENCE = 2n;

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
    
    //we don't insert a master request if master is dead
    private static def addMasterPending(req:FinishRequest, postSendAction:(Boolean, FinishResilient.Id)=>void) {
        try {
            glock.lock();
            if (Place(req.masterPlaceId).isDead())
                return TARGET_DEAD;
            pendingMaster.put(req.num, req);
            if (postSendAction != null)
                postActions.put(req.num, postSendAction);
            if (verbose>=1) debug("<<<< Replicator.addMasterPending(id="+req.id+", num="+req.num+") returned SUCCESS");
            return SUCCESS;
        } finally {
            glock.unlock();
        }
    }
    
    //we don't insert a backup request if backup is dead
    private static def masterToBackupPending(num:Long, masterPlaceId:Int, backupPlaceId:Int, submit:Boolean, parentId:FinishResilient.Id) {
        try {
            glock.lock();
            val req = pendingMaster.remove(num);
            if (req != null) {
                req.outSubmit = submit;   //in
                req.parentId = parentId; //in
                req.backupPlaceId = backupPlaceId; //in
                /////inReq.outAdopterId = req.outAdopterId; //out
                /////inReq.outSubmit = submit; //out
                if (req.reqType == FinishRequest.ADD_CHILD) /*in some cases, the parent may be transiting at the same time as its child, */ 
                    req.parentId = parentId;                /*the child may not find the parent's backup during globalInit, so it needs to create it*/
                
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
                    throw new Exception (here + " FATAL ERROR, pending master request not found although master is alive");
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

    private static def finalizeAsyncExec(num:Long, id:FinishResilient.Id, backupPlaceId:Int) {
        try {
            glock.lock();
            val req = pendingBackup.remove(num);
            if (verbose>=1) debug(">>>> Replicator.finalizeAsyncExec(id="+id+", num="+num+", backupPlace="+backupPlaceId+") called");
            if (req == null) {
                if (!Place(backupPlaceId).isDead())
                    throw new Exception (here + " FATAL ERROR, finalizeAsyncExec(id="+id+") pending backup request not found although backup is alive");
            }
            else {
                val postSendAction = postActions.remove(num); 
                if (postSendAction != null) {
                    if (verbose>=1) debug("==== Replicator.finalizeAsyncExec(id="+id+") executing postSendAction(submit="+req.outSubmit+",adopterId="+req.outAdopterId+")");
                    postSendAction(req.outSubmit, req.outAdopterId);
                }
                if (verbose>=1) debug("<<<< Replicator.finalizeAsyncExec(id="+id+", num="+num+", backupPlace="+backupPlaceId+") returning");
                FinishRequest.deallocReq(req);
            }
        } finally {
            glock.unlock();
        }
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState):void {
        val rc = addMasterPending(req, null);
        if (rc == SUCCESS)
            asyncExecInternal(localMaster, req.getGR(), 
                    req.id, req.masterPlaceId, req.reqType, req.parentId, req.finSrc, req.finKind,
                    req.map, req.childId, req.srcId, req.dstId, req.kind, req.ex, req.toAdopter); //we retry this line when needed
        else 
            handleMasterDied(req);
    }
    
    static def asyncExec(req:FinishRequest, localMaster:FinishMasterState, preSendAction:()=>void, postSendAction:(Boolean, FinishResilient.Id)=>void):void {
        preSendAction();
        val rc = addMasterPending(req, postSendAction);
        if (rc == SUCCESS)
            asyncExecInternal(localMaster, req.getGR(), 
                req.id, req.masterPlaceId, req.reqType, req.parentId, req.finSrc, req.finKind,
                req.map, req.childId, req.srcId, req.dstId, req.kind, req.ex, req.toAdopter ); //we retry this line when needed
        else
            handleMasterDied(req);
    }
    
    //FIXME: return adoptedId for PESSIMISTIC
    //FIXME: reduce serialization
    private static def asyncExecInternal(localMaster:FinishMasterState, gr:GlobalRef[FinishRequest],
            id:FinishResilient.Id, masterPlaceId:Int, reqType:Int, parentId:FinishResilient.Id, finSrc:Int, finKind:Int,
            map:HashMap[FinishResilient.Task,Int], 
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        val master = Place(masterPlaceId);
        if (master.id == here.id) {
            if (verbose>=1) debug(">>>> Replicator(id="+id+").asyncExecInternal local" );
            val mFin = localMaster != null ? findMasterOrAdd(id, localMaster) : findMaster(id);
            if (mFin == null)
                throw new Exception (here + " FATAL ERROR, master(id="+id+") is null1 while processing req["+reqType+", srcId="+srcId+", dstId="+dstId+"]");
            val mresp = mFin.exec(id, reqType, parentId, finSrc, finKind,
                    map, childId, srcId, dstId, kind, ex, toAdopter);
            if (mresp.excp != null && mresp.excp instanceof MasterMigrating) {
                if (verbose>=1) debug(">>>> Replicator(id="+id+").asyncExecInternal MasterMigrating1, try again after 10ms" );
                Runtime.submitUncounted( ()=>{
                    System.threadSleep(10);
                    asyncExecInternal(null, gr, id, masterPlaceId, reqType, parentId, finSrc, finKind,
                            map, childId, srcId, dstId, kind, ex, toAdopter );
                });
            }
            else {
                asyncMasterToBackup(gr, mresp);
            }
        } else {
            if (verbose>=1) debug(">>>> Replicator(id="+id+").asyncExecInternal remote" );
            at (master) @Immediate("async_master_exec") async {
                val mFin = findMaster(id);
                if (mFin == null)
                    throw new Exception (here + " FATAL ERROR, master(id="+id+") is null1 while processing req["+reqType+", srcId="+srcId+", dstId="+dstId+"]");

                val mresp = mFin.exec(id, reqType, parentId, finSrc, finKind,
                        map, childId, srcId, dstId, kind, ex, toAdopter);
                at (gr) @Immediate("async_master_exec_response") async {
                    val reqx = (gr as GlobalRef[FinishRequest]{self.home == here})();
                    if (mresp == null)
                        throw new Exception (here + " id="+reqx.id+" FATAL ERROR mresp is null");
                    if (mresp.excp != null && mresp.excp instanceof MasterMigrating) {
                        if (verbose>=1) debug(">>>> Replicator(id="+reqx.id+").asyncExecInternal MasterMigrating2, try again after 10ms" );
                        //we cannot block within an immediate thread
                        Runtime.submitUncounted( ()=>{
                            System.threadSleep(10); 
                            asyncExecInternal(null, gr, reqx.id, reqx.masterPlaceId, reqx.reqType, reqx.parentId, reqx.finSrc, reqx.finKind,
                                    reqx.map, reqx.childId, reqx.srcId, reqx.dstId, reqx.kind, reqx.ex, reqx.toAdopter);
                        });
                    }
                    else {
                        asyncMasterToBackup(gr, mresp);
                    }
                }
            }
        }
    }
    
    static def asyncMasterToBackup(gr:GlobalRef[FinishRequest], mresp:MasterResponse) {
        val req = (gr as GlobalRef[FinishRequest]{self.home == here})();
        val num = req.num;
        val id = req.id;
        val idStr = "<"+id.home+","+id.id+">";
        val masterPlaceId = req.masterPlaceId;
        val reqType = req.reqType;
        val finSrc = req.finSrc;
        val finKind = req.finKind;
        val map = req.map;
        val childId = req.childId;
        val srcId = req.srcId;
        val dstId = req.dstId;
        val kind = req.kind;
        val ex = req.ex;
        val toAdopter = req.toAdopter;
        val transitSubmitDPE = mresp.transitSubmitDPE;
        val parentId = (req.reqType == FinishRequest.ADD_CHILD) ? mresp.parentId : req.parentId ;
        
        if (verbose>=1) debug(">>>> Replicator(id="+id+").asyncMasterToBackup => backupPlaceId = " + mresp.backupPlaceId + " submit = " + mresp.submit );
        assert mresp.backupPlaceId != -1n : here + " fatal error [id="+id+"], backup -1 means master had a fatal error before reporting its backup value";
        if (mresp.backupChanged) {
            updateBackupPlace(id.home, mresp.backupPlaceId);
        }
        val backupGo = ( mresp.submit || (mresp.transitSubmitDPE && reqType == FinishRequest.TRANSIT));
        if (backupGo) {
            val backupPlaceId = mresp.backupPlaceId; //backupPlace = -1 here
            val rc = masterToBackupPending(num, masterPlaceId, backupPlaceId, mresp.submit, mresp.parentId);
            if (rc == TARGET_DEAD) {
                //ignore backup and go ahead with post processing
                handleBackupDied(req);
            } else if (rc == SUCCESS){ //normal path
                val backup = Place(backupPlaceId);
                val createOk = reqType == FinishRequest.ADD_CHILD || 
                        /*in some cases, the parent may be transiting at the same time as its child, 
                          the child may not find the parent's backup during globalInit, so it needs to create it*/
                        reqType == FinishRequest.TRANSIT ||
                        reqType == FinishRequest.EXCP ||
                        (reqType == FinishRequest.TERM && id.home == here.id as Int);
                
                if (backup.id == here.id) {
                    if (verbose>=1) debug("==== Replicator(id="+id+").asyncMasterToBackup backup local");
                    val bFin:FinishBackupState;
                    if (createOk)
                        bFin = findBackupOrCreate(id, parentId, Place(finSrc), finKind);
                    else
                        bFin = findBackupOrThrow(id, "asyncMasterToBackup local" + idStr);
                    val bexcp = bFin.exec(id, masterPlaceId, reqType, parentId, finSrc, finKind,
                            map, childId, srcId, dstId, kind, ex, toAdopter, transitSubmitDPE);
                    processBackupResponse(bexcp, req, backupPlaceId);
                }
                else {
                    if (verbose>=1) debug("==== Replicator(id="+id+").asyncMasterToBackup moving to backup " + backup);
                    at (backup) @Immediate("async_backup_exec") async {
                        if (verbose>=1) debug("==== Replicator(id="+id+").asyncMasterToBackup reached backup ");
                        val bFin:FinishBackupState;
                        if (createOk)
                            bFin = findBackupOrCreate(id, parentId, Place(finSrc), finKind);
                        else
                            bFin = findBackupOrThrow(id, "asyncMasterToBackup remote" + idStr);
                        val bexcp = bFin.exec(id, masterPlaceId, reqType, parentId, finSrc, finKind,
                                map, childId, srcId, dstId, kind, ex, toAdopter, transitSubmitDPE);
                        if (verbose>=1) debug("==== Replicator(id="+id+").asyncMasterToBackup moving to caller " + gr.home);
                        at (gr) @Immediate("async_backup_exec_response") async {
                            val reqx = (gr as GlobalRef[FinishRequest]{self.home == here})();
                            if (reqx == null)
                                throw new Exception (here + " FATAL ERROR reqx(id="+id+") is null" );
                            processBackupResponse(bexcp, reqx, backupPlaceId);
                        }
                    }
                }                
            } //else is LEGAL ABSENCE => backup death is being handled by notifyPlaceDeath
        } else {
            if (verbose>=1) debug("==== Replicator(id="+id+").asyncMasterToBackup() backupGo = false");
            removeMasterPending(num);
        }
    }
    
    static def processBackupResponse(bexcp:Exception, req:FinishRequest, backupPlaceId:Int) {
        if (verbose>=1) debug(">>>> Replicator(id="+req.id+",num="+req.num+").processBackupResponse called" );
        if (bexcp != null && bexcp instanceof MasterChanged) {
            val ex = bexcp as MasterChanged;
            req.toAdopter = true;
            req.id = ex.newMasterId;
            req.masterPlaceId = ex.newMasterPlace;
            if (!OPTIMISTIC)
                req.outAdopterId = req.id;
            if (verbose>=1) debug("==== Replicator(id="+req.id+",num="+req.num+").processBackupResponse MasterChanged exception caught, newMasterId["+ex.newMasterId+"] newMasterPlace["+ex.newMasterPlace+"]" );
            asyncExecInternal(null, req.getGR(), 
                    req.id, req.masterPlaceId, req.reqType, req.parentId, req.finSrc, req.finKind,
                    req.map, req.childId, req.srcId, req.dstId, req.kind, req.ex, req.toAdopter);
        } else {
            finalizeAsyncExec(req.num, req.id, backupPlaceId);
        }
        if (verbose>=1) debug("<<<< Replicator(id="+req.id+",num="+req.num+").processBackupResponse returned" );
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
            handleMasterDied(req);
        }
        if (verbose>=1) debug("<<<< Replicator.submitDeadMasterPendingRequests returning");
    }
    
    static def handleMasterDied(req:FinishRequest) {
        prepareRequestForNewMaster(req);
        if (!OPTIMISTIC) {
            req.outAdopterId = req.id;
            req.outSubmit = false;
        }
        asyncExec(req, null);
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
            if (verbose>=1) debug("==== Replicator.handleBackupDied(id="+req.id+") calling postSendAction(submit="+req.outSubmit+",adopterId="+req.outAdopterId+")");
            postSendAction(req.outSubmit, req.outAdopterId);
        }
        if (verbose>=1) debug("<<<< Replicator.handleBackupDied(id="+req.id+") returning");
        FinishRequest.deallocReq(req);
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
                    backupExec(req, mresp);
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
        
        val id = req.id;
        val reqType = req.reqType;
        val parentId = req.parentId;
        val finSrc = req.finSrc;
        val finKind = req.finKind;
        val map = req.map;
        val childId = req.childId;
        val srcId = req.srcId;
        val dstId = req.dstId;
        val kind = req.kind;
        val ex = req.ex;
        val toAdopter = req.toAdopter;
        val masterPlaceId = req.masterPlaceId;
        
        if (masterPlaceId == here.id as Int) {
            val mFin = findMasterOrAdd(id, localMaster);
            assert (mFin != null) : here + " fatal error, master(id="+id+") is null";
            val resp = mFin.exec(id, reqType, parentId, finSrc, finKind, map, 
                    childId, srcId, dstId, kind, ex, toAdopter );
            if (resp.excp != null) { 
                throw resp.excp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+id+").masterExec req[type="+reqType+", srcId="+srcId+", dstId="+dstId+"]" );
            return resp;
        }
        
        val masterRes = new GlobalRef[MasterResponse](new MasterResponse());
        val master = Place(masterPlaceId);
        val rCond = ResilientCondition.make(master);
        val closure = (gr:GlobalRef[Condition]) => {
            at (master) @Immediate("master_exec") async {
                val mFin = findMaster(id);
                assert (mFin != null) : here + " fatal error, master(id="+id+") is null";
                val resp = mFin.exec(id, reqType, parentId, finSrc, finKind, map, 
                        childId, srcId, dstId, kind, ex, toAdopter);
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
    
    public static def backupExec(req:FinishRequest, mresp:MasterResponse) {
        val id = req.id;
        val masterPlaceId = req.masterPlaceId;
        val reqType = req.reqType;
        val finSrc = req.finSrc;
        val finKind = req.finKind;
        val map = req.map;
        val childId = req.childId;
        val srcId = req.srcId;
        val dstId = req.dstId;
        val kind = req.kind;
        val ex = req.ex;
        val toAdopter = req.toAdopter;
        val transitSubmitDPE = mresp.transitSubmitDPE;
        val parentId = (req.reqType == FinishRequest.ADD_CHILD) ? mresp.parentId : req.parentId ;
        val backupPlaceId = mresp.backupPlaceId;
        
        val createOk = reqType == FinishRequest.ADD_CHILD || /*in some cases, the parent may be transiting at the same time as its child, 
                                                                   the child may not find the parent's backup during globalInit, so it needs to create it*/
                       reqType == FinishRequest.TRANSIT ||
                       reqType == FinishRequest.EXCP ||
                       (reqType == FinishRequest.TERM && id.home == here.id as Int);
        if (verbose>=1) debug(">>>> Replicator(id="+id+").backupExec called createOK=" + createOk );
        if (backupPlaceId == here.id as Int) {
            val bFin:FinishBackupState;
            if (createOk)
                bFin = findBackupOrCreate(id, parentId, Place(finSrc), finKind);
            else
                bFin = findBackupOrThrow(id, "backupExec local");
            val bexcp = bFin.exec(id, masterPlaceId, reqType, parentId, finSrc, finKind,
                    map, childId, srcId, dstId, kind, ex, toAdopter, transitSubmitDPE);
            if (bexcp != null) { 
                throw bexcp;
            }
            if (verbose>=1) debug("<<<< Replicator(id="+id+").backupExec returning" );
        }
        val backupRes = new GlobalRef[BackupResponse](new BackupResponse());
        val backup = Place(backupPlaceId);
        val rCond = ResilientCondition.make(backup);
        val closure = (gr:GlobalRef[Condition]) => {
            at (backup) @Immediate("backup_exec") async {
                val bFin:FinishBackupState;
                if (createOk)
                    bFin = findBackupOrCreate(id, parentId, Place(finSrc), finKind);
                else
                    bFin = findBackupOrThrow(id, "backupExec remote");
                val r_excp = bFin.exec(id, masterPlaceId, reqType, parentId, finSrc, finKind,
                        map, childId, srcId, dstId, kind, ex, toAdopter, transitSubmitDPE);
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
        if (verbose>=1) debug("<<<< Replicator(id="+id+").backupExec returning " );
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
        var count:Int = 0n;
        try {
            glock.lock();
            for (e in fbackups.entries()) {
                val backup = e.getValue() as FinishResilientOptimistic.OptimisticBackupState;
                if (backup.getParentId() == parentId && backup.finSrc.id as Int == src && backup.getId().home == deadDst) {
                    if (verbose>=1) debug("== countChildrenBackups(parentId="+parentId+", src="+src+") found state " + backup.id);
                    count++;
                }
            }
            //no more backups under this parent from that src place should be created
            backupDeny.add(BackupDenyId(parentId, deadDst));
            if (verbose>=1) debug("<<<< countChildrenBackups(parentId="+parentId+") returning, count = " + count + " and parentId added to denyList");
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
        if (verbose>=1) debug("<<<< getNewDeadPlaces returning");
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
                if ( bFin.getId().id != -5555n && ( OPTIMISTIC && newDead.contains(bFin.getPlaceOfMaster()) || 
                    !OPTIMISTIC && newDead.contains(bFin.getId().home) ) ) {
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
        var maxIter:Int = NUM_PLACES;
        var i:Int = 0n;
        try {
            glock.lock();
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
            glock.unlock();
        }
        if (verbose>=1) debug("<<<< nominateMasterPlaceIfDead(idHome="+idHome+") returning m="+m);
        return m;
    }
    
    static def nominateBackupPlaceIfDead(idHome:Int) {
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

    
    static def findBackupOrThrow(id:FinishResilient.Id, tag:String):FinishBackupState {
        if (verbose>=1) debug(">>>> findBackupOrThrow(id="+id+") called");
        try {
            glock.lock();
            val bs = fbackups.getOrElse(id, null);
            if (bs == null) {
                throw new Exception(here + " ["+Runtime.activity()+"] FATAL ERROR backup(id="+id+") not found here tag["+tag+"]");
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
    
    //added to mark a place as a new backup when no backups are there yet
    static def createDummyBackup(idHome:Int) {
        if (verbose>=1) debug(">>>> createDummyBackup called");
        try {
            glock.lock();
            val id = FinishResilient.Id(idHome, -5555n);
            if (OPTIMISTIC)
                fbackups.put(id, new FinishResilientOptimistic.OptimisticBackupState(id, id, Place(-1), -1n));
            else
                fbackups.put(id, new FinishResilientPessimistic.PessimisticBackupState(id, id));
            
            if (verbose>=1) debug("<<<< createDummyBackup returning");
        } finally {
            glock.unlock();
        }
    }
    
    
    static def createOptimisticBackupOrSync(id:FinishResilient.Id, parentId:FinishResilient.Id, src:Place, finKind:Int, numActive:Long, 
            sent:HashMap[FinishResilient.Edge,Int], 
            transit:HashMap[FinishResilient.Edge,Int],
            excs:GrowableRail[CheckedThrowable], placeOfMaster:Int):FinishBackupState {
        if (verbose>=1) debug(">>>> createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") called ");
        try {
            glock.lock();
            var bs:FinishBackupState = fbackups.getOrElse(id, null);
            
            if (bs == null) {
                assert !backupDeny.contains(BackupDenyId(parentId,id.home)) : "must not be in denyList";
                bs = new FinishResilientOptimistic.OptimisticBackupState(id, parentId, src, finKind, numActive, 
                		sent, transit, excs, placeOfMaster);
                fbackups.put(id, bs);
                if (verbose>=1) debug("<<<< createOptimisticBackupOrSync(id="+id+", parentId="+parentId+") returning, created bs="+bs);
            } else {
                (bs as FinishResilientOptimistic.OptimisticBackupState).sync(numActive, sent, transit, excs, placeOfMaster);
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
    
    static def pendingAsyncRequestsExists() {
        try {
            glock.lock();
            return pendingMaster.size() != 0 || pendingBackup.size() != 0;
        } finally {
            glock.unlock();
        }
    }
    
    static def waitForZeroPending() {
        if (verbose>=1) debug(">>>> waitForZeroPending called" );
        while (pending.get() != 0n || pendingAsyncRequestsExists() ) {
            System.threadSleep(100); // release the CPU to more productive pursuits
        }
        pending.set(-2000n); //this means the main finish has completed its work
        if (verbose>=1) debug("<<<< waitForZeroPending returning" );
    }
}