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

package x10.xrx;

import x10.util.Set;
import x10.util.HashSet;
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.TxConfig;
import x10.xrx.TxStoreConflictException;
import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.xrx.TxStoreFatalException;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.util.concurrent.Condition;
import x10.util.GrowableRail;

/*
 * Failure reporting semantics:
 *    The last time we report an non-fatal error here is during prepare:
 *    failures of the master leads to DPEs, and conflicts leads to CE
 *    
 *    We supress DPEs occuring during commit and abort
 *    We always supress DPEs occuring to the slaves
 * */
public class TxResilient extends Tx {
    private transient var gcId:FinishResilient.Id;
    private transient var readOnlyMasters:Set[Int] = null; //read-only masters in a non ready-only transaction
    private transient var masterSlave:HashMap[Int, Int]; 
    private transient var pending:HashMap[TxMember, Int];     //pending resilient communication
    private transient var ph2Started:Boolean;//false
    
    private transient var backupId:Int;
    private transient var pendingOnBackup:Boolean = false;

    private static val MASTER = 0n;
    private static val SLAVE = 1n;
    private static val DEAD_SLAVE = -1n;
    
    public static struct TxMember(place:int,ptype:int) {
        public def toString() = "<"+place+","+ptype+">";
    }
    
    public def this(plh:PlaceLocalHandle[LocalStore[Any]], id:Long) {
        super(plh, id);
    }
    
    protected def getAddMemberPrintMsg(m:Int, ro:Boolean, tag:String) {
        return "Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+gcId+"] "+tag+"["+m+"] readOnly["+ro+"] ...";
    }
    
    public def initialize(fid:FinishResilient.Id, backup:Int) {
        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+fid+"] activity["+Runtime.activity()+"] backup["+backup+"] initialize called ...");
        gcId = fid;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
        vote = true;
        readOnly = true;
        ph2Started = false;
        backupId = backup;
    }

    public def initializeNewMaster(fid:FinishResilient.Id,
            _mem:Set[Int], _excs:GrowableRail[CheckedThrowable], _ro:Boolean, backup:Int) {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+fid+"] activity["+Runtime.activity()+"] initializeNewMaster called ...");
        gcId = fid;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
        vote = true;
        ph2Started = false;
        members = _mem;
        excs = _excs;
        readOnly = _ro;
        backupId = backup;
    }
    
    public def getBackupClone():Tx { 
        try {
            lock.lock();
            val o = new TxResilient(plh,id);
            o.readOnly = readOnly;
            if (members != null) {
                o.members = new HashSet[Int]();
                for (mem in members)
                    o.members.add(mem);
            }
            if (excs != null) {
                o.excs = new GrowableRail[CheckedThrowable](excs.size());
                o.excs.addAll(excs);
            }
            return o;
        } finally {
            lock.unlock();
        }
    }
    
    public def finalizeWithBackup(finObj:Releasable, abort:Boolean, backupId:Int) {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backup["+backupId+"] finalize abort="+abort+" ...");
        this.finishObj = finObj;
        this.backupId = backupId;
        resilient2PC(abort);
    }
    
    public def finalize(finObj:Releasable, abort:Boolean) {
        finalizeWithBackup(finObj, abort, -1n);
    }
    
    public def finalizeLocal(finObj:Releasable, abort:Boolean) {
        finalizeWithBackup(finObj, abort, -1n);
    }

    public def resilient2PC(abort:Boolean) {
        if (abort) {
            //addExceptionUnsafe(new TxStoreConflictException());
            abortMastersOnly();
        } else {
            prepare();
        }
    }
    
    public def markAsCommitting() {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backup running markAsCommitting ...");
        vote = true;
    }
    
    public def backupFinalize(finObj:Releasable) {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backupFinalize vote["+vote+"] ...");
        masterSlave = plh().getMapping(members);
        this.finishObj = finObj;
        commitOrAbort(vote);
    }
    
    private def abortMastersOnly() {   
        val liveMasters:Set[Int] = new HashSet[Int]();
        lock.lock(); //altering the counts must be within lock/unlock
        ph2Started = true;
        pending = new HashMap[TxMember,Int]();
        count = 0n;
        for (m in members) {
            if (Place(m as Long).isDead())
                continue;
            count++;
            FinishResilient.increment(pending, TxMember(m, MASTER));
            liveMasters.add(m);
        }
         
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] abortMastersOnly  count="+count+" ...");
        lock.unlock();
        
        abort(liveMasters);
    }
    
    private def abort(liveMasters:Set[Int]) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcId = this.gcId;
        val plh = this.plh;
        
        for (p in liveMasters) {
            at (Place(p)) @Immediate("abort_mastersOnly_request") async {
                //gc
                optimisticGC(gcId);
                plh().getMasterStore().abort(id);
                val me = here.id as Int;
                at (gr) @Immediate("abort_mastersOnly_response") async {
                    (gr() as TxResilient).notifyAbortOrCommit(me, MASTER);
                }
            }
        }
    }
    
    private def printPending(tag:String) {
        var str:String = "";
        for (e in pending.entries()) {
            str += e.getKey() + " : " + e.getValue() + " , ";
        }
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] tag["+tag+"] pending["+str+"] count["+count+"] ...");
    }
    
    public def prepare() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val plh = this.plh;
        val readOnly = this.readOnly;
        
        var switchToAbort:Boolean = false;
        val liveMasters:Set[Int] = new HashSet[Int]();
        val liveSlaves:Set[Int] = new HashSet[Int]();
        
        masterSlave = plh().getMapping(members);
        
        lock.lock(); //altering the counts must be within lock/unlock
        pending = new HashMap[TxMember,Int]();
        count = 0n;
        for (m in members) {
            if (Place(m as Long).isDead()) {
                switchToAbort = true;
                continue;
            } else {
                count++;
                FinishResilient.increment(pending, TxMember(m, MASTER) );
                liveMasters.add(m);
            }
        }
        if (!switchToAbort && !readOnly) {
            for (m in members) {
                val slave = masterSlave.getOrThrow(m);
                if (!Place(slave as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(slave, SLAVE));
                    liveSlaves.add(slave as Int);
                }
                else {
                    masterSlave.put(m, DEAD_SLAVE);
                }
            }
        }
        printPending("PREPARE");
        lock.unlock();
        
        if (switchToAbort) { //if any of the masters is dead, abort the transaction
            abort(liveMasters);
        } else {
            for (p in liveMasters) {
                val slaveId = masterSlave.getOrElse(p, DEAD_SLAVE);
                at (Place(p)) @Immediate("prep_request_res") async {
                    Runtime.submitUncounted(()=> { //some times validation blocks, so we cannot execute this body within an immediate thread.
                        var vote:Boolean = true;
                        if (TxConfig.get().VALIDATION_REQUIRED) {
                            try {
                                plh().getMasterStore().validate(id);
                            }catch (e:Exception) {
                                debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] validation excp["+e.getMessage()+"] ...");
                                vote = false;
                            }
                        }        
                        var localReadOnly:Boolean = false;
                        if (!readOnly && slaveId != DEAD_SLAVE) {
                            val ownerPlaceIndex = plh().virtualPlaceId;
                            val log = plh().getMasterStore().getTxCommitLog(id);
                            if (log != null && log.size() > 0) {
                                at (Place(slaveId as Long)) @Immediate("slave_prep11") async {
                                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                                    at (gr) @Immediate("slave_prep_response11") async {
                                        (gr() as TxResilient).notifyPrepare(slaveId, SLAVE, true /*vote*/, false /*is master RO*/); 
                                    }
                                }
                            }
                            else 
                                localReadOnly = true;
                        }
                        val isMasterRO = localReadOnly;
                        val v = vote;
                        val masterId = here.id as Int;
                        at (gr) @Immediate("prep_response_res11") async {
                            (gr() as TxResilient).notifyPrepare(masterId, MASTER, v, isMasterRO);
                        }
                    });
                }
            }
        }
    }
    
    private def notifyPrepare(place:Int, ptype:Int, v:Boolean, isMasterRO:Boolean) {
        var prep:Boolean = false;
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place="+place+" ptype="+ptype+" v="+v+" isMRO="+isMasterRO+" ...");
        lock.lock();
        if (!Place(place as Long).isDead()) {
            if (ptype == MASTER) {
                count--;
                if (pending.getOrElse(TxMember(place, MASTER),-1n) == -1n)
                    debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare FATALERROR place["+place+"] type["+MASTER+"] count="+count+" doesn't exist...");
       
                FinishResilient.decrement(pending, TxMember(place, MASTER));
                if (!readOnly && isMasterRO) { //we don't except an acknowledgement from the slave
                    val slave = masterSlave.getOrElse(place, DEAD_SLAVE);
                    if (slave != DEAD_SLAVE && !Place(slave as Long).isDead()) {
                        count--;
                        FinishResilient.decrement(pending, TxMember(slave, SLAVE));
                    }
                    else {
                        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place="+place+" ptype="+ptype+" isMRO="+isMasterRO+" slave is dead, don't decrement count");
                    }
                }
                
            } else { //drop preparation message from a slave whose master died because notifyPlaceDead will remove them
                for (m in members) {
                    if (masterSlave.getOrElse(m, DEAD_SLAVE) == place && !Place(m).isDead()) {
                        count--;
                        FinishResilient.decrement(pending, TxMember(place, SLAVE));
                        break;
                    }
                }
            }
            
            vote = vote & v;
            if (ptype == MASTER && isMasterRO) {
                if (readOnlyMasters == null)
                    readOnlyMasters = new HashSet[Int]();
                readOnlyMasters.add(place);
            }
            if (count == 0n) {
                prep = true;
                if (!vote) //don't overwrite fatal exceptions
                    addExceptionUnsafe(new TxStoreConflictException(here + " Tx["+id+"] validation failed", here));
            }
        }
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place["+place+"] ptype="+ptype+" localVote["+v+"] vote["+vote+"] completed, count="+count+" ...");
        lock.unlock();
        
        if (prep) {
            if (backupId != -1n && vote) {
                updateBackup();
            }
            else
                commitOrAbort(vote);
        }
    }
    
    protected def commitOrAbort(isCommit:Boolean) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcId = this.gcId;
        val plh = this.plh;
        
        val liveMasters:Set[Int] = new HashSet[Int]();
        val liveSlaves:Set[Int] = new HashSet[Int]();
        
        lock.lock(); //altering the counts must be within lock/unlock
        ph2Started = true;
        pending = new HashMap[TxMember,Int]();
        count = 0n;
        if (readOnly) {
            for (m in members) {
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(m, MASTER));
                    liveMasters.add(m);
                }
            }
        } else {
            for (e in masterSlave.entries()) {
                val m = e.getKey();
                val s = e.getValue();
                
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(m, MASTER));
                    liveMasters.add(m);
                }
                if (readOnlyMasters == null || !readOnlyMasters.contains(m)) {
                    if (!Place(s as Long).isDead()) {
                        count++;
                        FinishResilient.increment(pending, TxMember(s, SLAVE));
                        liveSlaves.add(s);
                    }
                }
            }
        }
        printPending("COMMIT_OR_ABORT");
        lock.unlock();
        
        for (p in liveMasters) {
            at (Place(p)) @Immediate("abort_master_request") async {
                //gc
                optimisticGC(gcId);

                if (isCommit)
                    plh().getMasterStore().commit(id);
                else
                    plh().getMasterStore().abort(id);
                
                val me = here.id as Int;
                at (gr) @Immediate("abort_master_response") async {
                    (gr() as TxResilient).notifyAbortOrCommit(me, MASTER);
                }
            }            
        }
        
        for (p in liveSlaves) {
            at (Place(p)) @Immediate("abort_slave_request") async {
                if (isCommit)
                    plh().slaveStore.commit(id);
                else
                    plh().slaveStore.abort(id);
                
                val me = here.id as Int;
                at (gr) @Immediate("abort_slave_response") async {
                    (gr() as TxResilient).notifyAbortOrCommit(me, SLAVE);
                }
            }            
        }
    }
    
    
    private def notifyAbortOrCommit(place:Int, ptype:Int) {
        var rel:Boolean = false;
        
        try {
            lock.lock(); //altering the counts must be within lock/unlock
            if (!Place(place as Long).isDead()) {
                count--;
                if (pending == null) {
                    debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit FATALERROR pending is NULL");
                    throw new TxStoreFatalException("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit FATALERROR pending is NULL");
                }

                if (pending.getOrElse(TxMember(place, ptype),-1n) == -1n)
                    debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit FATALERROR place["+place+"] type["+ptype+"] count="+count+" doesn't exist...");
                
                FinishResilient.decrement(pending, TxMember(place, ptype));
                if (count == 0n) {
                    rel = true;
                    pending = null;
                }
            }
            debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit place["+place+"]  count="+count+" ...");
        } finally {
            lock.unlock();
        }
        
        if (rel) {
            release();
        }
    }
    
    private def optimisticGC(fid:FinishResilient.Id) {
        if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC) {
            FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObject(fid);
        } else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC){
            FinishResilientOptimistic.OptimisticRemoteState.deleteObject(fid);
        }
    }
    
    public def isImpactedByDeadPlaces(newDead:HashSet[Int]) {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces called");
        try {
            lock.lock();
            if (backupId != -1n && newDead.contains(backupId)) {
                debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning true, backup is: " + backupId);
                return true;
            }
            if (pending != null) {
                for (key in pending.keySet()) {
                    if (newDead.contains(key.place)) {
                        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning true, pending contains: " + key.place);
                        return true;
                    }
                }
            }
            debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning false");
            return false;
        } finally {
            lock.unlock();
        }
    }
    
    public def notifyPlaceDeath() {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead started");
        var rel:Boolean = false;
        lock.lock();
        val oldCnt = count;
        val toRemove = new HashSet[TxMember]();
        if (pending != null) {
            for (key in pending.keySet()) {
                if (Place(key.place as Long).isDead()) {
                    toRemove.add(key);
                    
                    var slave:Int = DEAD_SLAVE;
                    if (masterSlave != null) {
                        slave = masterSlave.getOrElse(key.place, DEAD_SLAVE);
                    }
                    
                    if (!ph2Started && key.ptype == MASTER) {
                        vote = false;
                    
                        //if the master died, don't except a message from the slave 
                        if (slave != DEAD_SLAVE && pending.getOrElse(TxMember(slave, SLAVE), -1000n) != -1000n) {
                            toRemove.add(TxMember(slave, SLAVE));
                        }
                    }
                    
                    if (slave != DEAD_SLAVE && Place(slave as Long).isDead()) {
                        addExceptionUnsafe(new TxStoreFatalException("Tx["+id+"] lost both master["+key.place+"] and slave["+slave+"] "));
                    }
                }
            }
        }
        
        for (t in toRemove) {
            if (!ph2Started && t.ptype == MASTER)
                addExceptionUnsafe( new DeadPlaceException( Place(t.place as Long) ) );
            
            val cnt = pending.remove(t);
            count -= cnt;
            debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead remove place="+t.place+" type="+t.ptype+" cnt="+cnt+" count="+count+"...");
        }
        
        if (oldCnt > 0 && count == 0n) {
            pending = null;
            rel = true;
        }
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead completed, pending count=" + count);
        lock.unlock();
        
        if (rel) {
            if (ph2Started)
                release();
            else
                commitOrAbort(vote);
        }
    }
    
    private static def debug(msg:String) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println( msg );
    }
    
    private def updateBackup() {
        val bId:Int;
        lock.lock();
        pendingOnBackup = true;
        bId = backupId;
        lock.unlock();
        val gcId = this.gcId;
        val gr = this.gr;
        val backup = Place(bId as Long);
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] updateBackup place"+backup+" ...");
        at (backup) @Immediate("notify_backup_commit") async {
            val bFin = FinishReplicator.findBackupOrThrow(gcId) as FinishResilientOptimistic.OptimisticBackupState;
            bFin.notifyCommit();
            at (gr) @Immediate("notify_backup_commit_resp") async {
                (gr() as TxResilient).notifyBackupUpdated();
            }
        }
    }
    
    public def notifyBackupChange(newBackupId:Int) {
        val restart:Boolean;
        lock.lock();
        val old = backupId;
        backupId = newBackupId;
        restart = pendingOnBackup;
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyBackupChange newBackup["+newBackupId+"] restart["+restart+"] ...");
        lock.unlock();
        if (restart)
            updateBackup();
    }
    
    private def notifyBackupUpdated() {
        debug("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] received notifyBackupUpdated ...");
        commitOrAbort(vote);
    }
}
