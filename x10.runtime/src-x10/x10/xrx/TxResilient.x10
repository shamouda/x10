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
import x10.xrx.txstore.TxLocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.compiler.Immediate;
import x10.xrx.txstore.TxConfig;
import x10.xrx.TxStoreConflictException;
import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.xrx.TxStoreFatalException;
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
    private transient var readOnlyMasters:Set[Int] = null; //read-only masters in a non ready-only transaction
    private transient var masterSlave:HashMap[Int, Int]; 
    private transient var pending:HashMap[TxMember, Int];     //pending resilient communication
    private transient var ph1Started:Boolean;//false
    private transient var ph2Started:Boolean;//false
    
    private transient var backupId:Int;
    private transient var pendingOnBackup:Boolean = false;

    private static val MASTER = 0n;
    private static val SLAVE = 1n;
    private static val DEAD_SLAVE = -1n;
    
    public static struct TxMember(place:int,ptype:int) {
        public def toString() = "("+place+","+ptype+")";
    }
    
    public def this(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
        super(plh, id);
    }
    
    protected def getAddMemberPrintMsg(m:Int, ro:Boolean, tag:String) {
        return "Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+gcId+"] "+tag+"["+m+"] readOnly["+ro+"] ...";
    }
    
    public def initialize(fid:FinishResilient.Id, backup:Int) {
        if (TxConfig.TM_DEBUG) {
            var str:String = "";
            if (members != null) {
                for (w in members) 
                    str += w + ":";
            }
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+fid+"] activity["+Runtime.activity()+"] backup["+backup+"] members["+str+"] initialize called ...");
        }
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
        if (TxConfig.TM_DEBUG) {
            var str:String = "";
            if (_mem != null) {
                for (w in _mem) {
                    str += w + " : ";
                }
            }
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] FID["+fid+"] activity["+Runtime.activity()+"] initializeNewMaster called, members are="+str+" ...");
        }
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
    
    public def finalizeWithBackup(finObj:Releasable, abort:Boolean, backupId:Int, isRecovered:Boolean) { 
        this.finishObj = finObj;
        if (gcId.home != 0n || FinishReplicator.PLACE0_BACKUP)
        	this.backupId = backupId;
        else
        	this.backupId = -1n;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backup["+backupId+"] finalize abort="+abort+" ...");
        resilient2PC(abort, isRecovered);
    }
    
    public def finalize(finObj:Releasable, abort:Boolean) {
        finalizeWithBackup(finObj, abort, -1n, false);
    }
    
    public def finalizeLocal(finObj:Releasable, abort:Boolean) {
        this.finishObj = finObj;
        if (abort) {
            plh().getMasterStore().abort(id);
            release();
        } else if (readOnly) { //validate and commit local
            try {
                if (TxConfig.VALIDATION_REQUIRED)
                    plh().getMasterStore().validate(id);
                plh().getMasterStore().commit(id);
            } catch (e:Exception) {
                addExceptionUnsafe(new TxStoreConflictException());
            }
            release();
        } else { //validate and commit on the slave
            try {
                if (TxConfig.VALIDATION_REQUIRED)
                    plh().getMasterStore().validate(id);
                
                masterSlave = plh().getMapping(members);
                val slave = masterSlave.getOrThrow(here.id as Int);
                pending = new HashMap[TxMember,Int]();
                count = 0n;
                var backupDo:Boolean = false;
                lock.lock();
                if (!Place(slave as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(slave, SLAVE));
                    backupDo = true;
                }
                ph1Started = true;
                ph2Started = true;
                vote = true;
                lock.unlock();
                
                if (backupDo) {
                    val gr = this.gr;
                    val ownerPlaceIndex = plh().virtualPlaceId;
                    val storeType = plh().getMasterStore().getType();
                    val log = plh().getMasterStore().getTxCommitLog(id);
                    val log1:HashMap[Any,Cloneable] = (storeType == TxLocalStore.KV_TYPE) ? log as HashMap[Any,Cloneable] : null;
                    val log2:HashMap[Long,Any] = (storeType == TxLocalStore.RAIL_TYPE) ? log as HashMap[Long,Any] : null;
                    val writeLog = (storeType == TxLocalStore.KV_TYPE) ? (log1 != null && log1.size() > 0) : (log2 != null && log2.size() > 0) ;
                    ////////////////////////////////////////
                    plh().getMasterStore().commit(id); // we cannot commit before getting the logs
                    ///////////////////////////////////////
                    if (writeLog) {
                        at (Place(slave as Long)) @Immediate("slave_commit_local") async {
                            plh().slaveStore.commit(id, log1, log2, ownerPlaceIndex);
                            at (gr) @Immediate("slave_commit_local_response") async {
                                (gr() as TxResilient).notifyAbortOrCommit(slave, SLAVE);
                            }
                        }
                    } else {
                        pending = null;
                        release();
                    }
                } else {
                    ///////////////////////////////////////
                    plh().getMasterStore().commit(id);
                    ///////////////////////////////////////
                    release();
                }
            } catch (e:Exception) {
                vote = false;
                addExceptionUnsafe(new TxStoreConflictException());
                release();
            }
        }
    }

    public def resilient2PC(abort:Boolean, isRecovered:Boolean) {
        if (isRecovered) {
            masterSlave = plh().getMapping(members); 
            commitOrAbort(false);//abort
        } else if (abort) {
            //addExceptionUnsafe(new TxStoreConflictException());
            abortMastersOnly();
        } else if (readOnly && !TxConfig.VALIDATION_REQUIRED) {
            vote = true;
            ph1Started = true;
            if (backupId != -1n) {
                updateBackup();
            } else {
                commitOrAbort(vote);
            }
        } else {
            prepare();
        }
    }
    
    public def markAsCommitting() {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backup running markAsCommitting ...");
        vote = true;
    }
    
    public def backupFinalize(finObj:Releasable) {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] backupFinalize vote["+vote+"] ...");
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
         
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] abortMastersOnly  count="+count+" ...");
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
            if (here.id as Int == p) {
                optimisticGC(gcId);
                plh().getMasterStore().abort(id);
                val me = here.id as Int;
                notifyAbortOrCommit(me, MASTER);
            } else {
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
    }
    
    private def printReadOnlyMasters() {
        var str:String = "";
        if (readOnlyMasters != null) {
            for (s in readOnlyMasters) {
                str += s + " : ";
            }
        }
        return str;
    }
    private def printPending() {
        var str:String = "";
        if (pending != null) {
            for (e in pending.entries()) {
                str += e.getKey() + " : " + e.getValue() + " , ";
            }
        }
        return str;
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
        if (ph1Started) { //avoid calling prepare twice -> by the master normally and Finish.notifyPlaceDeath 
            lock.unlock();
            return;
        }
        ph1Started = true;
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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] tag[PREPARE] pending["+printPending()+"] count["+count+"] ...");
        lock.unlock();
        
        if (switchToAbort) { //if any of the masters is dead, abort the transaction
            abort(liveMasters);
        } else {
            for (p in liveMasters) {
                val slaveId = masterSlave.getOrElse(p, DEAD_SLAVE);
                if (here.id as Int != p) {
                    at (Place(p)) @Immediate("prep_request_res") async {
                        Runtime.submitUncounted(()=> { //some times validation blocks, so we cannot execute this body within an immediate thread.
                            var vote:Boolean = true;
                            if (TxConfig.VALIDATION_REQUIRED) {
                                try {
                                    plh().getMasterStore().validate(id);
                                }catch (e:Exception) {
                                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] validation excp["+e.getMessage()+"] ...");
                                    vote = false;
                                }
                            }        
                            var localReadOnly:Boolean = false;
                            if (!readOnly && slaveId != DEAD_SLAVE) {
                                val ownerPlaceIndex = plh().virtualPlaceId;
                                val storeType = plh().getMasterStore().getType();
                                val log = plh().getMasterStore().getTxCommitLog(id);
                                val log1:HashMap[Any,Cloneable] = (storeType == TxLocalStore.KV_TYPE) ? (log as TxCommitLog[Any]).log1 as HashMap[Any,Cloneable] : null;
                                val log1V:HashMap[Any,Int] = (storeType == TxLocalStore.KV_TYPE) ? (log as TxCommitLog[Any]).log1V as HashMap[Any,Int] : null;
                                val log2:HashMap[Long,Any] = (storeType == TxLocalStore.RAIL_TYPE) ? log as HashMap[Long,Any] : null;
                                val writeLog = (storeType == TxLocalStore.KV_TYPE) ? (log1 != null && log1.size() > 0) : (log2 != null && log2.size() > 0) ; 
                                if (writeLog) {
                                    at (Place(slaveId as Long)) @Immediate("slave_prep111") async {
                                        plh().slaveStore.prepare(id, log1, log1V, log2, ownerPlaceIndex);
                                        at (gr) @Immediate("slave_prep_response111") async {
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
                } else {
                    Runtime.submitUncounted(()=> { //some times validation blocks, so we cannot execute this body within an immediate thread.
                        var vote:Boolean = true;
                        if (TxConfig.VALIDATION_REQUIRED) {
                            try {
                                plh().getMasterStore().validate(id);
                            }catch (e:Exception) {
                                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] validation excp["+e.getMessage()+"] ...");
                                vote = false;
                            }
                        }       
                        var localReadOnly:Boolean = false;
                        if (!readOnly && slaveId != DEAD_SLAVE) {
                            val ownerPlaceIndex = plh().virtualPlaceId;
                            val storeType = plh().getMasterStore().getType();
                            val log = plh().getMasterStore().getTxCommitLog(id);
                            val log1:HashMap[Any,Cloneable] = (storeType == TxLocalStore.KV_TYPE) ? (log as TxCommitLog[Any]).log1 as HashMap[Any,Cloneable] : null;
                            val log1V:HashMap[Any,Int] = (storeType == TxLocalStore.KV_TYPE) ? (log as TxCommitLog[Any]).log1V as HashMap[Any,Int] : null;
                            val log2:HashMap[Long,Any] = (storeType == TxLocalStore.RAIL_TYPE) ? log as HashMap[Long,Any] : null;
                            val writeLog = (storeType == TxLocalStore.KV_TYPE) ? (log1 != null && log1.size() > 0) : (log2 != null && log2.size() > 0) ; 
                            if (writeLog) {
                                at (Place(slaveId as Long)) @Immediate("slave_prep11") async {
                                    plh().slaveStore.prepare(id, log1, log1V, log2, ownerPlaceIndex);
                                    at (gr) @Immediate("slave_prep_response11") async {
                                        (gr() as TxResilient).notifyPrepare(slaveId, SLAVE, true /*vote*/, false /*is master RO*/); 
                                    }
                                }
                            }
                            else 
                                localReadOnly = true;
                        }
                        val masterId = here.id as Int;
                        ((gr as GlobalRef[Tx]{self.home == here})() as TxResilient).notifyPrepare(masterId, MASTER, vote, localReadOnly);
                    });
                }
            }
        }
    }
    
    private def notifyPrepare(place:Int, ptype:Int, v:Boolean, isMasterRO:Boolean) {
        var prep:Boolean = false;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place="+place+" ptype="+ptype+" v="+v+" isMRO="+isMasterRO+" ...");
        lock.lock();
        if (!Place(place as Long).isDead()) {
            if (ptype == MASTER) {
                count--;
                if (pending.getOrElse(TxMember(place, MASTER),-1n) == -1n)
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare FATALERROR place["+place+"] type["+MASTER+"] count="+count+" doesn't exist...");
       
                FinishResilient.decrement(pending, TxMember(place, MASTER));
                if (!readOnly && isMasterRO) { //we don't except an acknowledgement from the slave
                    val slave = masterSlave.getOrElse(place, DEAD_SLAVE);
                    if (slave != DEAD_SLAVE && !Place(slave as Long).isDead()) {
                        count--;
                        FinishResilient.decrement(pending, TxMember(slave, SLAVE));
                    }
                    else {
                        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place="+place+" ptype="+ptype+" isMRO="+isMasterRO+" slave is dead, don't decrement count");
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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPrepare place["+place+"] ptype="+ptype+" localVote["+v+"] vote["+vote+"] completed, count="+count+" ...");
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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX readOnly["+readOnly+"] ...");
        if (readOnly) {
            for (m in members) {
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX checkMem["+m+"] ...");
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(m, MASTER));
                    liveMasters.add(m);
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX Mem["+m+"] isLiveMaster ...");
                }
                else 
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX Mem["+m+"] isDeadMaster ...");
            }
        } else {
            for (e in masterSlave.entries()) {
                val m = e.getKey();
                val s = e.getValue();
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX master["+m+"] slave["+s+"] ...");
               
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX checkMem["+m+"] ...");
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, TxMember(m, MASTER));
                    liveMasters.add(m);
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX Mem["+m+"] isLiveMaster ...");
                } else
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX Mem["+m+"] isDeadMaster ...");
                
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX readOnlyMasters["+printReadOnlyMasters()+"] ...");
                if (readOnlyMasters == null || !readOnlyMasters.contains(m)) {
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX readOnlyMastersPath checkSlave["+s+"]  ...");
                    if (!Place(s as Long).isDead()) {
                        count++;
                        FinishResilient.increment(pending, TxMember(s, SLAVE));
                        liveSlaves.add(s);
                        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX readOnlyMastersPath Slave["+s+"] isLiveSlave ...");
                    }
                    else 
                        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] commitOrAbortX readOnlyMastersPath Slave["+s+"] isDeadSlave ...");
                }
            }
        }
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] tag[COMMIT_OR_ABORT] pending["+printPending()+"] count["+count+"] ...");
        lock.unlock();
        
        for (p in liveMasters) {
            if (here.id as Int != p) {
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
            } else {
                //gc
                optimisticGC(gcId);
                if (isCommit)
                    plh().getMasterStore().commit(id);
                else
                    plh().getMasterStore().abort(id);
                val me = here.id as Int;
                notifyAbortOrCommit(me, MASTER);
            }
        }
        
        for (p in liveSlaves) {
            if (here.id as Int != p) {
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
            } else {
                if (isCommit)
                    plh().slaveStore.commit(id);
                else
                    plh().slaveStore.abort(id);
                val me = here.id as Int;
                notifyAbortOrCommit(me, SLAVE);
            }
        }
    }
    
    private def notifyAbortOrCommit(place:Int, ptype:Int) {
        var rel:Boolean = false;
        
        try {
            lock.lock(); //altering the counts must be within lock/unlock
            if (!Place(place as Long).isDead()) {
                //FIXME: when I try to ignore slaves who's master died, the execution hangs!
                count--;
                if (pending != null) {
                    FinishResilient.decrement(pending, TxMember(place, ptype));
                    if (count == 0n) {
                        rel = true;
                        pending = null;
                    }
                } else {
                    if (pending == null) {
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit bug FATALERROR place["+place+"] type["+ptype+"] pending is NULL");
                    }
                    else if (pending.getOrElse(TxMember(place, ptype),-1n) == -1n) {
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit bug FATALERROR place["+place+"] type["+ptype+"] count="+count+" doesn't exist...");
                    }
                }
            }
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyAbortOrCommit place["+place+"]  count="+count+" ...");
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
        if (TxConfig.TM_DEBUG) {
            var str:String = "";
            for (newD in newDead) {
                str += newD + " : ";
            }
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces called, newDead["+str+"] ");
        }
    	if (lock == null) {
    	    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces lock is null, return false");
    		return false;
    	}
        try {
            lock.lock();
            if (newDead.contains(gcId.home))
                return true;
            if (backupId != -1n && newDead.contains(backupId)) {
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning true, backup is: " + backupId);
                return true;
            }
            if (pending != null) {
                for (key in pending.keySet()) {
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces check if pending place["+key.place+"] ");
                    if (newDead.contains(key.place)) {
                        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning true, pending contains: " + key.place);
                        return true;
                    } else if (key.ptype == SLAVE && masterSlave != null) {
                        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces check if pending place["+key.place+"] go to slave path");
                        for (e in masterSlave.entries()) {
                            val m = e.getKey();
                            val s = e.getValue();
                            if (s == key.place && Place(m).isDead()) {
                                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning true, pending contains slaveOfDead: " + key.place );
                                return true;
                            }
                        }
                    }
                }
            } else {
                if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces pending is null");    
            }
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] isImpactedByDeadPlaces returning false");
            return false;
        } finally {
            lock.unlock();
        }
    }
    
    public def notifyPlaceDeath() {
        //Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead started");
        var rel:Boolean = false;
        lock.lock();
        val oldCnt = count;
        val toRemove = new HashSet[TxMember]();
        if (pending != null) {
            for (key in pending.keySet()) {
                if (Place(key.place as Long).isDead()) {
                    toRemove.add(key);
                    
                    var slave:Int = DEAD_SLAVE;
                    if (masterSlave != null) { //the pending place is master and it is dead
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
                } else if (key.ptype == SLAVE && masterSlave != null) { //the pending place is slave and its master died
                    for (e in masterSlave.entries()) {
                        val m = e.getKey();
                        val s = e.getValue();
                        if (s == key.place && Place(m).isDead()) {
                            if (!ph2Started)
                                vote = false;
                            toRemove.add(TxMember(s, SLAVE));
                            break;
                        }
                    }
                }
            }
        }
        
        for (t in toRemove) {
            if (!ph2Started && t.ptype == MASTER)
                addExceptionUnsafe( new DeadPlaceException( Place(t.place as Long) ) );
            
            val cnt = pending.remove(t);
            count -= cnt;
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead remove place="+t.place+" type="+t.ptype+" cnt="+cnt+" count="+count+"...");
        }
        
        if (oldCnt > 0 && count == 0n) {
            pending = null;
            rel = true;
        }
        //Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead pending["+printPending()+"] count["+count+"] ...");
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyPlaceDead completed, pending count=" + count);
        lock.unlock();
        
        if (rel) {
            if (ph2Started) {
                release();
            } else {
                commitOrAbort(vote);
            }
        }
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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] updateBackup place"+backup+" ...");
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
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] notifyBackupChange newBackup["+newBackupId+"] restart["+restart+"] ...");
        lock.unlock();
        if (restart)
            updateBackup();
    }
    
    private def notifyBackupUpdated() {
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " FID["+gcId+"] here["+here+"] received notifyBackupUpdated ...");
        commitOrAbort(vote);
    }
}