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
import x10.util.resilient.localstore.tx.ConflictException;
import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.tx.FatalTransactionException;

public class TxResilient extends Tx {
    private transient var gcId:FinishResilient.Id;
    private transient var readOnlyMasters:Set[Int] = null; //read-only masters in a non ready-only transaction
    private transient var masterSlave:HashMap[Int, Int]; 
    private transient var pending:HashMap[Int, Int];     //pending resilient communication
    
    public def this(plh:PlaceLocalHandle[LocalStore[Any]], id:Long) {
    	super(plh, id);
    }
    
    public def initialize(fid:FinishResilient.Id) {
        gcId = fid;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
        vote = true;
        readOnly = true;
    }
    
    public def finalize(finObj:Releasable, abort:Boolean) {
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] finalize abort="+abort+" ...");
        this.finishObj = finObj;
        resilient2PC(abort);
    }
    
    public def finalizeLocal(finObj:Releasable, abort:Boolean) {
        finalize(finObj, abort);
    }

    public def resilient2PC(abort:Boolean) {
        if (abort) {
            abortMastersOnly();
        } else {
            prepare();
        }
    }
    
    private def abortMastersOnly() {   
        val liveMasters:Set[Int] = new HashSet[Int]();
        lock.lock(); //altering the counts must be within lock/unlock
        pending = new HashMap[Int,Int]();
        count = 0n;
        for (m in members) {
            if (Place(m as Long).isDead())
                continue;
            count++;
            FinishResilient.increment(pending, m);
            liveMasters.add(m);
        }
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abortMastersOnly  count="+count+" ...");
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
                    (gr() as TxResilient).notifyAbortOrCommit(me);
                }
            }
        }
    }
    
    public def prepare() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val plh = this.plh;
        val readOnly = this.readOnly;
        
        var switchToAbort:Boolean = false;
        val pg = plh().activePlacesUnsafe();
        val liveMasters:Set[Int] = new HashSet[Int]();
        val liveSlaves:Set[Int] = new HashSet[Int]();
        
        lock.lock(); //altering the counts must be within lock/unlock
        pending = new HashMap[Int,Int]();
        masterSlave = new HashMap[Int,Int]();
        count = 0n;
        for (m in members) {
            if (Place(m as Long).isDead()) {
                switchToAbort = true;
                continue;
            } else {
                count++;
                FinishResilient.increment(pending, m);
                liveMasters.add(m);
                
                if (!readOnly) { //we don't except acknowledgements from slaves in RO transactions
                    val slave = pg.next(Place(m));
                    masterSlave.put(m, slave.id as Int);
                    if (!slave.isDead()) {
                        count++;
                        FinishResilient.increment(pending, slave.id as Int);
                        liveSlaves.add(slave.id as Int);
                    }
                }
            }
        }
        lock.unlock();
        
        if (switchToAbort) { //if any of the masters is dead, abort the transaction
            abort(liveMasters);
        } else {
            for (p in liveMasters) {
                val slaveId = masterSlave.getOrElse(p, -1n);
                at (Place(p)) @Immediate("prep_request_res") async {
                    var vote:Boolean = true;
                    if (TxConfig.get().VALIDATION_REQUIRED) {
                        try {
                            plh().getMasterStore().validate(id);
                        }catch (e:Exception) {
                            if (TxConfig.get().TM_DEBUG) 
                                Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] validation excp["+e.getMessage()+"] ...");
                            vote = false;
                        }
                    }        
                    var localReadOnly:Boolean = false;
                    if (!readOnly) {
                        if (slaveId == -1n)
                            throw new Exception("FATAL error  slaveId == -1");
                        val ownerPlaceIndex = plh().virtualPlaceId;
                        val log = plh().getMasterStore().getTxCommitLog(id);
                        if (log != null && log.size() > 0) {
                            at (Place(slaveId as Long)) @Immediate("slave_prep") async {
                                plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                                at (gr) @Immediate("slave_prep_response") async {
                                    (gr() as TxResilient).notifyPrepare(slaveId, true /*vote*/, false /*is master RO*/); 
                                }
                            }
                        }
                        else 
                            localReadOnly = true;
                    }
                    val isMasterRO = localReadOnly;
                    val v = vote;
                    val masterId = here.id as Int;
                    at (gr) @Immediate("prep_response_res") async {
                        (gr() as TxResilient).notifyPrepare(masterId, v, isMasterRO);
                    }
                }
            }
        }
    }
    
    private def commitOrAbort(isCommit:Boolean) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcId = this.gcId;
        val plh = this.plh;
        
        val liveMasters:Set[Int] = new HashSet[Int]();
        val liveSlaves:Set[Int] = new HashSet[Int]();
        
        lock.lock(); //altering the counts must be within lock/unlock
        pending = new HashMap[Int,Int]();
        count = 0n;
        if (readOnly) {
            for (m in members) {
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, m);
                    liveMasters.add(m);
                }
            }
        } else {
            for (e in masterSlave.entries()) {
                val m = e.getKey();
                val s = e.getValue();
                
                if (!Place(m as Long).isDead()) {
                    count++;
                    FinishResilient.increment(pending, m);
                    liveMasters.add(m);
                }
                if (readOnlyMasters == null || !readOnlyMasters.contains(m)) {
                    if (!Place(s as Long).isDead()) {
                        count++;
                        FinishResilient.increment(pending, s);
                        liveSlaves.add(s);
                    }
                }
            }
        }
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
                    (gr() as TxResilient).notifyAbortOrCommit(me);
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
                    (gr() as TxResilient).notifyAbortOrCommit(me);
                }
            }            
        }
    }
    
    private def notifyPrepare(place:Int, v:Boolean, isMasterRO:Boolean) {
        var prep:Boolean = false;
    
        lock.lock();
        if (!Place(place as Long).isDead()) {
            if (!readOnly && isMasterRO) { //deduct master and slave
                val slave = masterSlave.getOrThrow(place);
                count -= 2;
                FinishResilient.decrement(pending, place);
                FinishResilient.decrement(pending, slave);
            } else { //deduct master only
                count--;
                FinishResilient.decrement(pending, place);
            }
            vote = vote & v;
            if (isMasterRO) {
                if (readOnlyMasters == null)
                    readOnlyMasters = new HashSet[Int]();
                readOnlyMasters.add(place);
            }
            if (count == 0n) {
                prep = true;
                if (!vote && exception == null) //don't overwrite fatal exceptions
                    exception = new ConflictException();
            }
        }
        if (TxConfig.get().TM_DEBUG)
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] notifyPrepare place["+place+"] localVote["+v+"] vote["+vote+"] count="+count+" ...");
        lock.unlock();
        
        if (prep) {
            commitOrAbort(vote);
        }
    }
    
    private def notifyAbortOrCommit(place:Int) {
        var rel:Boolean = false;
        
        lock.lock(); //altering the counts must be within lock/unlock
        if (!Place(place as Long).isDead()) {
            count--;
            FinishResilient.decrement(pending, place);
            if (count == 0n) {
                rel = true;
                pending = null;
            }
        }
        if (TxConfig.get().TM_DEBUG)
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] notifyAbortOrCommit place["+place+"]  count="+count+" ...");
        lock.unlock();
        
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
    
    public def notifyPlaceDead() {
        var rel:Boolean = false;
        lock.lock();
        
        val toRemove = new HashSet[Int]();
        if (pending != null) {
            for (m in pending.keySet()) {
                if (Place(m as Long).isDead()) {
                    toRemove.add(m);
                    if (masterSlave != null) {
                        val slave = masterSlave.getOrElse(m, -1n);
                        if (slave != -1n && Place(slave as Long).isDead()) {
                            exception = new FatalTransactionException("Tx["+id+"] lost both master["+m+"] and slave["+slave+"] ");
                        }
                    }
                }
            }
        }
        for (t in toRemove) {
            val cnt = pending.remove(t);
            count -= cnt;
        }
        
        if (count == 0n) {
            pending = null;
            rel = true;
        }
        lock.unlock();
        
        if (rel) {
            release();
        }
    }
}
