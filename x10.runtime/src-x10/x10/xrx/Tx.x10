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

public class Tx {
    
	private val plh:PlaceLocalHandle[LocalStore[Any]];
    public val id:Long;
    static resilient = Runtime.RESILIENT_MODE > 0;
    
    private transient var finishObj:Releasable = null;
    private transient var members:Set[Int] = null;
    private transient var readOnly:Boolean = true;
    private transient var lock:Lock = null;
    private transient var count:Int = 0n;
    private transient var vote:Boolean = true;
    private transient var gcGR:GlobalRef[FinishState];
    private transient var gcId:FinishResilient.Id;
    
    private transient var gr:GlobalRef[Tx];
    
    private transient var masterSlave:HashMap[Int, Int];
    
    //pending resilient communication
    private transient var pending:HashMap[Int, Int];
    
    private transient var exception:CheckedThrowable; //Fatal exception or Conflict exception
        
    public def this(plh:PlaceLocalHandle[LocalStore[Any]], id:Long) {
    	this.plh = plh;
        this.id = id;
    }
    
    public def setGCId(fgr:GlobalRef[FinishState]) {
        gcGR = fgr;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
    }
    
    public def setGCId(fid:FinishResilient.Id) {
        gcId = fid;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
    }
    
    /***************** Members *****************/
    public def addMember(m:Int, ro:Boolean){
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] add member["+m+"] readOnly["+ro+"] ...");
        lock.lock();
        if (members == null)
            members = new HashSet[Int]();
        members.add(m);
        readOnly = readOnly & ro;
        lock.unlock();
    }
    
    public def contains(place:Int) {
        if (members == null)
            return false;
        return members.contains(place);
    }
    
    public def getMembers() = members;
    
    public def isEmpty() = members == null || members.size() == 0;
    
    /***************** Get ********************/
    public def get(key:Any):Cloneable {
        Runtime.activity().tx = true;
        return plh().getMasterStore().get(id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:Any, value:Cloneable):Cloneable {
        Runtime.activity().tx = true;
        Runtime.activity().txReadOnly = false;
        return plh().getMasterStore().put(id, key, value);
    }
    
    /***************** Delete *****************/
    public def delete(key:Any):Cloneable {
        Runtime.activity().tx = true;
        Runtime.activity().txReadOnly = false;
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet *****************/
    public def keySet():Set[Any] {
        Runtime.activity().tx = true;
        return plh().getMasterStore().keySet(id); 
    }
    

    /********** Finalizing a transaction **********/
    public def finalize(finObj:Releasable, abort:Boolean) {
        this.finishObj = finObj;
        if (resilient)
            resilient2PC(abort);
        else
            nonResilient2PC(abort);
    }
    
    /********** Non-resilient finish **********/
    public def nonResilient2PC(abort:Boolean) {
        count = members.size() as Int;
        
        if (abort) {
            abort();
        } else {
            if (TxConfig.get().VALIDATION_REQUIRED)
                prepare();
            else
                commit();
        }
    }
    
    public def prepare() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val plh = this.plh;
        
        for (p in members) {
            at (Place(p)) @Immediate("prep_request") async {
                var vote:Boolean = true;
                try {
                    plh().getMasterStore().validate(id);
                }catch (e:Exception) {
                    vote = false;
                }
                val v = vote;
                at (gr) @Immediate("prep_response") async {
                    gr().notifyPrepare(v);
                }
            }
        }
    }
    
    public def commit() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcGR = this.gcGR;
        val plh = this.plh;
        
        for (p in members) {
            at (Place(p)) @Immediate("comm_request") async {
                //gc
                Runtime.finishStates.remove(gcGR);
                
                plh().getMasterStore().commit(id);
                at (gr) @Immediate("comm_response") async {
                    gr().notifyCommit();
                }
            }
        }        
    }
    
    public def abort() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcGR = this.gcGR;
        val plh = this.plh;
        
        for (p in members) {
            at (Place(p)) @Immediate("abort_request") async {
                //gc
                Runtime.finishStates.remove(gcGR);

                plh().getMasterStore().abort(id);
                at (gr) @Immediate("abort_response") async {
                    gr().notifyAbort();
                }
            }
        }
    }
    
    public def notifyPrepare(v:Boolean) {
        var prep:Boolean = false;
        var success:Boolean = false;
    
        lock.lock();
        count--;
        vote = vote & v;
        if (count == 0n) {
            prep = true;
            count = members.size() as Int;
            success = vote;
            if (!vote)
                exception = new ConflictException();
        }
        lock.unlock();
        
        if (prep) {
            if (success) 
                commit();
            else 
                abort() ;
        }
    }
    
    public def notifyCommit() {
        var rel:Boolean = false;
    
        lock.lock();
        count--;
        if (count == 0n) {
            rel = true;
        }
        lock.unlock();
        
        if (rel) {
            finishObj.releaseFinish(exception);
            (gr as GlobalRef[Tx]{self.home == here}).forget();
        }
    }
    
    public def notifyAbort() {
        var rel:Boolean = false;
        
        lock.lock();
        count--;
        if (count == 0n) {
            rel = true;
        }
        lock.unlock();
        
        if (rel) {
            finishObj.releaseFinish(exception);
            (gr as GlobalRef[Tx]{self.home == here}).forget();
        }
    }
    
    
    
    /********** Resilient finish **********/
    //TODO: if both master and slave died, add Fatal exception
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
            finishObj.releaseFinish(exception);
            (gr as GlobalRef[Tx]{self.home == here}).forget();
        }
    }
    
    public def resilient2PC(abort:Boolean) {
        if (abort) {
            abortMastersOnly();
        } else {
            prepareResilient();
        }
    }
    
    //fills the masters and slaves map
    //start the preparation
    public def prepareResilient() {
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
                val slaveId = masterSlave.getOrThrow(p);
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
                        val ownerPlaceIndex = plh().virtualPlaceId;
                        val log = plh().getMasterStore().getTxCommitLog(id);
                        if (log != null && log.size() > 0) {
                            at (Place(slaveId as Long)) @Immediate("slave_prep") async {
                                plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                                at (gr) @Immediate("slave_prep_response") async {
                                    gr().notifyPrepare(slaveId, true); 
                                }
                            }
                        }
                        else 
                            localReadOnly = true;
                    }
                    val localRO = localReadOnly;
                    val v = vote;
                    val masterId = here.id as Int;
                    at (gr) @Immediate("prep_response_res") async {
                        gr().notifyPrepare(masterId, v);
                        if (localRO)
                            gr().notifyPrepare(slaveId, true);
                    }
                }
            }
        }
    }
    
    public def abortMastersOnly() {        
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
                    gr().notifyAbortOrCommit(me);
                }
            }
        }
    }
    
    private def abortOrCommit(isCommit:Boolean) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcGR = this.gcGR;
        val plh = this.plh;
        
        val liveMasters:Set[Int] = new HashSet[Int]();
        val liveSlaves:Set[Int] = new HashSet[Int]();
        
        lock.lock(); //altering the counts must be within lock/unlock
        pending = new HashMap[Int,Int]();
        count = 0n;
        for (e in masterSlave.entries()) {
            val m = e.getKey();
            val s = e.getValue();
            
            if (!Place(m as Long).isDead()) {
                count++;
                FinishResilient.increment(pending, m);
                liveMasters.add(m);
            }
            if (!Place(s as Long).isDead()) {
                count++;
                FinishResilient.increment(pending, s);
                liveMasters.add(s);
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
                    gr().notifyAbortOrCommit(me);
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
                    gr().notifyAbortOrCommit(me);
                }
            }            
        }
    }
    
    public def notifyPrepare(place:Int, v:Boolean) {
        var prep:Boolean = false;
        var isCommit:Boolean = false;
    
        lock.lock();
        if (!Place(place as Long).isDead()) {
            count--;
            FinishResilient.decrement(pending, place);
            vote = vote & v;
            if (count == 0n) {
                prep = true;
                isCommit = vote;
                if (!vote && exception == null) //don't overwrite fatal exceptions
                    exception = new ConflictException();
            }
        }
        lock.unlock();
        
        if (prep) {
            abortOrCommit(isCommit);
        }
    }
    
    public def notifyAbortOrCommit(place:Int) {
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
        lock.unlock();
        
        if (rel) {
            finishObj.releaseFinish(exception);
            (gr as GlobalRef[Tx]{self.home == here}).forget();
        }
    }
    
    private def optimisticGC(fid:FinishResilient.Id) {
        if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC) {
            FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObject(fid);
        } else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC){
            FinishResilientOptimistic.OptimisticRemoteState.deleteObject(fid);
        }
    }
    
}
