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

public class Tx {
    private val gr = GlobalRef[Tx](this);
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
    
    public def this(plh:PlaceLocalHandle[LocalStore[Any]], id:Long) {
    	this.plh = plh;
        this.id = id;
    }
    
    public def set(fgr:GlobalRef[FinishState]) {
        gcGR = fgr;
        lock = new Lock();
    }
    
    public def set(fid:FinishResilient.Id) {
        gcId = fid;
        lock = new Lock();
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
    
    /********** Non-resilient finish **********/
    public def finalize(finObj:Releasable, abort:Boolean) {
        this.finishObj = finObj;
        nonResilient2PC(abort);
    }
    
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
            finishObj.releaseFinish();
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
            finishObj.releaseFinish();
            (gr as GlobalRef[Tx]{self.home == here}).forget();
        }
    }
    
    /*
    var masters:Set[Int] = null;
    var slaves:Set[Int] = null;

    def initMastersAndSlaves() {
        val pg = plh().getActivePlaces();
        for (m in members) {
            masters.add(m);
            slaves.add(pg.next(Place(m)));
        }
    }
    

    def initMasters() {
        val pg = plh().getActivePlaces();
        for (m in members) {
            masters.add(m);
        }
    }
    
    public def start2PCRes(abort:Boolean) {
        if (abort) {
            count = members.size() as Int;
            initMasters();
            prep = true;
            vote = false;
        } else {
            if (readOnly) {
                count = members.size() as Int ;
                initMasters();
            } else {
                count = members.size() as Int * 2n;
                initMastersAndSlaves();
            }
        }
    }
    
    public def notifyPlaceDeath() {
        val toRemove = new HashSet[Int]();
        if (masters != null) {
            for (m in masters) {
                if (Place(m as Long).isDead()) {
                    toRemove.add(m);
                }
            }
        }
        for (t in toRemove) {
            masters.remove(t);
            count--;
        }
        toRemove.clear();
        if (slaves != null) {
            for (s in slaves) {
                if (Place(s as Long).isDead()) {
                    toRemove.add(s);
                }
            }
        }
        for (t in toRemove) {
            slaves.remove(t);
            count--;
        }

        if (count == 0)
            finishObj.releaseFinish();
    }
    
    public def prepareRes(gr:GlobalRef[TxCoordinator]) {
        val ro = this.readOnly;
        val pg = plh().getActivePlaces();
        for (p in members) {
            val slaveId = pg.next(Place(p)).id as Int;
            at (Place(p)) @Immediate("prep_request_res") async {
                if (TxConfig.get().TM_DEBUG) 
                    Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] validate ...");
                
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
                if (!ro) {
                    val ownerPlaceIndex = plh().virtualPlaceId;
                    val log = plh().getMasterStore().getTxCommitLog(id);
                    if (log != null && log.size() > 0) {
                        at (Place(slaveId as Long)) @Immediate("slave_prep") async {
                            plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                            at (gr) @Immediate("slave_prep_response") async {
                                gr().notifyTxPrepare(slaveId, false, true);
                            }
                        }
                    }
                    else 
                        localReadOnly = true;
                }
                val localRO = localReadOnly;
                val v = vote;
                val me = here.id as Int;
                at (gr) @Immediate("prep_response_res") async {
                    gr().notifyTxPrepare(me, true, v);
                    if (localRO)
                        gr().notifyTxPrepare(slaveId, false, true);    
                }
            }
        }
    }
    
    public def completeRes(gr:GlobalRef[FinishState]) {
        if (vote)
            commit(gr);
        else
            abort(gr);
    }
    
    public def commitRes(gr:GlobalRef[TxCoordinator]) {
        
                
    }

    
    public def abortRes(gr:GlobalRef[TxCoordinator]) {
        
    }
    
    public def res_prepare(places:Rail[Int]) {
        if (TxConfig.get().TM_DEBUG) {
            var s:String = "";
            for (x in places)
                s += x + " ";
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ "["+Runtime.activity()+"] res_prepare places["+s+"]...");
        }
        val pg = plh().getActivePlaces();
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                if (Place(p).isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    val slave = pg.next(Place(p));
                    
                    at (Place(p)) @Immediate("res_prep_request") async {
                        if (TxConfig.get().TM_DEBUG) 
                            Console.OUT.println("Tx[" + id+"] " + TxConfig.txIdToString (id)
                                                      + " here["+here+"] validate ...");
                        var vote:Boolean = true;
                        try {
                            validateMaster(slave);
                        }catch (e:Exception) {
                            if (TxConfig.get().TM_DEBUG) 
                                Console.OUT.println("Tx["+ id + "] " + TxConfig.txIdToString (id)
                                                         + " here["+here+"] validation excp["+e.getMessage()+"] ...");
                            vote = false;
                        }
                        val v = vote;
                        val me = here.id as Int;
                        at (gr) @Immediate("res_prep_response") async {
                            gr().notifyTermination(me, v);
                        }
                    }
                }
            }
        };
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+ id +"] " + TxConfig.txIdToString (id) 
                                     + " here["+here+"] VALIDATION_DONE failed[" + fin.failed() 
                                     + "] yesVote["+fin.yesVote()+"]...");
        // a failed master will not vote
        if (!fin.yesVote())
            throw new ConflictException();
        
        //if all active masters voted yet, check the slaves of dead master 
        //to know if the master wanted to vote yet
        if (fin.failed()) {
            val slavesReady = checkSlavesPreparation(places, pg);
            if (!slavesReady)
                throw new ConflictException();
        }
    }
    
    
    
    private def checkSlavesPreparation(places:Rail[Int], pg:PlaceGroup) {
        val slaves = new HashSet[Int]();
        for (p in places) {
            if (Place(p).isDead()) {
                slaves.add(pg.next(Place(p)).id as Int);
            }
        }
        val slavesRail = new Rail[Int](slaves.size());
        var i:Long = 0;
        for (s in slaves) {
            slavesRail(i++) = s;
        }
        
        val fin = LowLevelFinish.make(slavesRail);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in slavesRail) {
                if (Place(p).isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("res_check_prep_request") async {
                        if (TxConfig.get().TM_DEBUG) 
                            Console.OUT.println("Tx[" + id+"] " + TxConfig.txIdToString (id)
                                                      + " here["+here+"] validate ...");
                        var vote:Boolean = plh().slaveStore.isPrepared(id);
                        val v = vote;
                        val me = here.id as Int;
                        at (gr) @Immediate("res_check_prep_response") async {
                            gr().notifyTermination(me, v);
                        }
                    }
                }
            }
        };
        fin.run(closure);
        
        if (fin.failed() || !fin.yesVote() )
            return false;
        return true;
    }
    
    public def res_commit(fid:FinishResilient.Id, places:Rail[Int]) {
        val mastersAndSlaves = new Rail[Int](places.size * 2);
        val pg = plh().getActivePlaces();
        var i:Long = 0;
        for (p in places) {
            mastersAndSlaves(i) = p;
            mastersAndSlaves(i+places.size) = pg.next(Place(p)).id as Int;
            i++;
        }
        if (TxConfig.TM_DEBUG) {
            var str:String = "";
            for (x in mastersAndSlaves)
                str += x + " ";
            Console.OUT.println("Tx["+ id +"] " + TxConfig.txIdToString (id) 
                + " here["+here+"] commit mastersAndSlaves ["+str+"] ...");
        }
        val fin = LowLevelFinish.make(mastersAndSlaves);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (var j:Long = 0; j < mastersAndSlaves.size; j++) {
                val p = mastersAndSlaves(j);
                var tmp:Boolean = false;
                if (j < places.size)
                    tmp = true;
                val isMaster = tmp;
                if (Place(p).isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit place["+p+"] isMaster["+isMaster+"] ...");
                    at (Place(p)) @Immediate("res_comm_request") async {
                        if (isMaster) {
                            if (TxConfig.get().TM_DEBUG) 
                                Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] master commit ...");
                            // gc
                            optimisticGC(fid);
                            plh().getMasterStore().commit(id);
                        } else {
                            if (TxConfig.get().TM_DEBUG) 
                                Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] slave commit ...");
                            plh().slaveStore.commit(id);
                        }
                        val me = here.id as Int;
                        at (gr) @Immediate("res_comm_response") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] COMMIT_DONE ...");
    }
    
    public def res_abort(fid:FinishResilient.Id, places:Rail[Int]) {
        if (TxConfig.get().TM_DEBUG) {
            var s:String = "";
            for (x in places)
                s += x + " ";
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " res_commit places["+s+"]...");
        }
        
        val mastersAndSlaves = new Rail[Int](places.size * 2);
        val pg = plh().getActivePlaces();
        var i:Long = 0;
        for (p in places) {
            mastersAndSlaves(i) = p;
            mastersAndSlaves(i+places.size) = pg.next(Place(p)).id as Int;
            i++;
        }
        if (TxConfig.TM_DEBUG) {
            var str:String = "";
            for (x in mastersAndSlaves)
                str += x + " ";
            Console.OUT.println("Tx["+ id +"] " + TxConfig.txIdToString (id) 
                + " here["+here+"] commit mastersAndSlaves ["+str+"] ...");
        }
        val fin = LowLevelFinish.make(mastersAndSlaves);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (var j:Long = 0; j < mastersAndSlaves.size; j++) {
                val p = mastersAndSlaves(j);
                var tmp:Boolean = false;
                if (j < places.size)
                    tmp = true;
                val isMaster = tmp;
                if (Place(p).isDead()) {
                    (gr as GlobalRef[LowLevelFinish]{self.home == here})().notifyFailure();
                } else {
                    at (Place(p)) @Immediate("res_abort_request") async {
                        if (TxConfig.get().TM_DEBUG) 
                            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort ...");
                        if (isMaster) {
                            plh().getMasterStore().abort(id);
                        } else {
                            plh().slaveStore.abort(id);
                        }
                        val me = here.id as Int;
                        at (gr) @Immediate("res_abort_response") async {
                            gr().notifyTermination(me);
                        }
                    }
                }
            }
        };
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] ABORT_DONE ...");
    }
    
    private def optimisticGC(fid:FinishResilient.Id) {
        if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC) {
            FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObject(fid);
        } else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC){
            FinishResilientOptimistic.OptimisticRemoteState.deleteObject(fid);
        }
    }
    */ 
}
