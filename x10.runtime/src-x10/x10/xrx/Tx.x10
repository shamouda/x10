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
import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.ResilientCondition;

public class Tx {
	public val plh:PlaceLocalHandle[LocalStore[Any]];
    public val id:Long;
    static resilient = Runtime.RESILIENT_MODE > 0;
    
    public def this(plh:PlaceLocalHandle[LocalStore[Any]], id:Long) {
    	this.plh = plh;
        this.id = id;
    }
    
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
    
    /***************** Delete ********************/
    public def delete(key:Any):Cloneable {
        Runtime.activity().tx = true;
        Runtime.activity().txReadOnly = false;
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[Any] {
        Runtime.activity().tx = true;
        return plh().getMasterStore().keySet(id); 
    }
    
    public def asyncAt(virtualPlace:Long, closure:()=>void) {
        val pl = plh().getPlace(virtualPlace);
        at (pl) async closure();
    }
    
    public def evalAt(virtualPlace:Long, closure:()=>Any) {
        val pl = plh().getPlace(virtualPlace);
        return at (pl) closure();
    }
    
    //finish methods//
    public def prepare(places:Rail[Int]) {
        if (!TxConfig.get().VALIDATION_REQUIRED)
            return;
        
        //prepare here
        try {
            plh().getMasterStore().validate(id);
        } catch (e:Exception) {
            throw new ConflictException();
        }
        
        //prepare remaining places
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("prep_request") async {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] validate ...");
                    
                    var vote:Boolean = true;
                    try {
                        plh().getMasterStore().validate(id);
                    }catch (e:Exception) {
                        if (TxConfig.get().TM_DEBUG) 
                            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] validation excp["+e.getMessage()+"] ...");
                        vote = false;
                    }
                    val v = vote;
                    val me = here.id as Int;
                    at (gr) @Immediate("prep_response") async {
                        gr().notifyTermination(me, v);
                    }
                }
            }
        };
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] VALIDATION_DONE failed["+fin.failed()+"] yesVote["+fin.yesVote()+"]...");
        
        if (fin.failed())
            throw new DeadPlaceException();
        
        if (!fin.yesVote())
            throw new ConflictException();
    }
    
    public def commit(root:GlobalRef[FinishState], places:Rail[Int]) {
        //commit here
        plh().getMasterStore().commit(id);
        
        //commit remaining places
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("comm_request") async {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit ...");
                    //gc
                    Runtime.finishStates.remove(root);
                    
                    plh().getMasterStore().commit(id);
                    val me = here.id as Int;
                    at (gr) @Immediate("comm_response") async {
                        gr().notifyTermination(me);
                    }
                }
            }
        };
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] COMMIT_DONE ...");
    }
    
    public def abort(root:GlobalRef[FinishState], places:Rail[Int]) {
        //abort here
        plh().getMasterStore().abort(id);
        
        //abort remaining places
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("abort_request") async {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort ...");
                    
                    //gc
                    Runtime.finishStates.remove(root);
                    
                    plh().getMasterStore().abort(id);
                    val me = here.id as Int;
                    at (gr) @Immediate("abort_response") async {
                        gr().notifyTermination(me);
                    }
                }
            }
        };
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] ABORT_DONE ...");
    }
    
    //**********************    finish methods        **********************//
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
    
    private def validateMaster(slave:Place) {
        if (TxConfig.get().VALIDATION_REQUIRED)
            plh().getMasterStore().validate(id);        
        var ex:Exception = null;
        val ownerPlaceIndex = plh().virtualPlaceId;
        val log = plh().getMasterStore().getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            val rCond = ResilientCondition.make(slave);
            val closure = (gr:GlobalRef[Condition]) => {
                at (slave) @Immediate("cmthandler_prepare") async {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                    at (gr) @Immediate("cmthandler_prepare_response") async {
                        gr().release();
                    }
                }
            };
            
            if (TxConfig.TM_DEBUG) 
                Console.OUT.println("Tx["+ id +"] " + TxConfig.txIdToString (id) 
                                     + " here["+here+"] ["+Runtime.activity()+"] going to slave ["+slave+"] to prepare ...");
            rCond.run(closure);
            
            if (rCond.failed()) {
                ex = new DeadPlaceException(slave); //dead slave not fatal
            }
            rCond.forget();
        } else {
            if (TxConfig.TM_DEBUG) 
                Console.OUT.println("Tx["+ id +"] " + TxConfig.txIdToString (id) 
                                     + " here["+here+"] log is empty, don't go to slave ["+slave+"] to prepare ...");
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
}
