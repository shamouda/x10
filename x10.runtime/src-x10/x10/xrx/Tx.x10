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
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.ConflictException;

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
        return plh().getMasterStore().get(id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:Any, value:Cloneable):Cloneable {
        return plh().getMasterStore().put(id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:Any):Cloneable {
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[Any] {
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
    public def prepare(places:Rail[Int], includeHere:Boolean) {
        if (!TxConfig.get().VALIDATION_REQUIRED)
            return;
        if (includeHere) {
            try {
                plh().getMasterStore().validate(id);
            } catch (e:Exception) {
                throw new ConflictException();
            }
        }
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
    
    public def commit(root:GlobalRef[FinishState], places:Rail[Int], includeHere:Boolean) {
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
        if (includeHere)
            plh().getMasterStore().commit(id);
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] COMMIT_DONE ...");
    }
    
    public def abort(root:GlobalRef[FinishState], places:Rail[Int], includeHere:Boolean) {
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
        
        if (includeHere)
            plh().getMasterStore().abort(id);
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] ABORT_DONE ...");
    }
    
    //finish methods//
    public def res_prepare(places:Rail[Int], includeHere:Boolean) {
        if (!TxConfig.get().VALIDATION_REQUIRED)
            return;
        if (includeHere) {
            try {
                plh().getMasterStore().validate(id);
            } catch (e:Exception) {
                throw new ConflictException();
            }
        }
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("res_prep_request") async {
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
                    at (gr) @Immediate("res_prep_response") async {
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
    
    public def res_commit(fid:FinishResilient.Id, places:Rail[Int], includeHere:Boolean) {
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("res_comm_request") async {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] commit ...");
                    //gc
                    optimisticGC(fid);
                    
                    plh().getMasterStore().commit(id);
                    val me = here.id as Int;
                    at (gr) @Immediate("res_comm_response") async {
                        gr().notifyTermination(me);
                    }
                }
            }
        };
        if (includeHere)
            plh().getMasterStore().commit(id);
        fin.run(closure);
        if (TxConfig.get().TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] COMMIT_DONE ...");
    }
    
    public def res_abort(fid:FinishResilient.Id, places:Rail[Int], includeHere:Boolean) {
        val fin = LowLevelFinish.make(places);
        val closure = (gr:GlobalRef[LowLevelFinish]) => {
            for (p in places) {
                at (Place(p)) @Immediate("res_abort_request") async {
                    if (TxConfig.get().TM_DEBUG) 
                        Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] abort ...");
                    
                    //gc
                    optimisticGC(fid);
                    
                    plh().getMasterStore().abort(id);
                    val me = here.id as Int;
                    at (gr) @Immediate("res_abort_response") async {
                        gr().notifyTermination(me);
                    }
                }
            }
        };
        
        if (includeHere)
            plh().getMasterStore().abort(id);
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
