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
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.compiler.Immediate;
import x10.xrx.txstore.TxConfig;
import x10.util.concurrent.Lock;
import x10.util.HashMap;

/*
 * Used for performance evaluation only
 * It works based on the same assumptions of a non-resilient transaction:
 *    - no failured are expected
 *    - no master-slave replication
 *    - uses counters, rather than place ids, to identify pending signals
 * */
public class TxResilientNoSlaves extends Tx {
    private transient var gcId:FinishResilient.Id;
    
    public def this(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
        super(plh, id);
    }
    
    public def initialize(fid:FinishResilient.Id, dummyBackupId:Int) {
        gcId = fid;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
        vote = true;
        readOnly = true;
    }
        
    protected def commitOrAbort(isCommit:Boolean) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcId = this.gcId;
        val plh = this.plh;
        
        for (p in members) {
            at (Place(p)) @Immediate("comm_request_noslaves") async {
                //gc
                optimisticGC(gcId);
                
                if (isCommit)
                    plh().getMasterStore().commit(id);
                else
                    plh().getMasterStore().abort(id);
                at (gr) @Immediate("comm_response_no_slaves") async {
                    gr().notifyAbortOrCommit();
                }
            }
        }        
    }
    
    private def optimisticGC(fid:FinishResilient.Id) {
        if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_PLACE0_OPTIMISTIC) {
            FinishResilientPlace0Optimistic.P0OptimisticRemoteState.deleteObject(fid);
        } else if (Runtime.RESILIENT_MODE == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC){
            FinishResilientOptimistic.OptimisticRemoteState.deleteObject(fid);
        }
    }
        
    private static def debug(msg:String) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println( msg );
    }
}
