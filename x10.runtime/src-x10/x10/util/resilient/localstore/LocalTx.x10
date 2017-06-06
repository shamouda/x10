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

package x10.util.resilient.localstore;

import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.Timer;
import x10.util.concurrent.Future;

public class LocalTx[K] {K haszero} extends AbstractTx[K] {
    public transient val startTime:Long = Timer.milliTime();
    public transient var processingElapsedTime:Long = 0;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        super(plh, id);
    }
    
    /***************** Get ********************/
    public def get(key:K):Cloneable {
        try {
            return plh().getMasterStore().get(id, key);
        }catch(ex:Exception){
            updateAbortStat();
            throw ex;
        }
    }

    /***************** PUT ********************/
    public def put(key:K, value:Cloneable):Cloneable {
        try {
            return plh().getMasterStore().put(id, key, value);
        }catch(ex:Exception){
            updateAbortStat();
            throw ex;
        }
    }
    
    /***************** Delete ********************/
    public def delete(key:K):Cloneable {
        try {
            return plh().getMasterStore().delete(id, key);
        }catch(ex:Exception){
            updateAbortStat();
            throw ex;
        }
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[K] {
        try {
            return plh().getMasterStore().keySet(id);
        }catch(ex:Exception){
            updateAbortStat();
            throw ex;
        }
    }

    /***********************   Abort ************************/  
    
    private def updateAbortStat() {
        if (processingElapsedTime == 0)
            processingElapsedTime = Timer.milliTime() - startTime;
        plh().stat.addAbortedLocalTxStats(Timer.milliTime() - startTime, 
                processingElapsedTime);
    }
    
    public def abort() {
        if (processingElapsedTime == 0)
            processingElapsedTime = Timer.milliTime() - startTime;
        
        plh().getMasterStore().abort(id);
        
        plh().stat.addAbortedLocalTxStats(Timer.milliTime() - startTime, 
                processingElapsedTime);
    }
    
    /***********************   Local Commit Protocol ************************/  
    public def commit():Int {
        return commit(true); //by default we can ignore the slave
    }
    
    public def commit(ignoreDeadSlave:Boolean):Int {
        if (processingElapsedTime == 0)
            processingElapsedTime = Timer.milliTime() - startTime;
        
        var success:Int = AbstractTx.SUCCESS;
        val id = this.id;
        val plh = this.plh;
        val ownerPlaceIndex = plh().virtualPlaceId;
        try {
            if (TxConfig.get().VALIDATION_REQUIRED)
                plh().getMasterStore().validate(id);
            
            val log = plh().getMasterStore().getTxCommitLog(id);
            if (resilient && log != null && log.size() > 0 && !TxConfig.get().DISABLE_SLAVE) {
                try {
                    finish at (plh().slave) async {
                        plh().slaveStore.commit(id, log, ownerPlaceIndex);
                    }
                } catch(exSl:Exception) {
                	if (!ignoreDeadSlave)
                		throw exSl;
                    success = AbstractTx.SUCCESS_RECOVER_STORE;
                }
            }
            
            if (log != null) {
                //master commit
                plh().getMasterStore().commit(id);
            }
            
            plh().stat.addCommittedLocalTxStats(Timer.milliTime() - startTime, 
                    processingElapsedTime);
            
            return success;
        } catch(ex:Exception) { // slave is dead         
            //master abort
            abort();
            throw ex;
        }
    }
    
}