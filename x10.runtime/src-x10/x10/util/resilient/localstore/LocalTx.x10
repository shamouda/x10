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

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.Timer;

public class LocalTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    /*We can skip the validation phase in RL_EA configurations*/
    private static val VALIDATION_REQUIRED = ! ( !TxManager.TM_DISABLED && 
    										      TxManager.TM_READ    == TxManager.READ_LOCKING && 
    										      TxManager.TM_ACQUIRE == TxManager.EARLY_ACQUIRE );
    
    public transient val startTime:Long = Timer.milliTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;   
    public transient var preCommitTime:Long = -1;
    
    public def setPreCommitTime(t:Long) {
    	preCommitTime = t;
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
    	try {
    		return plh().masterStore.get(mapName, id, key);
    	}catch(ex:Exception){
    		abortTime = Timer.milliTime();
    		throw ex;
    	}
    }

    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
    	try {
    		return plh().masterStore.put(mapName, id, key, value);
    	}catch(ex:Exception){
    		abortTime = Timer.milliTime();
    		throw ex;
    	}
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
    	try {
    		return plh().masterStore.delete(mapName, id, key);
    	}catch(ex:Exception){
    		abortTime = Timer.milliTime();
    		throw ex;
    	}
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
    	try {
    		return plh().masterStore.keySet(mapName, id);
    	}catch(ex:Exception){
    		abortTime = Timer.milliTime();
    		throw ex;
    	}
    }
    
    /***********************   Abort ************************/  
    
    public def abort() {
        plh().masterStore.abort(mapName, id);
        abortTime = Timer.milliTime();
    }
    
    /***********************   Local Commit Protocol ************************/  
    public def commit():Int {
        var success:Int = Tx.SUCCESS;
        val id = this.id;
        val mapName = this.mapName;
        val plh = this.plh;
        try {
        	if (VALIDATION_REQUIRED)
        		plh().masterStore.validate(mapName, id);
        	
            val log = plh().masterStore.getTxCommitLog(mapName, id);
            try {
                if (resilient && log != null && log.size() > 0) {
                    finish at (plh().slave) async {
                        plh().slaveStore.commit(id, mapName, log);
                    }
                }
            }catch(exSl:Exception) {
                success = Tx.SUCCESS_RECOVER_STORE;                
            }
            
            if (log != null) {
                //master commit
                plh().masterStore.commit(mapName, id);
            }
            commitTime = Timer.milliTime();
            return success;
        } catch(ex:Exception) { // slave is dead         
            //master abort
        	abort();
            throw ex;
        }
    }
}