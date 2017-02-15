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

public class LocalTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public transient val startTime:Long = System.nanoTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;   
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
    	try {
    		return plh().masterStore.get(mapName, id, key);
    	}catch(e:Exception) {
    		abortTime = System.nanoTime();
    		throw e;
    	}
    }

    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
    	try {
    		return plh().masterStore.put(mapName, id, key, value);
    	}catch(e:Exception) {
    		abortTime = System.nanoTime();
    		throw e;
    	}
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
    	try {
    		return plh().masterStore.delete(mapName, id, key);
    	}catch(e:Exception) {
    		abortTime = System.nanoTime();
    		throw e;
    	}
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
    	try {
    		return plh().masterStore.keySet(mapName, id);
    	} catch(e:Exception) {
    		abortTime = System.nanoTime();
    		throw e;
    	}
    }
    
    /***********************   Abort ************************/  
    
    public def abort() {
        plh().masterStore.abort(mapName, id);
        abortTime = System.nanoTime();
    }
    
    /***********************   Local Commit Protocol ************************/  
    public def commit():Int {
        var success:Int = Tx.SUCCESS;
        val id = this.id;
        val mapName = this.mapName;
        val plh = this.plh;
        try {
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
            commitTime = System.nanoTime();
            return success;
        } catch(ex:Exception) { // slave is dead         
            //master abort
        	abort();
            throw ex;
        }
    }
}