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

public class LocalTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) extends AbstractTx {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public transient val startTime:Long = Timer.milliTime();
    public transient var commitTime:Long = 0;
    public transient var abortTime:Long = 0;   
    public transient var processingElapsedTime:Long = 0;
       
    
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
        plh().masterStore.abort(id);
        abortTime = Timer.milliTime();
    }
    
    /***********************   Local Commit Protocol ************************/  
    public def commit():Int {
        var success:Int = Tx.SUCCESS;
        val id = this.id;
        val mapName = this.mapName;
        val plh = this.plh;
        val placeIndex = plh().virtualPlaceId;
        try {
        	if (TxConfig.getInstance().VALIDATION_REQUIRED)
        		plh().masterStore.validate(id);
        	
            val log = plh().masterStore.getTxCommitLog(id);
            try {
                if (resilient && log != null && log.size() > 0) {
                    finish at (plh().slave) async {
                        plh().slaveStore.commit(id, log, placeIndex);
                    }
                }
            }catch(exSl:Exception) {
                success = Tx.SUCCESS_RECOVER_STORE;                
            }
            
            if (log != null) {
                //master commit
                plh().masterStore.commit(id);
            }
            commitTime = Timer.milliTime();
            return success;
        } catch(ex:Exception) { // slave is dead         
            //master abort
        	abort();
            throw ex;
        }
    }
    
    /***************** Unsupported Methods ********************/
    public def getRemote(dest:Place, key:String):Cloneable {
    	throw new Exception("Unsupported method");
    }
    public def asyncGetRemote(dest:Place, key:String):Future[Any] {
    	throw new Exception("Unsupported method");
    }    
    public def putRemote(dest:Place, key:String, value:Cloneable):Cloneable {
    	throw new Exception("Unsupported method");
    }    
    public def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Any] {
    	throw new Exception("Unsupported method");
    }    
    public def deleteRemote(dest:Place, key:String):Cloneable {
    	throw new Exception("Unsupported method");
    }    
    public def asyncDeleteRemote(dest:Place, key:String):Future[Any] {
    	throw new Exception("Unsupported method");
    }    
    public def keySetRemote(dest:Place):Set[String] {
    	throw new Exception("Unsupported method");
    }    
    public def asyncKeySetRemote(dest:Place):Future[Any] {
    	throw new Exception("Unsupported method");
    }    
    public def syncAt(dest:Place, closure:()=>void) {
    	throw new Exception("Unsupported method");
    }    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
    	throw new Exception("Unsupported method");
    }    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
    	throw new Exception("Unsupported method");
    }    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
    	throw new Exception("Unsupported method");
    }
    public def setWaitElapsedTime(t:Long) {
    	throw new Exception("Unsupported method");
    }
}