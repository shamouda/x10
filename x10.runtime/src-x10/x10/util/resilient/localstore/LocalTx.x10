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
      
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return plh().masterStore.get(mapName, id, key); 
    }

    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return plh().masterStore.put(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return plh().masterStore.delete(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return plh().masterStore.keySet(mapName, id); 
    }
    
    /***********************   Abort ************************/  
    
    public def abort() {
        plh().masterStore.abort(mapName, id);
    }
    
    /***********************   Local Commit Protocol ************************/  
    public def commit() {
        commit(false);
    }
    public def commitIgnoreDeadSlave():Int {
        return commit(true);
    }
    
    private def commit(ignoreDeadSlave:Boolean):Int {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() ignoreSlave["+ignoreDeadSlave+"] ");
        var success:Int = Tx.SUCCESS;
        val id = this.id;
        val mapName = this.mapName;
        val plh = this.plh;
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() masterStore = " + plh().masterStore);
        plh().masterStore.validate(mapName, id);
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() masterStore validation succeeded ");
        val log = plh().masterStore.getTxCommitLog(mapName, id);
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() get commit log = " + log);
        try {
            try {
                if (resilient && log != null && log.size() > 0) {
                    if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() before moving to slave["+plh().slave+"] ...");
                    finish at (plh().slave) async {
                        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commit() before moved to slave["+here+"] ...");
                        plh().slaveStore.commit(id, mapName, log);
                    }
                }
            }catch(exSl:Exception) {
                success = Tx.SUCCESS_RECOVER_STORE;
                if (!ignoreDeadSlave)
                    throw exSl;
            }
            
            if (log != null) {
                //master commit
                plh().masterStore.commit(mapName, id);
            }
            return success;
        } catch(ex:Exception) { // slave is dead         
            //master abort
            plh().masterStore.abort(mapName, id);
            throw ex;
        }
    }
}