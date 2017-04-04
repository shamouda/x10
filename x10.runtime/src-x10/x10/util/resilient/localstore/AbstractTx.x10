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

import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;

public class AbstractTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) {
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    /* Constants */
    protected static val GET_LOCAL = 0n;
    protected static val PUT_LOCAL = 1n;
    protected static val DELETE_LOCAL = 2n;
    protected static val KEYSET_LOCAL = 3n;
    protected static val LOCK = 4n;
    protected static val UNLOCK = 5n;
    
    public static val SUCCESS = 0n;
    public static val SUCCESS_RECOVER_STORE = 1n;
        
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
    
    public def asyncAt(virtualPlace:Long, closure:()=>void) {
        val pl = plh().getPlace(virtualPlace);
        assert (pl.id >= 0 && pl.id < Place.numPlaces()) : "fatal bug, wrong place id " + pl.id;
        at (pl) async closure();
    }
    
    public def evalAt(virtualPlace:Long, closure:()=>Any) {
        val pl = plh().getPlace(virtualPlace);
        assert (pl.id >= 0 && pl.id < Place.numPlaces()) : "fatal bug, wrong place id " + pl.id;
        return at (pl) closure();
    }
    
    protected static def opDesc(op:Int) {
        switch(op) {
            case LOCK: return "LOCK";
            case UNLOCK: return "UNLOCK";
            case GET_LOCAL: return "GET_LOCAL";
            case PUT_LOCAL: return "PUT_LOCAL";
            case DELETE_LOCAL: return "DELETE_LOCAL";
            case KEYSET_LOCAL: return "KEYSET_LOCAL";
        }
        return "";
    }
}
