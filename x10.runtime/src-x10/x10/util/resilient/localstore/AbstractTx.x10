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
import x10.util.resilient.localstore.tx.TxManager;

public class AbstractTx[K] {K haszero} {
	public val plh:PlaceLocalHandle[LocalStore[K]];
    public val id:Long;
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
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
    	this.plh = plh;
        this.id = id;
    }
    
    /***************** Get ********************/
    public def get(key:K):Cloneable {
        return plh().getMasterStore().get(id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:K, value:Cloneable):Cloneable {
        return plh().getMasterStore().put(id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:K):Cloneable {
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[K] {
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
}
