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

import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;
import x10.xrx.txstore.TxConfig;
import x10.xrx.txstore.TxLocalStore;
import x10.xrx.txstore.TxMasterStoreForRail;

/*should be used in non-resilient mode only*/
public class TxLocking(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
    public val members:Rail[Long];
    public val keys:Rail[Any];
    public val readFlags:Rail[Boolean];
    public val opPerPlace:Long;

    protected def this(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long, members:Rail[Long],keys:Rail[Any],readFlags:Rail[Boolean],opPerPlace:Long) {
        property(plh, id);
        this.members = members;
        this.keys = keys;
        this.readFlags = readFlags;
        this.opPerPlace = opPerPlace;
    }
    
    protected def this(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
        property(plh, id);
        this.members = null;
        this.keys = null;
        this.readFlags = null;
        this.opPerPlace = -1;
    }
    

    /***************** Get ********************/
    public def get(key:Any):Cloneable {
        return plh().getMasterStore().get(id, key);
    }
    
    public def getRail(index:Long):Any {
        return (plh().getMasterStore() as TxMasterStoreForRail[Any]).getRailLocking(id, index);
    }
    
    /***************** PUT ********************/
    public def put(key:Any, value:Cloneable):Cloneable {
        return plh().getMasterStore().put(id, key, value);
    }
    
    public def putRail(index:Long, value:Any) {
        (plh().getMasterStore() as TxMasterStoreForRail[Any]).putRailLocking(id, index, value);
    }
    
    /***************** Delete *****************/
    public def delete(key:Any):Cloneable {
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet *****************/
    public def keySet():Set[Any] {
        return plh().getMasterStore().keySet(id); 
    }
    
    public def asyncAt(virtualPlace:Long, closure:()=>void) {
        val pl = plh().getPlace(virtualPlace);
        at (pl) async closure();
    }

    public def tryLockWrite(key:Any) {
        return plh().getMasterStore().tryLockWrite(id, key);
    }
    
    public def tryLockRead(key:Any) {
        return plh().getMasterStore().tryLockRead(id, key);
    }
    
    public def unlockRead(key:Any) {
        plh().getMasterStore().unlockRead(id, key);
    }
    
    public def unlockWrite(key:Any) {
        plh().getMasterStore().unlockWrite(id, key);
    }
    /****************lock and unlock all keys**********************/

    public def lock() {
        //don't copy this in remote operations
        val members = this.members;
        val keys = this.keys;
        val readFlags = this.readFlags;
        val plh = this.plh;
        val opPerPlace = this.opPerPlace;
        
        for (var i:Long = 0; i < members.size; i++) {
            val dest = members(i);
            val start = opPerPlace*i;
            finish at (Place(dest)) async { //locking must be done sequentially to avoid out of order locking
                if (!TxConfig.LOCK_FREE)
                    Runtime.increaseParallelism();
                
                plh().getMasterStore().lockAll(id, start, opPerPlace, keys, readFlags);
                
                if (!TxConfig.LOCK_FREE)
                    Runtime.decreaseParallelism(1n);
            }
        }
    }

    public def unlock() {
        //don't copy this in remote operations
        val members = this.members;
        val keys = this.keys;
        val readFlags = this.readFlags;
        val plh = this.plh;
        val opPerPlace = this.opPerPlace;
        
        finish for (var i:Long = 0; i < members.size; i++) {
            val dest = members(i);
            val start = opPerPlace*i;
            at (Place(dest)) async {
                plh().getMasterStore().unlockAll(id, start, opPerPlace, keys, readFlags);
            }
        }
    }
}