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
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;


public class LockManager (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    /***************** Locking ********************/
    
    public def lock(p1:Place, key1:String, p2:Place, key2:String) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) plh().masterStore.lock(mapName, id, key1);
            at (p2) plh().masterStore.lock(mapName, id, key2);
        }
        else {
            at (p2) plh().masterStore.lock(mapName, id, key2);
            at (p1) plh().masterStore.lock(mapName, id, key1);
        }
    }
    public def unlock(p1:Place, key1:String, p2:Place, key2:String) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) plh().masterStore.unlock(mapName, id, key1);
            at (p2) plh().masterStore.unlock(mapName, id, key2);
        }
        else {
            at (p2) plh().masterStore.unlock(mapName, id, key2);
            at (p1) plh().masterStore.unlock(mapName, id, key1);
        }
    }
    
    public def lock(key:String) {
        plh().masterStore.lock(mapName, id, key);
    }
    
    public def unlock(key:String) {
        plh().masterStore.unlock(mapName, id, key);
    }
    
    /***************** Get ********************/
    public def getLocked(key:String):Cloneable {
        return plh().masterStore.getLocked(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def putLocked(key:String, value:Cloneable):Cloneable {
        return plh().masterStore.putLocked(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def deleteLocked(key:String):Cloneable {
        return plh().masterStore.deleteLocked(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        at (dest) closure();
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return (at (dest) closure()) as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):TxFuture {
        val fid = plh().masterStore.getNextFutureId();
        val future = new TxFuture(id, fid, dest);
        val gr = new GlobalRef(future);
        
        try {
            at (dest) @Uncounted async {
                var result:Any = null;
                var exp:Exception = null;
                try {
                    closure();
                }catch(e:Exception) {
                    exp = e;
                }
                val x = result;
                val y = exp;
                at (gr) @Immediate ("fill_tx_future") async {
                    gr().fill(x, y);
                }
            }
        } catch (m:Exception) {
            future.fill(null, m);
        }
        return future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):TxFuture {
        val fid = plh().masterStore.getNextFutureId();
        val future = new TxFuture(id, fid, dest);
        val gr = new GlobalRef(future);
        
        try {
            at (dest) @Uncounted async {
                var result:Any = null;
                var exp:Exception = null;
                try {
                    result = closure();
                }catch(e:Exception) {
                    exp = e;
                }
                val x = result;
                val y = exp;
                at (gr) @Immediate ("fill_tx_future2") async {
                    gr().fill(x, y);
                }
            }
        } catch (m:Exception) {
            future.fill(null, m);
        }
        return future;
    }   
    
}