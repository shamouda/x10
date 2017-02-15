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
import x10.util.concurrent.Future;

public class LockManager (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    /***************** Locking ********************/
    
    public def lockWrite(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) plh().masterStore.lockWrite(mapName, id, key1);
            at (p2) plh().masterStore.lockWrite(mapName, id, key2);
        }
        else {
            at (p2) plh().masterStore.lockWrite(mapName, id, key2);
            at (p1) plh().masterStore.lockWrite(mapName, id, key1);
        }
    }
    public def unlockWrite(p1:Place, key1:String, p2:Place, key2:String, id:Long) {
        if (key1.hashCode() < key2.hashCode()) {
            at (p1) plh().masterStore.unlockWrite(mapName, id, key1);
            at (p2) plh().masterStore.unlockWrite(mapName, id, key2);
        }
        else {
            at (p2) plh().masterStore.unlockWrite(mapName, id, key2);
            at (p1) plh().masterStore.unlockWrite(mapName, id, key1);
        }
    }
    
    public def lockWrite(key:String) {
        plh().masterStore.lockWrite(mapName, id, key);
    }
    
    public def unlockWrite(key:String) {
        plh().masterStore.unlockWrite(mapName, id, key);
    }
    
    public def lockRead(key:String) {
        plh().masterStore.lockRead(mapName, id, key);
    }
    
    public def unlockRead(key:String) {
        plh().masterStore.unlockRead(mapName, id, key);
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
    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
        val future = Future.make[Any]( () => at (dest) { closure(); return null;} );                
        return future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
        val future = Future.make[Any]( () => at (dest) { return closure(); } );                
        return future;
    }   
    
}