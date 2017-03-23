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
import x10.util.HashSet;
import x10.util.concurrent.Future;
import x10.util.Set;

public class NestingTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, depth:Long) {
    private val members = new HashSet[Place]();
    private val futures = new ArrayList[Future[Any]]();
    private var parent:GlobalRef[NestingTx];
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, depth:Long) {
        property(plh, id, mapName, depth);
    }
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, depth:Long, parent:GlobalRef[NestingTx]) {
        property(plh, id, mapName, depth);
        this.parent = parent;
    }
    
    public def addMember(target:Place) {
        if (!members.contains(target))
    	    members.add(target);
    }
    
    public def addFuture(f:Future[Any]) {
        futures.add(f);
    }
    
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
        return deleteLocal(key, plh, id, mapName);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** Commit/Abort methods ********************/
    public def commit() {
        if (depth == 0)
            rootCommit();
        else
            childCommit();
    }
    
    public def rootCommit() {
        
            
    }
    
    public def childCommit() {
        
    }
    
    public def abort() {
        if (depth == 0)
            rootAbort();
        else
            childAbort();
            
    }
    
    public def rootAbort() {
            
    }
    
    public def childAbort() {
        
    }
    
}