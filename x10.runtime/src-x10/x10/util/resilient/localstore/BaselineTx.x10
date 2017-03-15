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
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;

public class BaselineTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String) extends AbstractTx {  

	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup) {
        property(plh, id, mapName);
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return execute(GET_LOCAL, here, key, null, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def getRemote(dest:Place, key:String):Cloneable {
        return execute(GET_REMOTE, dest, key, null, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def asyncGetRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_GET, dest, key, null, null, null, plh, id, mapName).future;
    }
    
    private def getLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.get(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return execute(PUT_LOCAL, here, key, value, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def putRemote(dest:Place, key:String, value:Cloneable):Cloneable {
        return execute(PUT_REMOTE, dest, key, value, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Any] {
        return execute(ASYNC_PUT, dest, key, value, null, null, plh, id, mapName).future;
    }
    
    private def putLocal(key:String, value:Cloneable, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.put(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return execute(DELETE_LOCAL, here, key, null, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def deleteRemote(dest:Place, key:String):Cloneable {
        return execute(DELETE_REMOTE, dest, key, null, null, null, plh, id, mapName).value as Cloneable;
    }
    
    public def asyncDeleteRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_DELETE, dest, key, null, null, null, plh, id, mapName).future;
    }
    
    private def deleteLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.delete(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return execute(KEYSET_LOCAL, here, null, null, null, null, plh, id, mapName).set; 
    }
    
    public def keySetRemote(dest:Place):Set[String] {
        return execute(KEYSET_REMOTE, dest, null, null, null, null, plh, id, mapName).set; 
    }
    
    public def asyncKeySetRemote(dest:Place):Future[Any] {
        return execute(ASYNC_KEYSET, dest, null, null, null, null, plh, id, mapName).future; 
    }
    
    private def keySetLocal(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        execute(AT_VOID, dest, null, null, closure, null, plh, id, mapName);
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return execute(AT_RETURN, dest, null, null, null, closure, plh, id, mapName).value as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
        return execute(ASYNC_AT_VOID, dest, null, null, closure, null, plh, id, mapName).future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
        return execute(ASYNC_AT_RETURN, dest, null, null, null, closure, plh, id, mapName).future;
    }
    
    /***************** Execution of All Operations ********************/
    private def execute(op:Int, dest:Place, key:String, value:Cloneable, closure_void:()=>void, closure_return:()=>Any, 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):TxOpResult {
        if (op == GET_LOCAL) {
            return new TxOpResult(getLocal(key, plh, id, mapName));
        }
        else if (op == GET_REMOTE) {
            return new TxOpResult (at (dest) getLocal(key, plh, id, mapName));
        }
        else if (op == PUT_LOCAL) {
            return new TxOpResult(putLocal(key, value, plh, id, mapName)); 
        }
        else if (op == PUT_REMOTE) {
            return new TxOpResult(at (dest) putLocal(key, value, plh, id, mapName));
        }
        else if (op == DELETE_LOCAL) {
            return new TxOpResult(deleteLocal(key, plh, id, mapName)); 
        }
        else if (op == DELETE_REMOTE) {
            return new TxOpResult(at (dest) deleteLocal(key, plh, id, mapName));
        }
        else if (op == KEYSET_LOCAL) {
            return new TxOpResult(keySetLocal(plh, id, mapName));
        }
        else if (op == KEYSET_REMOTE) {
            return new TxOpResult(at (dest) keySetLocal(plh, id, mapName));
        }
        else if (op == AT_VOID) {
            at (dest) closure_void();
            return null;
        }
        else if (op == AT_RETURN) {
            return new TxOpResult(at (dest) closure_return());
        }
        else {  /*Remote Async Operations*/
            val future = Future.make[Any](() => 
                at (dest) {
                    var result:Any = null;
                    if (op == ASYNC_GET) {
                        result = getLocal(key, plh, id, mapName);
                    }
                    else if (op == ASYNC_PUT) {
                        result = putLocal(key, value, plh, id, mapName);
                    }
                    else if (op == ASYNC_DELETE) {
                        result = deleteLocal(key, plh, id, mapName);
                    }
                    else if (op == ASYNC_KEYSET) {
                        result = keySetLocal(plh, id, mapName);
                    }
                    else if (op == ASYNC_AT_VOID) {
                        closure_void();
                    }
                    else if (op == ASYNC_AT_RETURN) {
                        result = closure_return();
                    }
                    return result;
                }
            );
            return new TxOpResult(future);
        }
    }
    
    public def setWaitElapsedTime(t:Long) {
    	
    }
}
