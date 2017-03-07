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

public class LockingTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, requests:ArrayList[LockingRequest]) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public transient val startTime:Long = Timer.milliTime(); ////
	public transient var lockingElapsedTime:Long = -1;  //////
	public transient var processingElapsedTime:Long = -1; //// (including waitTime)
	public transient var waitElapsedTime:Long = -1; ///
    public transient var unlockingElapsedTime:Long = -1; ///////
    public transient var totalElapsedTime:Long = -1; //////
   
    /* Constants */
    
    private static val GET_LOCAL = 0n;
    private static val GET_REMOTE = 1n;
    private static val PUT_LOCAL = 2n;
    private static val PUT_REMOTE = 3n;
    private static val DELETE_LOCAL = 4n;
    private static val DELETE_REMOTE = 5n;
    private static val KEYSET_LOCAL = 6n;
    private static val KEYSET_REMOTE = 7n;
    private static val AT_VOID = 8n;
    private static val AT_RETURN = 9n;
    private static val ASYNC_GET = 10n;
    private static val ASYNC_PUT = 11n;
    private static val ASYNC_DELETE = 12n;
    private static val ASYNC_KEYSET = 13n;
    private static val ASYNC_AT_VOID = 14n;
    private static val ASYNC_AT_RETURN = 15n;
    
    private static val LOCK = 20n;
    private static val UNLOCK = 21n;
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, requests:ArrayList[LockingRequest]) {
        property(plh, id, mapName, requests);
        var membersStr:String = "";
        for (p in members)
            membersStr += p +" ";
        if (TM_DEBUG) Console.OUT.println("LockingTx["+id+"] here["+here+"] started members["+membersStr+"]");
    }
    
    public def setWaitElapsedTime(t:Long) {
    	waitElapsedTime = t;
    }
    
    /****************lock and unlock all keys**********************/

    public def lock() {
        execute(LOCK, here, null, null, null, null, plh, id, mapName, requests);
    }

    public def unlock() {
        execute(UNLOCK, here, null, null, null, null, plh, id, mapName, requests);
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return execute(GET_LOCAL, here, key, null, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def getRemote(dest:Place, key:String):Cloneable {
        return execute(GET_REMOTE, dest, key, null, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def asyncGetRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_GET, dest, key, null, null, null, plh, id, mapName, null).future;
    }
    
    private def getLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.get(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return execute(PUT_LOCAL, here, key, value, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def putRemote(dest:Place, key:String, value:Cloneable):Cloneable {
        return execute(PUT_REMOTE, dest, key, value, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Any] {
        return execute(ASYNC_PUT, dest, key, value, null, null, plh, id, mapName, null).future;
    }
    
    private def putLocal(key:String, value:Cloneable, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.put(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return execute(DELETE_LOCAL, here, key, null, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def deleteRemote(dest:Place, key:String):Cloneable {
        return execute(DELETE_REMOTE, dest, key, null, null, null, plh, id, mapName, null).value as Cloneable;
    }
    
    public def asyncDeleteRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_DELETE, dest, key, null, null, null, plh, id, mapName, null).future;
    }
    
    private def deleteLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.delete(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return execute(KEYSET_LOCAL, here, null, null, null, null, plh, id, mapName, null).set; 
    }
    
    public def keySetRemote(dest:Place):Set[String] {
        return execute(KEYSET_REMOTE, dest, null, null, null, null, plh, id, mapName, null).set; 
    }
    
    public def asyncKeySetRemote(dest:Place):Future[Any] {
        return execute(ASYNC_KEYSET, dest, null, null, null, null, plh, id, mapName, null).future; 
    }
    
    private def keySetLocal(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        execute(AT_VOID, dest, null, null, closure, null, plh, id, mapName, null);
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return execute(AT_RETURN, dest, null, null, null, closure, plh, id, mapName, null).value as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
        return execute(ASYNC_AT_VOID, dest, null, null, closure, null, plh, id, mapName, null).future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
        return execute(ASYNC_AT_RETURN, dest, null, null, null, closure, plh, id, mapName, null).future;
    }
    
    /***************** Execution of All Operations ********************/
    private def execute(op:Int, dest:Place, key:String, value:Cloneable, closure_void:()=>void, closure_return:()=>Any, 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, requests:ArrayList[LockingRequest]):TxOpResult {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
        val startExec = Timer.milliTime();
        try {
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
            else if (op == LOCK) {
                val startLock = Timer.milliTime();
                finish for (req in requests) {                	
                    at (req.dest) async {
                    	Runtime.increaseParallelism();
                        for (var i:Long = 0; i < req.keys.size ; i++) {
                        	//Console.OUT.println("locking " + req.keys(i).key + "  rw: " + req.keys(i).rw);
                            if (req.keys(i).rw)
                                plh().masterStore.lockRead(mapName, id, req.keys(i).key);
                            else
                                plh().masterStore.lockWrite(mapName, id, req.keys(i).key);
                        }
                        Runtime.decreaseParallelism(1n);
                    }
                }
                lockingElapsedTime = Timer.milliTime() - startLock;
                return null;
            }
            else if (op == UNLOCK) {
                val startUnlock = Timer.milliTime();
                finish for (req in requests) {
                    at (req.dest) async {
                        for (var i:Long = 0; i < req.keys.size ; i++) {
                        	//Console.OUT.println("unlocking " + req.keys(i).key + "  rw: " + req.keys(i).rw);
                            if (req.keys(i).rw)
                                plh().masterStore.unlockRead(mapName, id, req.keys(i).key);
                            else
                                plh().masterStore.unlockWrite(mapName, id, req.keys(i).key);
                        }
                    }
                }
                unlockingElapsedTime = Timer.milliTime() - startUnlock;
                totalElapsedTime = Timer.milliTime() - startTime;
                return null;
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
        }catch (ex:Exception) {
            if(TM_DEBUG) {
            	Console.OUT.println("Tx["+id+"]  Failed Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
            	ex.printStackTrace();
            }
            throw ex;  // someone must call Tx.abort
        } finally {
            val endExec = Timer.milliTime();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] execute Op["+opDesc(op)+"] time: [" + ((endExec-startExec)) + "] ms");
        }
        
    }
    
    
    public static def opDesc(op:Int) {
        switch(op) {
            case LOCK: return "LOCK";
            case UNLOCK: return "UNLOCK";
            case GET_LOCAL: return "GET_LOCAL";
            case GET_REMOTE: return "GET_REMOTE";
            case PUT_LOCAL: return "PUT_LOCAL";
            case PUT_REMOTE: return "PUT_REMOTE";
            case DELETE_LOCAL: return "DELETE_LOCAL";
            case DELETE_REMOTE: return "DELETE_REMOTE";
            case KEYSET_LOCAL: return "KEYSET_LOCAL";
            case KEYSET_REMOTE: return "KEYSET_REMOTE";
            case AT_VOID: return "AT_VOID";
            case AT_RETURN: return "AT_RETURN";
            case ASYNC_GET: return "ASYNC_GET";
            case ASYNC_PUT: return "ASYNC_PUT";
            case ASYNC_DELETE: return "ASYNC_DELETE";
            case ASYNC_KEYSET: return "ASYNC_KEYSET";
            case ASYNC_AT_VOID: return "ASYNC_AT_VOID";
            case ASYNC_AT_RETURN: return "ASYNC_AT_RETURN";
        }
        return "";
    }

    
}
