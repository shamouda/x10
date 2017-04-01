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

public abstract class AbstractTx {
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    /* Constants */
    protected static val GET_LOCAL = 0n;
    protected static val GET_REMOTE = 1n;
    protected static val PUT_LOCAL = 2n;
    protected static val PUT_REMOTE = 3n;
    protected static val DELETE_LOCAL = 4n;
    protected static val DELETE_REMOTE = 5n;
    protected static val KEYSET_LOCAL = 6n;
    protected static val KEYSET_REMOTE = 7n;
    protected static val AT_VOID = 8n;
    protected static val AT_RETURN = 9n;
    protected static val ASYNC_GET = 10n;
    protected static val ASYNC_PUT = 11n;
    protected static val ASYNC_DELETE = 12n;
    protected static val ASYNC_KEYSET = 13n;
    protected static val ASYNC_AT_VOID = 14n;
    protected static val ASYNC_AT_RETURN = 15n;
    protected static val LOCK = 20n;
    protected static val UNLOCK = 21n;
    
    public static val SUCCESS = 0n;
    public static val SUCCESS_RECOVER_STORE = 1n;
    
        
    /***************** Get ********************/
    public abstract def get(key:String):Cloneable;
    
    public abstract def getRemote(dest:Long, key:String):Cloneable;
    
    public abstract def asyncGetRemote(dest:Long, key:String):Future[Any];
    
    /***************** PUT ********************/
    public abstract def put(key:String, value:Cloneable):Cloneable;
    
    public abstract def putRemote(dest:Long, key:String, value:Cloneable):Cloneable;
    
    public abstract def asyncPutRemote(dest:Long, key:String, value:Cloneable):Future[Any];
    
    /***************** Delete ********************/
    public abstract def delete(key:String):Cloneable;
    
    public abstract def deleteRemote(dest:Long, key:String):Cloneable;
    
    public abstract def asyncDeleteRemote(dest:Long, key:String):Future[Any];
    
    /***************** KeySet ********************/
    public abstract def keySet():Set[String];
    
    public abstract def keySetRemote(dest:Long):Set[String];
    
    public abstract def asyncKeySetRemote(dest:Long):Future[Any] ;
    
    /***************** At ********************/
    public abstract def syncAt(dest:Long, closure:()=>void):void;
    
    public abstract def syncAt(dest:Long, closure:()=>Any):Cloneable ;
    
    public abstract def asyncAt(dest:Long, closure:()=>void):Future[Any] ;
    
    public abstract def asyncAt(dest:Long, closure:()=>Any):Future[Any] ;
 
    public abstract def setWaitElapsedTime(t:Long):void;
    
    protected static def opDesc(op:Int) {
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
