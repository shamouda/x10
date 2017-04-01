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
import x10.util.resilient.localstore.tx.commit.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;
import x10.util.concurrent.Lock;

public class Tx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) extends AbstractTx {
    private val root = GlobalRef[Tx](this);
    private val commitHandler:CommitHandler;
    
    public transient val startTime:Long = Timer.milliTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;
    
    // consumed time
    public transient var processingElapsedTime:Long = 0; ////including waitTime
    public transient var waitElapsedTime:Long = 0;
    public transient var txLoggingElapsedTime:Long = 0;
    
    /* resilient mode variables */
    private transient var aborted:Boolean = false;
    private transient var txDescMap:ResilientNativeMap;
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers, txDescMap:ResilientNativeMap) {
        property(plh, id, mapName, members);
        if (!TxConfig.get().DISABLE_TX_LOGGING) //enable desc requires enable slave
            assert(!TxConfig.get().DISABLE_SLAVE);
        
        if (resilient) {
            this.txDescMap = txDescMap;
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("TX["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] started members["+members.toString()+"]");
        	
        if (resilient) {
        	if (TxConfig.get().TM_REP.equals("lazy")) {
        		commitHandler = new LazyReplicationCommitHandler(plh, id, mapName, members, txDescMap);
        	}
        	else
        		commitHandler = new EagerReplicationCommitHandler(plh, id, mapName, members, txDescMap);
        }
        else
        	commitHandler = new NonResilientCommitHandler(plh, id, mapName, members, txDescMap);
    }
    
    /********** Setting the pre-commit time for statistical analysis **********/   
    public def setWaitElapsedTime(t:Long) {
        waitElapsedTime = t;
    }
    
    public def getPhase1ElapsedTime() = commitHandler.phase1ElapsedTime;
    public def getPhase2ElapsedTime() = commitHandler.phase2ElapsedTime;
    public def getTxLoggingElapsedTime() = commitHandler.txLoggingElapsedTime;
    
    public def noop(key:String):Cloneable {
        return null;
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return execute(GET_LOCAL, here.id, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def getRemote(dest:Long, key:String):Cloneable {
        return execute(GET_REMOTE, dest, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncGetRemote(dest:Long, key:String):Future[Any] {
        return execute(ASYNC_GET, dest, key, null, null, null, plh, id, mapName, members, root).future;
    }
    
    private def getLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.get(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return execute(PUT_LOCAL, here.id, key, value, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def putRemote(dest:Long, key:String, value:Cloneable):Cloneable {
        return execute(PUT_REMOTE, dest, key, value, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncPutRemote(dest:Long, key:String, value:Cloneable):Future[Any] {
        return execute(ASYNC_PUT, dest, key, value, null, null, plh, id, mapName, members, root).future;
    }
    
    private def putLocal(key:String, value:Cloneable, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.put(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return execute(DELETE_LOCAL, here.id, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def deleteRemote(dest:Long, key:String):Cloneable {
        return execute(DELETE_REMOTE, dest, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncDeleteRemote(dest:Long, key:String):Future[Any] {
        return execute(ASYNC_DELETE, dest, key, null, null, null, plh, id, mapName, members, root).future;
    }
    
    private def deleteLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.delete(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return execute(KEYSET_LOCAL, here.id, null, null, null, null, plh, id, mapName, members, root).set; 
    }
    
    public def keySetRemote(dest:Long):Set[String] {
        return execute(KEYSET_REMOTE, dest, null, null, null, null, plh, id, mapName, members, root).set; 
    }
    
    public def asyncKeySetRemote(dest:Long):Future[Any] {
        return execute(ASYNC_KEYSET, dest, null, null, null, null, plh, id, mapName, members, root).future; 
    }
    
    private def keySetLocal(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Long, closure:()=>void) {
        execute(AT_VOID, dest, null, null, closure, null, plh, id, mapName, members, root);
    }
    
    public def syncAt(dest:Long, closure:()=>Any):Cloneable {
        return execute(AT_RETURN, dest, null, null, null, closure, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncAt(dest:Long, closure:()=>void):Future[Any] {
        return execute(ASYNC_AT_VOID, dest, null, null, closure, null, plh, id, mapName, members, root).future;
    }
    
    public def asyncAt(dest:Long, closure:()=>Any):Future[Any] {
        return execute(ASYNC_AT_RETURN, dest, null, null, null, closure, plh, id, mapName, members, root).future;
    }
    
    /***************** Execution of All Operations ********************/
    private def execute(op:Int, destIndx:Long, key:String, value:Cloneable, closure_void:()=>void, closure_return:()=>Any, 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers, root:GlobalRef[Tx]):TxOpResult {
        val dest = members.getPlace(destIndx);
        assert (members.contains(dest));
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
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
            else {  /*Remote Async Operations*/
            	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] (2) ...");
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
                        	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] moved to here["+here+"] (3) ...");
                            closure_void();
                            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] finished here["+dest+"] (4) ...");
                        }
                        else if (op == ASYNC_AT_RETURN) {
                        	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] moved to here["+here+"] (3) ...");
                            result = closure_return();
                            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] finished here["+dest+"] (4) ...");
                        }
                        return result;
                    }
                );
                return new TxOpResult(future);
            }
        }catch (ex:Exception) {
            if (TxConfig.get().TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Failed Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
                ex.printStackTrace();
            }
            throw ex;  // someone must call Tx.abort
        } finally {
            val endExec = Timer.milliTime();
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " execute Op["+opDesc(op)+"] time: [" + ((endExec-startExec)) + "] ms");
        }
        
    }
    /*********************** Abort ************************/  
    @Pinned public def abort() {
        abort(new ArrayList[Place]());
    }
    
    @Pinned public def abort(ex:Exception) {
        val list = CommitHandler.getDeadAndConflictingPlaces(ex);
        abort(list);
    }
    
    @Pinned private def abort(abortedPlaces:ArrayList[Place]) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " abort (abortPlaces.size = " + abortedPlaces.size() + ") alreadyAborted = " + aborted);
        if (!aborted)
            aborted = true;
        else 
            return;
        
        commitHandler.abort(abortedPlaces);
        
        if (abortTime == -1) {
            abortTime = Timer.milliTime();
          if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " aborted, allTxTime ["+(abortTime-startTime)+"] ms");
        }
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    @Pinned public def commit():Int {
        return commit(false);
    }
    
    @Pinned public def commit(skipPhaseOne:Boolean) {
        assert(here.id == root.home.id);
        
        val success = commitHandler.commit(skipPhaseOne);

        commitTime = Timer.milliTime();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " committed, allTxTime [" + (commitTime-startTime) + "] ms");
        
        return success;
    }
        
}