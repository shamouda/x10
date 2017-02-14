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

public class Tx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    private val root = GlobalRef[Tx](this);

    public transient val startTime:Long = System.nanoTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;
    
    private transient val excs:GrowableRail[CheckedThrowable]; 
    
    /* resilient mode variables */
    private transient var aborted:Boolean = false;
    private transient var txDescMap:ResilientNativeMap;
    private transient var activePlaces:PlaceGroup;
    
    /* Constants */
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private static val DISABLE_SLAVE = System.getenv("DISABLE_SLAVE") != null && System.getenv("DISABLE_SLAVE").equals("1");
    private static val DISABLE_DESC = System.getenv("DISABLE_DESC") != null && System.getenv("DISABLE_DESC").equals("1");
    
    public static val SUCCESS = 0n;
    public static val SUCCESS_RECOVER_STORE = 1n;

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
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, 
            activePlaces:PlaceGroup, txDescMap:ResilientNativeMap) {
        property(plh, id, mapName, members);
        if (!DISABLE_DESC) //enable desc requires enable slave
            assert(!DISABLE_SLAVE);
        excs = new GrowableRail[CheckedThrowable]();
        if (resilient) {
            this.activePlaces = activePlaces;
            this.txDescMap = txDescMap;
        }
        var membersStr:String = "";
        for (p in members)
            membersStr += p +" ";
        if (TM_DEBUG) Console.OUT.println("TX["+id+"] here["+here+"] started members["+membersStr+"]");
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return execute(GET_LOCAL, here, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def getRemote(dest:Place, key:String):Cloneable {
        return execute(GET_REMOTE, dest, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncGetRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_GET, dest, key, null, null, null, plh, id, mapName, members, root).future;
    }
    
    private def getLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.get(mapName, id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return execute(PUT_LOCAL, here, key, value, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def putRemote(dest:Place, key:String, value:Cloneable):Cloneable {
        return execute(PUT_REMOTE, dest, key, value, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Any] {
        return execute(ASYNC_PUT, dest, key, value, null, null, plh, id, mapName, members, root).future;
    }
    
    private def putLocal(key:String, value:Cloneable, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.put(mapName, id, key, value);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return execute(DELETE_LOCAL, here, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def deleteRemote(dest:Place, key:String):Cloneable {
        return execute(DELETE_REMOTE, dest, key, null, null, null, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncDeleteRemote(dest:Place, key:String):Future[Any] {
        return execute(ASYNC_DELETE, dest, key, null, null, null, plh, id, mapName, members, root).future;
    }
    
    private def deleteLocal(key:String, plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Cloneable {
        return plh().masterStore.delete(mapName, id, key);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return execute(KEYSET_LOCAL, here, null, null, null, null, plh, id, mapName, members, root).set; 
    }
    
    public def keySetRemote(dest:Place):Set[String] {
        return execute(KEYSET_REMOTE, dest, null, null, null, null, plh, id, mapName, members, root).set; 
    }
    
    public def asyncKeySetRemote(dest:Place):Future[Any] {
        return execute(ASYNC_KEYSET, dest, null, null, null, null, plh, id, mapName, members, root).future; 
    }
    
    private def keySetLocal(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String):Set[String] {
        return plh().masterStore.keySet(mapName, id);
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        execute(AT_VOID, dest, null, null, closure, null, plh, id, mapName, members, root);
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return execute(AT_RETURN, dest, null, null, null, closure, plh, id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):Future[Any] {
        return execute(ASYNC_AT_VOID, dest, null, null, closure, null, plh, id, mapName, members, root).future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):Future[Any] {
        return execute(ASYNC_AT_RETURN, dest, null, null, null, closure, plh, id, mapName, members, root).future;
    }
    
    /***************** Execution of All Operations ********************/
    private def execute(op:Int, dest:Place, key:String, value:Cloneable, closure_void:()=>void, closure_return:()=>Any, 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[Tx]):TxOpResult {
        assert (members.contains(dest));
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
        val startExec = System.nanoTime();
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
                plh().masterStore.addFuture(mapName, id, future);                
                return new TxOpResult(future);
            }
        }catch (ex:Exception) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"]  Failed Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
            try {
                val startIntAbort = System.nanoTime();
                internalAbort(ex, root);
                val endIntAbort = System.nanoTime();
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] internal abort Op["+opDesc(op)+"] time ["+((endIntAbort - startIntAbort)/1e6)+"] ms");
            }catch(abortEx:Exception) {}
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] here[" + here + "] after internal Abort throwing exception ["+ex.getMessage()+"] ...");
            throw ex;
        } finally {
            val endExec = System.nanoTime();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] execute Op["+opDesc(op)+"] time: [" + ((endExec-startExec)/1e6) + "] ms");
        }
        
    }
    /*********************** Abort ************************/
    
    private def internalAbort(ex:Exception, root:GlobalRef[Tx]) {
        val abortedPlaces = new ArrayList[Place]();
        if (ex instanceof ConflictException) {
            abortedPlaces.add( (ex as ConflictException).place);
        }
        finish at (root) async {
            root().abort(abortedPlaces);
        }
    }
    
    @Pinned public def abort() {
        abort(new ArrayList[Place]());
    }
    
    @Pinned private def abort(abortedPlaces:ArrayList[Place]) {
        atomic {
            if (!aborted) {
                aborted = true;
            }
            else
                return;
        }
        
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, mapName, members, root);
        }
        catch(ex:MultipleExceptions) {
            if (resilient) {
                try {
                    val deadMasters = getDeadPlaces(ex, members);
                    //some masters died while rolling back,ask slaves to abort
                    finalizeSlaves(false, deadMasters, plh, id, mapName, members, root);
                }
                catch(ex2:Exception) {
                    Console.OUT.println("Warning: ignoring exception during finalizeSlaves(false): " + ex2.getMessage());
                    ex2.printStackTrace();
                }
            }
            else {
                Console.OUT.println("Warning: ignoring exception during finalize(false): " + ex.getMessage());
                ex.printStackTrace();
            }
        }
        
        if (resilient && !DISABLE_DESC)
            deleteTxDesc();
        
        if (abortTime == -1)
            abortTime = System.nanoTime();
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    @Pinned public def commit():Int {
        return commit(false);
    }
    
    @Pinned public def commit(skipPhaseOne:Boolean):Int {
        assert(here.id == root.home.id);
        if (!skipPhaseOne) {
            try {
                val startWhen = System.nanoTime();
                commitPhaseOne(plh, id, mapName, members, root); // failures are fatal
                val endWhen = System.nanoTime();
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne time [" + ((endWhen-startWhen)/1e6) + "] ms");
                
                if (resilient && !DISABLE_DESC)
                    updateTxDesc(TxDesc.COMMITTING);
            } catch(ex:ConflictException) {
                val list = new ArrayList[Place]();
                list.add(ex.place);
                abort(list);
                throw ex;
            } catch(ex:DeadPlaceException) {
                val list = new ArrayList[Place]();
                list.add(ex.place);
                abort(list);
                throw ex;
            } catch(ex:MultipleExceptions) {
                val list = getDeadAndConflictingPlaces(ex);
                abort(list);
                throw ex;
            }
        }
        /*Transaction MUST Commit (if master is dead, commit at slave)*/
        if (skipPhaseOne && TM_DEBUG) {
            Console.OUT.println("Tx["+id+"] skip phase one");
        }
        
        val startWhen = System.nanoTime();
        commitPhaseTwo(plh, id, mapName, members, root);
        val endWhen = System.nanoTime();
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseTwo time [" + ((endWhen-startWhen)/1e6) + "] ms");
        
        if (resilient && !DISABLE_DESC)
            deleteTxDesc();
            
        commitTime = System.nanoTime();
        
        if (excs.size() > 0) {
            for (e in excs.toRail()) {
                Console.OUT.println("Tx["+id+"] excs>0  msg["+e.getMessage()+"]");
                e.printStackTrace();
            }
            return SUCCESS_RECOVER_STORE;
        }
        else
            return SUCCESS;
    }
    
    private def updateTxDesc(status:Long){
        val localTx = txDescMap.startLocalTransaction();
        try {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] updateTxDesc localTx["+localTx.id+"] started ...");
            val desc = localTx.get("tx"+id) as TxDesc;
            desc.status = status;
            localTx.put("tx"+id, desc);
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] updateTxDesc localTx["+localTx.id+"] completed ...");
    
        } catch(ex:Exception) {
            if(TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] updateTxDesc localTx["+localTx.id+"] failed exception["+ex.getMessage()+"] ...");
                ex.printStackTrace();
            }
            throw ex;
        }
    }
    private def deleteTxDesc(){
        try {
            val localTx = txDescMap.startLocalTransaction();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] deleteTxDesc localTx["+localTx.id+"] started ...");
            localTx.put("tx"+id, null);
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] deleteTxDesc localTx["+localTx.id+"] completed ...");
        }catch(ex:Exception) {
            Console.OUT.println("Warning: ignoring exception during deleteTxDesc: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
    
    private def commitPhaseOne(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne ...");
        
        plh().masterStore.waitForFutures(mapName, id);
        
        finish for (p in members) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne going to move to ["+p+"] ...");
            at (p) async {
                //check for local conflicts and remove readonly keys
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseOne : validate started ...");
                plh().masterStore.validate(mapName, id);
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseOne : validate done ...");
            
                if (resilient && !DISABLE_SLAVE) {
                    val log = plh().masterStore.getTxCommitLog(mapName, id);
                    if (log != null && log.size() > 0) {
                        val remainingEntries = new HashMap[String,Cloneable]();
                        
                        val iter = log.keySet().iterator();
                        while (iter.hasNext()) {
                            val key = iter.next();
                            val value = log.getOrThrow(key);
                            if (value.asyncRemoteCopySupported())
                                value.asyncRemoteCopy(id, mapName, key, plh);
                            else
                                remainingEntries.put(key, value);
                        }
                        if ( remainingEntries.size() > 0 ) {
                            //send txLog to slave (very important to be able to tolerate failures of masters just after prepare)
                            at (plh().slave) async {
                                plh().slaveStore.prepare(id, mapName, remainingEntries );
                            }
                        }
                    }
                }
            }
        }
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseTwo ...");
        try {
            //ask masters and slaves to commit
            finalize(true, null, plh, id, mapName, members, root);
        }
        catch(ex:MultipleExceptions) {
            if (TM_DEBUG) {
                if(TM_DEBUG) Console.OUT.println(here + "commitPhaseTwo Exception[" + ex.getMessage() + "]");
                ex.printStackTrace();
            }
            
            if (!resilient) {
                throw ex;
            }
            
            val deadMasters = getDeadPlaces(ex, members);
            //some masters have died, after validation
            //ask slaves to commit
            try {
                finalizeSlaves(true, deadMasters, plh, id, mapName, members, root);
            }
            catch(ex2:Exception) {
                ex2.printStackTrace();
                //FATAL Exception
                throw new Exception("FATAL ERROR: Master and Slave died together, Exception["+ex2.getMessage()+"] ");
            }
            
        }
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"]  here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");
        
        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
        finish for (p in members) {
            /*skip aborted places*/
            if (!commit && abortedPlaces.contains(p))
                continue;
            
            at (p) async {
                var ex:Exception = null;
                if (resilient && !DISABLE_SLAVE) {
                    val log = plh().masterStore.getTxCommitLog(mapName, id);
                    if (log != null && log.size() > 0) {
                        //ask slave to commit, slave's death is not fatal
                        try {
                            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] finalize here["+here+"] moving to slave["+plh().slave+"] to " + ( commit? "commit": "abort" ));
                            finish at (plh().slave) async {
                                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] finalize here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                                if (commit)
                                    plh().slaveStore.commit(id);
                                else
                                    plh().slaveStore.abort(id);
                            }
                        }catch (e:Exception) {
                            ex = e;
                        }
                    }
                }
                    
                if (commit)
                    plh().masterStore.commit(mapName, id);
                else
                    plh().masterStore.abort(mapName, id);
                    
                if (resilient && ex != null) {
                    val slaveEx = ex;
                    at (root) async {
                        atomic root().excs.add(slaveEx as CheckedThrowable);
                    }
                }
            }
        }
    }
    
    private def finalizeSlaves(commit:Boolean, deadMasters:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[Tx]) {
        
        if (DISABLE_SLAVE)
            return;
        
        //ask slaves to commit (their master died, 
        //and we need to resolve the slave's pending transactions)
        finish for (p in deadMasters) {
            assert(members.contains(p));
            val slave = activePlaces.next(p);
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] finalizeSlaves here["+here+"] moving to slave["+plh().slave+"] to " + ( commit? "commit": "abort" ));
            at (slave) async {
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] finalizeSlaves here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                if (commit)
                    plh().slaveStore.commit(id);
                else
                    plh().slaveStore.abort(id);
            }
        }
    }
    
    public static def opDesc(op:Int) {
        switch(op) {
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
    
    private static def getDeadAndConflictingPlaces(mulExp:MultipleExceptions) {
        val list = new ArrayList[Place]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                list.add(dpe.place);
            }
        }
        val confExList = mulExp.getExceptionsOfType[ConflictException]();
        if (confExList != null) {
            for (ce in confExList) {
                list.add(ce.place);
            }
        }
        return list;
    }
    
    private static def getDeadPlaces(mulExp:MultipleExceptions, members:PlaceGroup) {
        val list = new ArrayList[Place]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                if (members.contains(dpe.place))
                    list.add(dpe.place);
            }
        }
        return list;
    }
    
}

class TxOpResult {
    var set:Set[String];
    var value:Any;
    var future:Future[Any];
    public def this(s:Set[String]) {
        set = s;
    }
    public def this(v:Any) {
        value = v;
    }
    public def this(f:Future[Any]) {
        future = f;
    }
}