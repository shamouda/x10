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
import x10.util.concurrent.Lock;

public class Tx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup) extends AbstractTx {
    private static val TM_REP = System.getenv("TM_REP") == null ? "lazy" : System.getenv("TM_REP");
    
    private val root = GlobalRef[Tx](this);

    public transient val startTime:Long = Timer.milliTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;
    
    // consumed time
    public transient var processingElapsedTime:Long = 0; ////including waitTime
    public transient var waitElapsedTime:Long = 0;
    public transient var phase1ElapsedTime:Long = 0;
    public transient var phase2ElapsedTime:Long = 0;
    public transient var txLoggingElapsedTime:Long = 0;
    
    private transient var excs:GrowableRail[CheckedThrowable];
    private transient var excsLock:Lock;
    
    /* resilient mode variables */
    private transient var aborted:Boolean = false;
    private transient var txDescMap:ResilientNativeMap;
    private transient var activePlaces:PlaceGroup;
    
    /* Constants */
    private static val DISABLE_SLAVE = System.getenv("DISABLE_SLAVE") != null && System.getenv("DISABLE_SLAVE").equals("1");
    private static val DISABLE_DESC = System.getenv("DISABLE_DESC") != null && System.getenv("DISABLE_DESC").equals("1");
    
    public static val SUCCESS = 0n;
    public static val SUCCESS_RECOVER_STORE = 1n;

    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:PlaceGroup, 
            activePlaces:PlaceGroup, txDescMap:ResilientNativeMap) {
        property(plh, id, mapName, members);
        if (!DISABLE_DESC) //enable desc requires enable slave
            assert(!DISABLE_SLAVE);
        
        if (resilient) {
            this.activePlaces = activePlaces;
            this.txDescMap = txDescMap;
            this.excs = new GrowableRail[CheckedThrowable]();
            this.excsLock = new Lock();
        }
        var membersStr:String = "";
        for (p in members)
            membersStr += p +" ";
        if (TM_DEBUG) Console.OUT.println("TX["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] started members["+membersStr+"]");
    }
    
    /********** Setting the pre-commit time for statistical analysis **********/   
    public def setWaitElapsedTime(t:Long) {
        waitElapsedTime = t;
    }
    
    public def noop(key:String):Cloneable {
        return null;
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
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
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
            	if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] (2) ...");
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
                        	if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] moved to here["+here+"] (3) ...");
                            closure_void();
                            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] finished here["+dest+"] (4) ...");
                        }
                        else if (op == ASYNC_AT_RETURN) {
                        	if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] moved to here["+here+"] (3) ...");
                            result = closure_return();
                            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Start Op["+opDesc(op)+"] finished here["+dest+"] (4) ...");
                        }
                        return result;
                    }
                );
                return new TxOpResult(future);
            }
        }catch (ex:Exception) {
            if(TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " Failed Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
                ex.printStackTrace();
            }
            throw ex;  // someone must call Tx.abort
        } finally {
            val endExec = Timer.milliTime();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " execute Op["+opDesc(op)+"] time: [" + ((endExec-startExec)) + "] ms");
        }
        
    }
    /*********************** Abort ************************/  
    @Pinned public def abort() {
        abort(new ArrayList[Place]());
    }
    
    @Pinned public def abort(ex:Exception) {
           val list = getDeadAndConflictingPlaces(ex);
        abort(list);
    }
    
    @Pinned private def abort(abortedPlaces:ArrayList[Place]) {
        //ssif (TM_DEBUG) 
        	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " abort (abortPlaces.size = " + abortedPlaces.size() + ") alreadyAborted = " + aborted);
        if (!aborted)
            aborted = true;
        else 
            return;
        
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, members, root);
        }
        catch(ex:MultipleExceptions) {
            if (resilient) {
                try {
                    val deadMasters = getDeadPlaces(ex, members);
                    //some masters died while rolling back,ask slaves to abort
                    finalizeSlaves(false, deadMasters, plh, id, members, root);
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
        
        if (abortTime == -1) {
            abortTime = Timer.milliTime();
          //ssif (TM_DEBUG) 
            	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " aborted, allTxTime ["+(abortTime-startTime)+"] ms");
        }
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    @Pinned public def commit():Int {
        return commit(false);
    }
    
    @Pinned public def commit(skipPhaseOne:Boolean):Int {
        assert(here.id == root.home.id);
        if (!skipPhaseOne) {
            try {
                commitPhaseOne(plh, id, members, root); // failures are fatal
                
                if (resilient && !DISABLE_DESC)
                    updateTxDesc(TxDesc.COMMITTING);
            } catch(ex:Exception) {
                val list = getDeadAndConflictingPlaces(ex);
                abort(list);
                throw ex;
            }
        }
        /*Transaction MUST Commit (if master is dead, commit at slave)*/
        if (skipPhaseOne && TM_DEBUG) {
            Console.OUT.println("Tx["+id+"] skip phase one");
        }
        
        val startP2 = Timer.milliTime();
        val p2success = commitPhaseTwo(plh, id, members, root);
        val endP2 = Timer.milliTime();
      //ssif(TM_DEBUG) 
        	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo time [" + (endP2-startP2) + "] ms");
        
        
        if (resilient && !DISABLE_DESC ){
            if (!TM_REP.equals("lazy"))
                deleteTxDesc();
            else
                updateTxDesc(TxDesc.COMMITTED);
        }
            

        commitTime = Timer.milliTime();
      //ssif (TM_DEBUG) 
        	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " committed, allTxTime [" + (commitTime-startTime) + "] ms");
        
        if (resilient && excs.size() > 0 || p2success == SUCCESS_RECOVER_STORE) {
            for (e in excs.toRail()) {
                Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " excs>0  msg["+e.getMessage()+"]");
                e.printStackTrace();
            }
            return SUCCESS_RECOVER_STORE;
        }
        else
            return SUCCESS;
    }
    
    private def updateTxDesc(status:Long){
        val start = Timer.milliTime();
        val localTx = txDescMap.startLocalTransaction();
        try {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] started ...");
            val desc = localTx.get("tx"+id) as TxDesc;
            //while recovering a transaction, the current place will not have the TxDesc stored, NPE can be thrown
            if (desc != null) {
                desc.status = status;
                localTx.put("tx"+id, desc);
            }
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] completed ...");
    
        } catch(ex:Exception) {
            if(TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] failed exception["+ex.getMessage()+"] ...");
                ex.printStackTrace();
            }
            throw ex;
        }
        finally {
            txLoggingElapsedTime += Timer.milliTime() - start;
        }
    }
    private def deleteTxDesc(){
        val start = Timer.milliTime();
        try {
            val localTx = txDescMap.startLocalTransaction();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] started ...");
            localTx.put("tx"+id, null);
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] completed ...");
        }catch(ex:Exception) {
            Console.OUT.println("Warning: ignoring exception during deleteTxDesc: " + ex.getMessage());
            ex.printStackTrace();
        }
        finally {
            txLoggingElapsedTime += Timer.milliTime() - start;
        }
    }
    
    private def commitPhaseOne(plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        val start = Timer.milliTime();
        try {
            val placeIndex = plh().virtualPlaceId;
          //ssif(TM_DEBUG) 
            	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne ...");
            finish for (p in members) {
                if ((resilient && !DISABLE_SLAVE) || TxConfig.getInstance().VALIDATION_REQUIRED) {
                	//ssif(TM_DEBUG) 
                    	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne going to move to ["+p+"] ...");
                    at (p) async {
                    	commitPhaseOne_local(plh, id, placeIndex);
                    }
                }
                else
                    if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate NOT required ...");
            }
        } finally {
            phase1ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    private def commitPhaseOne_local(plh:PlaceLocalHandle[LocalStore], id:Long, placeIndex:Long) {
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate started ...");
            plh().masterStore.validate(id);
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate done ...");
        }
    
        if (resilient && !DISABLE_SLAVE) {
        	//ssif(TM_DEBUG) 
        	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"]  ...");
            val log = plh().masterStore.getTxCommitLog(id);
            if (log != null && log.size() > 0) {                            
                //send txLog to slave (very important to be able to tolerate failures of masters just after prepare)
                at (plh().slave) async {
                    plh().slaveStore.prepare(id, log, placeIndex);
                }
            }
            //ssif(TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"] DONE ...");
        }
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        //ssif(TM_DEBUG) 
    	Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo ...");
        val start = Timer.milliTime();
        try {
            //ask masters and slaves to commit
            finalize(true, null, plh, id, members, root);
            return SUCCESS;
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
                finalizeSlaves(true, deadMasters, plh, id, members, root);
            }
            catch(ex2:Exception) {
                ex2.printStackTrace();
                //FATAL Exception
                throw new Exception("FATAL ERROR: Master and Slave died together, Exception["+ex2.getMessage()+"] ");
            }
        } finally {
            phase2ElapsedTime = Timer.milliTime() - start;
        }
        return SUCCESS_RECOVER_STORE;
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");
        val placeIndex = plh().virtualPlaceId;
        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
        finish for (p in members) {
            /*skip aborted places*/
            if (!commit && abortedPlaces.contains(p))
                continue;
            
            if (p.id == here.id) {
            	finalizeLocal(commit, plh, id, root);
            }
            else {
            	at (p) async {
            		finalizeLocal(commit, plh, id, root);
            	}
            }
        }
    }
    
    
    private def finalizeLocal(commit:Boolean, plh:PlaceLocalHandle[LocalStore], id:Long, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal  here["+here+"] " + ( commit? " Commit Local Started ": " Abort Local Started " ) + " ...");
        var ex:Exception = null;
        if (resilient && !TM_REP.equals("lazy") && !DISABLE_SLAVE) {
            try {
            val log = plh().masterStore.getTxCommitLog(id);
            if (log != null && log.size() > 0) {
                //ask slave to commit, slave's death is not fatal
                try {
                    if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal here["+here+"] moving to slave["+plh().slave+"] to " + ( commit? "commit": "abort" ));
                    finish at (plh().slave) async {
                        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                        if (commit)
                            plh().slaveStore.commit(id);
                        else
                            plh().slaveStore.abort(id);
                    }
                }catch (e:Exception) {
                    ex = e;
                }
            }
            }catch(tm:Exception) {
                tm.printStackTrace();
                throw tm;
            }
        }
            
        if (commit)
            plh().masterStore.commit(id);
        else
            plh().masterStore.abort(id);
            
        if (resilient && ex != null) {
            val slaveEx = ex;
            at (root) async {
                excsLock.lock();
                root().excs.add(slaveEx as CheckedThrowable);
                excsLock.unlock();
            }
        }
    }
    
    
    private def finalizeSlaves(commit:Boolean, deadMasters:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        
        if (DISABLE_SLAVE)
            return;
        
        if (!TM_REP.equals("lazy")) {
            //ask slaves to commit (their master died, 
            //and we need to resolve the slave's pending transactions)
            finish for (p in deadMasters) {
                assert(members.contains(p));
                val slave = activePlaces.next(p);
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moving to slave["+plh().slave+"] to " + ( commit? "commit": "abort" ));
                at (slave) async {
                    if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                    if (commit)
                        plh().slaveStore.commit(id);
                    else
                        plh().slaveStore.abort(id);
                }
            }
        }
        else if (TM_DEBUG)
            Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " LazyRep, IGNORE finalize slave ...");
    }
    
    private static def getDeadAndConflictingPlaces(ex:Exception) {
        val list = new ArrayList[Place]();
        if (ex != null) {
            if (ex instanceof DeadPlaceException) {
                list.add((ex as DeadPlaceException).place);
            }
            else if (ex instanceof ConflictException) {
                list.add((ex as ConflictException).place);
            }
            else {
                val mulExp = ex as MultipleExceptions;
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