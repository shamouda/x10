package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.AbstractTx;
import x10.util.resilient.localstore.TxMembers;
import x10.util.GrowableRail;

/*
 * In eager replication, transaction side effects are applied at the slave immediately by the master.
 * If the master is dead, the transaction coordinator applies the changes at the slave, on behalf of the master.
 * The coordinator does not need to remember the committed transactions.
 * 
 * Tx logging is as follows (by the coordinator only):  
 * -->  committed transaction: STARTED, COMMITTING, NULL (Tx log removed)
 * -->  aborted transaction: STARTED, NULL (Tx log removed)
 * 
 * Should be used in resilient mode only.
 * */
public class EagerReplicationCommitHandler extends CommitHandler {
	
	private val root = GlobalRef[EagerReplicationCommitHandler](this);
	private var nonFatalDeadPlace:Boolean = false;
	
	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers, txDescMap:ResilientNativeMap) {
	    super(plh, id, mapName, members, txDescMap);	
	}
	
	public def abort(abortedPlaces:ArrayList[Place]) {
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, members);
        }
        catch(ex:MultipleExceptions) {
            try {
                val deadMasters = getDeadPlaces(ex, members);
                //some masters died while rolling back,ask slaves to abort
                finalizeSlaves(false, deadMasters, plh, id, members);
            }
            catch(ex2:Exception) {
                if (TxConfig.get().TM_DEBUG) {
                    Console.OUT.println("Warning: ignoring exception during finalizeSlaves(false): " + ex2.getMessage());
                    ex2.printStackTrace();
                }
            }
        }
        
        
        deleteTxDesc();
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(skipPhaseOne:Boolean):Int {
        if (!skipPhaseOne) {
            try {
            	commitPhaseOne(plh, id, members); // master failure is fatal, slave failure is not fatal
                updateTxDesc(TxDesc.COMMITTING);
                
            } catch(ex:Exception) {
                val list = getDeadAndConflictingPlaces(ex);
                abort(list);
                throw ex;
            }
        }
        
        commitPhaseTwo(plh, id, members);
        
        deleteTxDesc();

        if (nonFatalDeadPlace)
        	return AbstractTx.SUCCESS_RECOVER_STORE;
        else
        	return AbstractTx.SUCCESS;
    
    }
   
    private def commitPhaseOne(plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        val start = Timer.milliTime();
        try {
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne ...");
        	finish for (p in members.pg()) {
                at (p) async {
                	commitPhaseOne_local(plh, id);
                }
        	}
        } finally {
            phase1ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    private def commitPhaseOne_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        if (TxConfig.get().VALIDATION_REQUIRED) {
            plh().masterStore.validate(id);
        }
    	
    	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"]  ...");
    	var ex:Exception = null;
    	val ownerPlaceIndex = plh().virtualPlaceId;
    	val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            //send txLog to slave (very important to be able to tolerate failures of masters just after prepare)
            try {
            	finish at (plh().slave) async {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                }
            	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"] DONE ...");
            }catch(e:Exception) {
            	ex = e;
            	//ignoring dead slave: if we ignore it later during commit, why not ignore it during validation!!
            }
            
            if (ex != null) {
                at (root) async {
                    root().nonFatalDeadPlace = true;
                }
            }
        }
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo ...");
        val start = Timer.milliTime();
        try {
            finalize(true, null, plh, id, members);
        }
        catch(ex:MultipleExceptions) {            
            try {
                val deadMasters = getDeadPlaces(ex, members); //some masters have died after validation, ask slaves to commit
                finalizeSlaves(true, deadMasters, plh, id, members);
            }
            catch(ex2:Exception) {
                ex2.printStackTrace();
                throw new Exception("FATAL ERROR: Master and Slave died together, Exception["+ex2.getMessage()+"] ");
            }
        } finally {
            phase2ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");

        //if one of the masters die, let the exception be thrown to the caller, but hide dying slaves
        val deadMasters = new ArrayList[Place]();
        finish for (p in members.pg()) {
            if (!commit && abortedPlaces.contains(p) && !p.isDead()) /*skip aborted places*/
                continue;
            
            if (p.isDead()) {
                deadMasters.add(p);
            }
            else {
                at (p) async {
                    finalizeLocal(commit, plh, id);
                }
            }
        }
        
        if (deadMasters.size() > 0) {
            val rail = new GrowableRail[CheckedThrowable](deadMasters.size());
            for (var i:Long = 0 ; i < deadMasters.size() ; i ++){
                rail(i) = new DeadPlaceException(deadMasters.get(i));
            }
            throw new MultipleExceptions(rail);
        }
    }
    
    private def finalizeLocal(commit:Boolean, plh:PlaceLocalHandle[LocalStore], id:Long) {
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal  here["+here+"] " + ( commit? " Commit Local Started ": " Abort Local Started " ) + " ...");
        var ex:Exception = null;
        
        val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            //ask slave to commit, slave's death is not fatal
            try {
                if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal here["+here+"] moving to slave["+plh().slave+"] to " + ( commit? "commit": "abort" ));
                finish at (plh().slave) async {
                    if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                    if (commit)
                        plh().slaveStore.commit(id);
                    else
                        plh().slaveStore.abort(id);
                }
            }catch (e:Exception) {
            	ex = e;
                //ignore dead slave
            }
        }
            
        if (commit)
            plh().masterStore.commit(id);
        else
            plh().masterStore.abort(id);
        
        if (ex != null) {
            at (root) async {
                root().nonFatalDeadPlace = true;
            }
        }
    }
    
    
    private def finalizeSlaves(commit:Boolean, deadMasters:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        
        //ask slaves to commit (their master died, 
        //and we need to resolve the slave's pending transactions)
        finish for (p in deadMasters) {
            assert(members.contains(p));
            val virtualPlace = members.getVirtualPlaceId(p);
            val slave = plh().getSlave(virtualPlace);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moving to slave["+slave+"] to " + ( commit? "commit": "abort" ));
            at (slave) async {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                if (commit)
                    plh().slaveStore.commit(id);
                else
                    plh().slaveStore.abort(id);
            }
        }
    }

}