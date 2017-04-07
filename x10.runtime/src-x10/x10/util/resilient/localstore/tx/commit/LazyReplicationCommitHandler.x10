package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.AbstractTx;
import x10.util.resilient.localstore.TxMembers;
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.util.GrowableRail;

/*
 * In lazy replication, transaction side effects are NOT applied at the slave until a master dies. 
 * The master sends a change-log to the slave during the validation phase of the 2PC protocol.
 * The slave keeps these logs in order of arrival.
 * When the master dies, the slave contacts the coordinators of each transaction, for which it has a log, to know if that transaction was committed or aborted.
 * Because of that, each coordinator has to remember all the committed transactions.
 * The slave applies the side effects of the committed transactions to the master's data, 
 * before copying this data to the spare place that will replace the master.
 * 
 * In the 2PC, the slave is completely ignored after the validation phase.
 * 
 * Tx logging is as follows (by the coordinator only):  
 * --> committed transaction: STARTED, COMMITTING, COMMITTED   (log not removed,  GC required)
 * --> aborted transaction: STARTED, NULL (Tx log removed)
 * 
 * Known issue: we did not implement a garbage collection mechanism, 
 * so the list of committed transactions at each coordinator can grow very large and cause out-of-memory errors.
 *  
 * Should be used in resilient mode only
 * */
public class LazyReplicationCommitHandler extends CommitHandler {
	
	private val root = GlobalRef[LazyReplicationCommitHandler](this);
	private var nonFatalDeadPlace:Boolean = false;
	
	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
	    super(plh, id, mapName, members);	
	}
	
	public def abort(abortedPlaces:ArrayList[Place], recovery:Boolean) {
	    try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, members);
        }
        catch(ex:MultipleExceptions) {
            if (recovery) {
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
        }
        
	    plh().txDescManager.delete(id, true);
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(commitRecovery:Boolean):Int {
        if (!commitRecovery) {
            try {
                commitPhaseOne(plh, id, members); // master failure is fatal, slave failure is not fatal
                plh().txDescManager.updateStatus(id, TxDesc.COMMITTING, true);
            } catch(ex:Exception) {
                val list = getDeadAndConflictingPlaces(ex);
                abort(list, false);
                throw ex;
            }
        }

        commitPhaseTwo(plh, id, members, commitRecovery);
        
    	/* if we don't mark the committed transaction, 
    	 * we will have to recommit all transactions that are already committed */
        plh().txDescManager.updateStatus(id, TxDesc.COMMITTED, true);
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
    	val ownerPlaceIndex = plh().virtualPlaceId;
        if (TxConfig.get().VALIDATION_REQUIRED) {
            plh().masterStore.validate(id);
        }

    	var ex:Exception = null;
    	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"]  ...");
        val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {                            
            //send txLog to slave (very important to be able to tolerate failures of masters just after prepare)
            try {
            	finish at (plh().slave) async {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                }
            }catch(e:Exception) {
            	ex = e;
            	//ignoring dead slave
            }
            
            if (ex != null) {
                at (root) async {
                    root().nonFatalDeadPlace = true;
                }
            }
        }
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"] DONE ...");
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers, commitRecovery:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo ...");
        val start = Timer.milliTime();
        try {
            finalize(true, null, plh, id, members);
        }
        catch(ex:MultipleExceptions) {    
            if (commitRecovery) {
                try {
                    val deadMasters = getDeadPlaces(ex, members); //some masters have died after validation, ask slaves to commit
                    finalizeSlaves(true, deadMasters, plh, id, members);
                }
                catch(ex2:Exception) {
                    ex2.printStackTrace();
                    throw new Exception("FATAL ERROR: Master and Slave died together, Exception["+ex2.getMessage()+"] ");
                }
            }
        } finally {
            phase2ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");

        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
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
        if (commit)
            plh().masterStore.commit(id);
        else
            plh().masterStore.abort(id);
    }

}