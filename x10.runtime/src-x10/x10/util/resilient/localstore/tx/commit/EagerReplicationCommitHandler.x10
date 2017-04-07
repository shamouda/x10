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
import x10.util.HashSet;

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
    
	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
	    super(plh, id, mapName, members);	
	}
	
	public def abort(recovery:Boolean) {
	    val abort_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { abort_local(plh, id); } ;
        try {
            if (members != null)
                finish executeFlat(abort_master, true);
            else
                finish executeRecursively(abort_master, new HashSet[Long](), true);
        }
        catch(ex:MultipleExceptions) {
            try {
                val abort_slave = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { plh().slaveStore.abort(id); } ;
                val deadMasters = getDeadPlaces(ex, members);
                finish executeFlatSlaves(deadMasters, abort_slave, true);
            }
            catch(ex2:Exception) {
                if (TxConfig.get().TM_DEBUG) {
                    Console.OUT.println("Warning: ignoring exception during finalizeSlaves(false): " + ex2.getMessage());
                    ex2.printStackTrace();
                }
            }
        }
    }
	
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(commitRecovery:Boolean):Int {
        commitPhaseOne(commitRecovery);
        
        plh().txDescManager.updateStatus(id, TxDesc.COMMITTING, true);
        
        commitPhaseTwo();
        
        plh().txDescManager.delete(id, true);

        if (nonFatalDeadPlace)
        	return AbstractTx.SUCCESS_RECOVER_STORE;
        else
        	return AbstractTx.SUCCESS;
    }
   
    private def commitPhaseOne(commitRecovery:Boolean) {
        if (!commitRecovery) {
            val validate_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { validate_local(plh, id); };
            val startP1 = Timer.milliTime();
            try {
                if (members != null)
                    finish executeFlat(validate_master, false);
                else
                    finish executeRecursively(validate_master, new HashSet[Long](), false);
            } catch(ex:Exception) {
                abort(false);
                throw ex;
            }
            finally {
                phase1ElapsedTime = Timer.milliTime() - startP1;
            }
        }
    }
    
    private def commitPhaseTwo() {
        val commit_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { commit_local(plh, id); } ;
        
        val startP2 = Timer.milliTime();
        try {
            if (members != null)
                finish executeFlat(commit_master, true);
            else
                finish executeRecursively(commit_master, new HashSet[Long](), true);
        }
        catch(ex:MultipleExceptions) {
            try {
                val commit_slave = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { plh().slaveStore.commit(id); } ;
                val deadMasters = getDeadPlaces(ex, members);
                finish executeFlatSlaves(deadMasters, commit_slave, true);
            }
            catch(ex2:Exception) {
                if (TxConfig.get().TM_DEBUG) {
                    Console.OUT.println("Warning: ignoring exception during finalizeSlaves(false): " + ex2.getMessage());
                    ex2.printStackTrace();
                }
            }
        }
        phase2ElapsedTime = Timer.milliTime() - startP2;
    }

    private def abort_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        var ex:Exception = null;
        val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            try {
                at (plh().slave) {
                    plh().slaveStore.abort(id);
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
            
        plh().masterStore.abort(id);
        
        if (ex != null) {
            at (root) async {
                root().nonFatalDeadPlace = true;
            }
        }
    }
    
    private def commit_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        var ex:Exception = null;
        val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            try {
                at (plh().slave)  {
                    plh().slaveStore.commit(id);
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
        
        plh().masterStore.commit(id);
        
        if (ex != null) {
            at (root) async {
                root().nonFatalDeadPlace = true;
            }
        }
    }
    
    private def validate_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        if (TxConfig.get().VALIDATION_REQUIRED)
            plh().masterStore.validate(id);
        
        var ex:Exception = null;
        val ownerPlaceIndex = plh().virtualPlaceId;
        val log = plh().masterStore.getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            try {
                async at (plh().slave) {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                }
            }catch(e:Exception) {
                ex = e;
            }
            
            if (ex != null) {
                at (root) async {
                    root().nonFatalDeadPlace = true;
                }
            }
        }
    }
    
}