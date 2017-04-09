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
public class LazyReplicationCommitHandler extends ResilientCommitHandler {
	
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
        super(plh, id, mapName, members);   
    }
    
    public def abort_resilient(recovery:Boolean){
        val abort_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { abort_local_resilient(plh, id); } ;
        try {
            if (members != null)
                finish executeFlat(abort_master, true);
            else
                finish executeRecursively(abort_master, new HashSet[Long](), true);
        }
        catch(ex:MultipleExceptions) {
            
        }
    }
    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit_resilient(commitRecovery:Boolean) {
        commitPhaseOne(commitRecovery);
        
        plh().txDescManager.updateStatus(id, TxDesc.COMMITTING, true);
        
        commitPhaseTwo();
        
        plh().txDescManager.updateStatus(id, TxDesc.COMMITTED, true);
    }
   
    private def commitPhaseOne(commitRecovery:Boolean) {
        if (!commitRecovery) {
            val validate_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { validate_local_resilient(plh, id); };
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
        val commit_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { commit_local_resilient(plh, id); } ;
        
        val startP2 = Timer.milliTime();
        try {
            if (members != null)
                finish executeFlat(commit_master, true);
            else
                finish executeRecursively(commit_master, new HashSet[Long](), true);
        }
        catch(ex:MultipleExceptions) {
            
        }
        phase2ElapsedTime = Timer.milliTime() - startP2;
    }

}