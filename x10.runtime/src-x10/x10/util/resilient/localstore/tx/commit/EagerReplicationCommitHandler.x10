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
import x10.compiler.Pinned;
import x10.util.resilient.localstore.tx.logging.TxDescManager;
import x10.compiler.Uncounted;

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
public class EagerReplicationCommitHandler[K] {K haszero} extends ResilientCommitHandler[K] {
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, mapName:String, members:TxMembers) {
        super(plh, id, mapName, members);
    }
    
    private def asyncDeleteDescriptors(recovery:Boolean, masters:ArrayList[Place]) {
        val deleteDesc_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { plh().txDescManager.delete(id, true); } ;
        val deleteDesc_slave = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { plh().txDescManager.deleteTxDescFromSlaveStore(id); } ;
    
        @Uncounted async {
            try {
                if (recovery) { //delete at slave
                    plh().txDescManager.deleteTxDescFromSlaveStore(id);
                }
                executeRecursivelyResilient(deleteDesc_master, deleteDesc_slave, masters);
            }catch(ex:Exception) {
                //ignore exceptions while delete
            }
        }
    }
    
    /*
     * recovery = false (default) master is alive
     * recovery = true means that the slave is acting as master to abort a transaction that the master started before it died
     **/
    public def abort_resilient(recovery:Boolean){
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " abort_resilient started ...");
        val abort_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { abort_local_resilient(plh, id); } ;
        val abort_slave = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { plh().slaveStore.abort(id); } ;
        if (members != null) {
            try { 
                finish executeFlat(abort_master, true);
            }
            catch(ex:MultipleExceptions) {
                val deadMasters = getDeadPlaces(ex);
                executeFlatSlaves(deadMasters, abort_slave, true);
            }
        }
        else {
            val masters = new ArrayList[Place]();
            if (!recovery) {
                masters.add(here);
            }
            else {
                abort_slave(plh, id);
                val childrenVirtual = plh().txDescManager.getVirtualMembers(id, TxDescManager.FROM_SLAVE);
                if (childrenVirtual != null) {
                    val places = plh().getTxMembers( childrenVirtual , true).places;
                                        
                    for (var i:Long = 0; i<childrenVirtual.size; i++) {
                        if (plh().getSlave(childrenVirtual(i)).id != here.id) {
                            masters.add(places(i));
                        }
                        else {
                            Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " Skipping place ["+childrenVirtual(i)+"] because he is my dead master ");
                        }
                    }
                }
            }
            
            if (masters.size() > 0) {
                executeRecursivelyResilient(abort_master, abort_slave, masters);
                asyncDeleteDescriptors(recovery, masters);
            }
        }
        
    }
    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit_resilient(commitRecovery:Boolean) {
        if (!commitRecovery) 
            commitPhaseOne();
        
        commitPhaseTwo(commitRecovery);
    }
   
    private def commitPhaseOne() {
        val validate_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { validate_local_resilient(plh, id); };
        val validate_slave = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => {  };
        val startP1 = Timer.milliTime();
        try {
            if (members != null)
                finish executeFlat(validate_master, false);
            else {
                val masters = new ArrayList[Place]();
                masters.add(here);
                executeRecursivelyResilient(validate_master, validate_slave, masters);  
            }
            plh().txDescManager.updateStatus(id, TxDesc.COMMITTING, true);
        } catch(ex:Exception) {
            abort(false);
            throw ex;
        }
        finally {
            phase1ElapsedTime = Timer.milliTime() - startP1;
        }
    }
    
    private def commitPhaseTwo(recovery:Boolean) {
        val commit_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { commit_local_resilient(plh, id); } ;
        val commit_slave = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { plh().slaveStore.commit(id); } ;
        val startP2 = Timer.milliTime();
        if (members != null) {
            try {
                finish executeFlat(commit_master, true);
            }
            catch(ex:MultipleExceptions) {
                try {
                    val deadMasters = getDeadPlaces(ex);
                    finish executeFlatSlaves(deadMasters, commit_slave, true);
                }
                catch(ex2:Exception) {
                    if (TxConfig.get().TM_DEBUG) {
                        Console.OUT.println("Warning: ignoring exception during finalizeSlaves(false): " + ex2.getMessage());
                        ex2.printStackTrace();
                    }
                }
            }
        }
        else {
            val masters = new ArrayList[Place]();
            if (!recovery) {
                masters.add(here);
            }
            else {
                commit_slave(plh, id);
                val childrenVirtual = plh().txDescManager.getVirtualMembers(id, TxDescManager.FROM_SLAVE);
                if (childrenVirtual != null) {
                    val places = plh().getTxMembers( childrenVirtual , true).places;
                                        
                    for (var i:Long = 0; i<childrenVirtual.size; i++) {
                        if (plh().getSlave(childrenVirtual(i)).id != here.id) {
                            masters.add(places(i));
                        }
                        else {
                            Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " Skipping place ["+childrenVirtual(i)+"] because he is my dead master ");
                        }
                    }
                }
            }
            if (masters.size() > 0) {
                executeRecursivelyResilient(commit_master, commit_slave, masters);
                asyncDeleteDescriptors(recovery, masters);
            }
        }
        phase2ElapsedTime = Timer.milliTime() - startP2;
        
    }

}