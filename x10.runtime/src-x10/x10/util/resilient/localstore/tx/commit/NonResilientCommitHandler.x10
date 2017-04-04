package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.AbstractTx;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.TxMembers;

public class NonResilientCommitHandler extends CommitHandler {
	
	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers, txDescMap:ResilientNativeMap) {
	    super(plh, id, mapName, members, txDescMap);
	}
	
    public def abort(abortedPlaces:ArrayList[Place], recovery:Boolean) {
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, members);
        }
        catch(ex:MultipleExceptions) {
            Console.OUT.println("Warning: ignoring exception during finalize(false): " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(skipPhaseOne:Boolean):Int {
    	assert (!skipPhaseOne) : "fatal error, skipPhaseOne must always be false in NonResilientCommitHandler";
        
        try {
        	commitPhaseOne(plh, id, members); // failures are fatal
            
        } catch(ex:Exception) {
            val list = getDeadAndConflictingPlaces(ex);
            abort(list, false);
            throw ex;
        }
        
        val startP2 = Timer.milliTime();
        commitPhaseTwo(plh, id, members);
        val endP2 = Timer.milliTime();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo time [" + (endP2-startP2) + "] ms");
        return AbstractTx.SUCCESS;
    }
   
    private def commitPhaseOne(plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        val start = Timer.milliTime();
        try {
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne ...");
            if (TxConfig.get().VALIDATION_REQUIRED) {
	            finish for (p in members.pg()) {
                	if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne going to move to ["+p+"] ...");
                    at (p) async {
                    	commitPhaseOne_local(plh, id);
                    }
	            }
            }
            else
                if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate NOT required ...");
        } finally {
            phase1ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    private def commitPhaseOne_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        if (TxConfig.get().VALIDATION_REQUIRED) {
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate started ...");
            plh().masterStore.validate(id);
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate done ...");
        }
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo ...");
        val start = Timer.milliTime();
        try {
            //ask masters and slaves to commit
            finalize(true, null, plh, id, members);
        }
        catch(ex:MultipleExceptions) {
            if (TxConfig.get().TM_DEBUG) {
                Console.OUT.println(here + "commitPhaseTwo Exception[" + ex.getMessage() + "]");
                ex.printStackTrace();
            }
            throw ex;
        } finally {
            phase2ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");
        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
        finish for (p in members.pg()) {
            /*skip aborted places*/
            if (!commit && abortedPlaces.contains(p))
                continue;
        	at (p) async {
        		finalizeLocal(commit, plh, id);
        	}
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