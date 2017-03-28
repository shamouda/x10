package x10.util.resilient.localstore.tx;

import x10.util.Timer;
public class LazyReplicationCommitHandler extends CommitHandler {
    public def abort(abortedPlaces:ArrayList[Place]) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " abort (abortPlaces.size = " + abortedPlaces.size() + ") alreadyAborted = " + aborted);
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces, plh, id, members, root);
        }
        catch(ex:MultipleExceptions) {
            Console.OUT.println("Warning: ignoring exception during finalize(false): " + ex.getMessage());
            ex.printStackTrace();
        }
        
        if (resilient && !DISABLE_DESC)
            deleteTxDesc();
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(skipPhaseOne:Boolean) {
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
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo time [" + (endP2-startP2) + "] ms");
        
        
        if (resilient && !DISABLE_DESC ){
        	/* if we don't mark the committed transaction, 
        	 * we will have to recommit all transactions that are already committed */
            updateTxDesc(TxDesc.COMMITTED); 
        }

        commitTime = Timer.milliTime();
        
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " committed, allTxTime [" + (commitTime-startTime) + "] ms");
    }

    private def commitPhaseOne(plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        val start = Timer.milliTime();
        try {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne ...");
            if ((resilient && !DISABLE_SLAVE) || TxConfig.getInstance().VALIDATION_REQUIRED) {
            	finish for (p in members) {
                	if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseOne going to move to ["+p+"] ...");
                    at (p) async {
                    	commitPhaseOne_local(plh, id);
                    }
            	}
            }
            else
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate NOT required ...");
        } finally {
            phase1ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    private def commitPhaseOne_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate started ...");
            plh().masterStore.validate(id);
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] commitPhaseOne : validate done ...");
        }
    
        if (resilient && !DISABLE_SLAVE) {
        	if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"]  ...");
            val log = plh().masterStore.getTxCommitLog(id);
            if (log != null && log.size() > 0) {                            
                //send txLog to slave (very important to be able to tolerate failures of masters just after prepare)
                try {
	            	finish at (plh().slave) async {
	                    plh().slaveStore.prepare(id, log);
	                }
                }catch(ex:Exception) {
                	//ignoring dead slave
                }
            }
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] send log to slave ["+plh().slave+"] DONE ...");
        }
    }
    
    private def commitPhaseTwo(plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        if (TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " commitPhaseTwo ...");
        val start = Timer.milliTime();
        try {
            //ask masters and slaves to commit
            finalize(true, null, plh, id, members, root);
        }
        catch(ex:MultipleExceptions) {
            if (TM_DEBUG) {
                Console.OUT.println(here + "commitPhaseTwo Exception[" + ex.getMessage() + "]");
                ex.printStackTrace();
            }
            
            if (!resilient) {
                throw ex;
            }
        } finally {
            phase2ElapsedTime = Timer.milliTime() - start;
        }
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:PlaceGroup, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");
        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
        finish for (p in members) {
            /*skip aborted places*/
            if (!commit && abortedPlaces.contains(p))
                continue;
        	at (p) async {
        		finalizeLocal(commit, plh, id, root);
        	}
        }
    }
    
    private def finalizeLocal(commit:Boolean, plh:PlaceLocalHandle[LocalStore], id:Long, root:GlobalRef[Tx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeLocal  here["+here+"] " + ( commit? " Commit Local Started ": " Abort Local Started " ) + " ...");
        if (commit)
            plh().masterStore.commit(id);
        else
            plh().masterStore.abort(id);
    }

}