package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.AbstractTx;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.TxMembers;
import x10.util.HashSet;

public class NonResilientCommitHandler extends CommitHandler {
	
    val OPERATION_ABORT = 1;
    val OPERATION_VALIDATE = 2;
    val OPERATION_COMMIT = 3;
    
	public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
	    super(plh, id, mapName, members);
	}
	
    public def abort(abortedPlaces:ArrayList[Place], recovery:Boolean) {
        try {
            if (members != null)
                finish executeFlat(abort_local);
            else
                finish executeRecursively(abort_local, new HashSet[Long]());
        }
        catch(ex:MultipleExceptions) {
            Console.OUT.println("Warning: ignoring exception during finalize(false): " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commit(commitRecovery:Boolean):Int {
    	assert (!commitRecovery) : "fatal error, commitRecovery must always be false in NonResilientCommitHandler";
        
    	val startP1 = Timer.milliTime();
    	if (TxConfig.get().VALIDATION_REQUIRED) {
            try {
                if (members != null)
                    finish executeFlat(validate_local); // failures are fatal
                else
                    finish executeRecursively(validate_local, new HashSet[Long]());
                
            } catch(ex:Exception) {
                val list = getDeadAndConflictingPlaces(ex);
                abort(list, false);
                throw ex;
            }
    	}
    	phase1ElapsedTime = Timer.milliTime() - startP1;
    	
        val startP2 = Timer.milliTime();
        if (members != null)
            finish executeFlat(commit_local);
        else {
            finish executeRecursively(commit_local, new HashSet[Long]());
        }
        phase2ElapsedTime = Timer.milliTime() - startP2;
        
        return AbstractTx.SUCCESS;
    }
   
    
}