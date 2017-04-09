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
            Console.OUT.println("Warning: ignoring exception during finalize(false): " + ex.getMessage());
            ex.printStackTrace();
        }
    }
    
    public def commit(commitRecovery:Boolean):Int {
    	commitPhaseOne();

    	commitPhaseTwo();
        
    	return AbstractTx.SUCCESS;
    }
   
    private def commitPhaseOne() {
        val validate_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { validate_local(plh, id); };
        val startP1 = Timer.milliTime();
        if (TxConfig.get().VALIDATION_REQUIRED) {
            try {
                if (members != null)
                    finish executeFlat(validate_master, false); // failures are fatal
                else
                    finish executeRecursively(validate_master, new HashSet[Long](), false);
                
            } catch(ex:Exception) {
                abort(false);
                throw ex;
            }
        }
        phase1ElapsedTime = Timer.milliTime() - startP1;
    }
    
    private def commitPhaseTwo() {
        val commit_master = (plh:PlaceLocalHandle[LocalStore], id:Long ):void => { commit_local(plh, id); } ;
        val startP2 = Timer.milliTime();
        if (members != null)
            finish executeFlat(commit_master, true);
        else
            finish executeRecursively(commit_master, new HashSet[Long](), true);
        phase2ElapsedTime = Timer.milliTime() - startP2;
    }
    
    private def validate_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        plh().masterStore.validate(id);
    }
    
    private def commit_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        plh().masterStore.commit(id);
    }
    
    private def abort_local(plh:PlaceLocalHandle[LocalStore], id:Long) {
        plh().masterStore.abort(id);
    }
}