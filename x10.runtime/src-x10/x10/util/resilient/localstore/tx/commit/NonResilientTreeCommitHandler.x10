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

public class NonResilientTreeCommitHandler[K] {K haszero} extends CommitHandler[K] {
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        super(plh, id);
    }
    
    public def abort(recovery:Boolean) {
        val abort_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { abort_local(plh, id); } ;
        finish executeRecursively(abort_master, new HashSet[Long](), true);
    }
    
    public def commit(commitRecovery:Boolean):Int {
        commitPhaseOne();
        if (TxConfig.EXPR_LVL == 3)
            return AbstractTx.SUCCESS;
        commitPhaseTwo();
        return AbstractTx.SUCCESS;
    }
   
    private def commitPhaseOne() {
        if (TxConfig.get().VALIDATION_REQUIRED) {
        	val startP1 = Timer.milliTime();
        	val validate_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { validate_local(plh, id); };
            try {
            	finish executeRecursively(validate_master, new HashSet[Long](), false);
            } catch(ex:Exception) {
                abort(false);
                throw ex;
            } finally {
                phase1ElapsedTime = Timer.milliTime() - startP1;
            }
        }
    }
    
    private def commitPhaseTwo() {
        val commit_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { commit_local(plh, id); } ;
        val startP2 = Timer.milliTime();
        finish executeRecursively(commit_master, new HashSet[Long](), true);
        phase2ElapsedTime = Timer.milliTime() - startP2;
    }
}