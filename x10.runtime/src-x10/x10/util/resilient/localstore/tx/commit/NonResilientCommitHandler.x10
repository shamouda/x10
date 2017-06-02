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

public class NonResilientCommitHandler[K] {K haszero} extends CommitHandler[K] {
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, mapName:String, members:TxMembers) {
        super(plh, id, mapName, members);
    }
    
    public def abort(recovery:Boolean) {
        val abort_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { abort_local(plh, id); } ;
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseOne() started ...");
        val validate_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { validate_local(plh, id); };
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
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseOne() completed ...");
    }
    
    private def commitPhaseTwo() {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseTwo() started ...");
        val commit_master = (plh:PlaceLocalHandle[LocalStore[K]], id:Long ):void => { commit_local(plh, id); } ;
        val startP2 = Timer.milliTime();
        if (members != null)
            finish executeFlat(commit_master, true);
        else
            finish executeRecursively(commit_master, new HashSet[Long](), true);
        phase2ElapsedTime = Timer.milliTime() - startP2;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] here["+here+"] commitPhaseTwo() completed ...");
    }
    
    private def validate_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().validate(id);
    }
    
    private def commit_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().commit(id);
    }
    
    private def abort_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().abort(id);
    }
}