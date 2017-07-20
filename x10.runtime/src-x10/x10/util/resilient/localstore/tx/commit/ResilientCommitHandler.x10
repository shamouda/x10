package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.TxMembers;
import x10.util.HashSet;
import x10.compiler.Pinned;
import x10.util.resilient.localstore.AbstractTx;
import x10.xrx.Runtime;

public abstract class ResilientCommitHandler[K] {K haszero} extends CommitHandler[K] {
   
    //private val root = GlobalRef[ResilientCommitHandler[K]](this);
    protected var nonFatalDeadPlace:Boolean = false;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, members:TxMembers) {
        super(plh, id, members);
    }
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        super(plh, id);
    }
    
    protected abstract def abort_resilient(recovery:Boolean):void;
    protected abstract def commit_resilient(commitRecovery:Boolean):void;
    
    public def abort(recovery:Boolean){
        try {
            abort_resilient(recovery);
        }
        finally {
            //(root as GlobalRef[ResilientCommitHandler[K]]{self.home == here}).forget();
        }
    }
    
    public def commit(commitRecovery:Boolean):Int {
        try {
            commit_resilient(commitRecovery);
            
            if (nonFatalDeadPlace)
                return AbstractTx.SUCCESS_RECOVER_STORE;
            else
                return AbstractTx.SUCCESS;
        }
        finally {
            //(root as GlobalRef[ResilientCommitHandler[K]]{self.home == here}).forget();
        }
    }
    
    protected def getDeadPlaces(mulExp:MultipleExceptions) {
        val list = new ArrayList[Place]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                list.add(dpe.place);
            }
        }
        return list;
    }
    
    protected def getDeadPlaces2(mulExp:MultipleExceptions) {
        val list = new ArrayList[Long]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                list.add(dpe.place.id);
            }
        }
        return list;
    }
    
    protected def abort_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        var ex:Exception = null;
        if (!plh().getMasterStore().isReadOnlyTransaction(id)) {
            try {
                if (TxConfig.IMM_AT) {
                    Runtime.runImmediateAt(plh().slave, ()=>{
                        plh().slaveStore.abort(id);
                    });
                }
                else {
                    at (plh().slave) {
                        plh().slaveStore.abort(id);
                    }
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
            
        plh().getMasterStore().abort(id);
        
        if (ex != null) {
            /*at (root) async {
                root().nonFatalDeadPlace = true;
            }
            */
        }
    }
    
    protected def commit_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        var ex:Exception = null;
        if (!plh().getMasterStore().isReadOnlyTransaction(id)) {
            try {
                if (TxConfig.IMM_AT) {
                    Runtime.runImmediateAt(plh().slave, ()=>{
                        plh().slaveStore.commit(id);
                    });
                }
                else {
                    at (plh().slave)  {
                        plh().slaveStore.commit(id);
                    }
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
        
        plh().getMasterStore().commit(id);
        
        if (ex != null) {
            /*at (root) async {
                root().nonFatalDeadPlace = true;
            }*/
        }
    }
    
    protected def validate_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        if (TxConfig.get().VALIDATION_REQUIRED)
            plh().getMasterStore().validate(id);        
        var ex:Exception = null;
        val ownerPlaceIndex = plh().virtualPlaceId;
        val log = plh().getMasterStore().getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            try {
                if (TxConfig.IMM_AT) {
                    Runtime.runImmediateAt(plh().slave, ()=>{
                        plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                    });
                }
                else {
                    at (plh().slave) {
                        plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                    }
                }
            }catch(e:Exception) {
                ex = e;
            }
            
            if (ex != null) {
                /*at (root) async {
                    root().nonFatalDeadPlace = true;
                }*/
            }
        }
    }
}