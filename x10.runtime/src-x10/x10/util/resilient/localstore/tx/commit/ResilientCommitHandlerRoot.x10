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
import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.compiler.Immediate;

public abstract class ResilientCommitHandlerRoot[K] {K haszero} extends CommitHandler[K] {
   
    private val root = GlobalRef[ResilientCommitHandlerRoot[K]](this);
    protected var nonFatalDeadPlace:Boolean = false;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, members:TxMembers) {
        super(plh, id, members);
    }
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        super(plh, id);
    }
    
    protected abstract def abort_resilient(recovery:Boolean):void;
    protected abstract def commit_resilient(commitRecovery:Boolean):void;
    
    public def clean() {
        (root as GlobalRef[ResilientCommitHandlerRoot[K]]{self.home == here}).forget();
    }
    
    public def abort(recovery:Boolean){
        try {
            abort_resilient(recovery);
        }
        finally {
            (root as GlobalRef[ResilientCommitHandlerRoot[K]]{self.home == here}).forget();
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
            (root as GlobalRef[ResilientCommitHandlerRoot[K]]{self.home == here}).forget();
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
            val slave = plh().slave;
            val rCond = ResilientCondition.make(slave);
            val closure = (gr:GlobalRef[Condition]) => {
                at (slave) @Immediate("cmthandlerroot_abort") async {
                    plh().slaveStore.abort(id);
                    at (gr) @Immediate("cmthandlerroot_abort_response") async {
                        gr().release();
                    }
                }
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
                ex = new DeadPlaceException(slave); //dead slave not fatal
            }
            rCond.forget();
        }
            
        plh().getMasterStore().abort(id);
    }
    
    protected def commit_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        var ex:Exception = null;
        if (!plh().getMasterStore().isReadOnlyTransaction(id)) {
            val slave = plh().slave;
            val rCond = ResilientCondition.make(slave);
            val closure = (gr:GlobalRef[Condition]) => {
                at (slave) @Immediate("cmthandlerroot_commit") async {
                    plh().slaveStore.commit(id);
                    at (gr) @Immediate("cmthandlerroot_commit_response") async {
                        gr().release();
                    }
                }
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
                ex = new DeadPlaceException(slave); //dead slave not fatal
            }
            rCond.forget();
        }
        
        plh().getMasterStore().commit(id);

    }
    
    protected def validate_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        if (TxConfig.get().VALIDATION_REQUIRED)
            plh().getMasterStore().validate(id);        
        var ex:Exception = null;
        val ownerPlaceIndex = plh().virtualPlaceId;
        val log = plh().getMasterStore().getTxCommitLog(id);
        if (log != null && log.size() > 0) {
            val slave = plh().slave;
            val rCond = ResilientCondition.make(slave);
            val closure = (gr:GlobalRef[Condition]) => {
                at (slave) @Immediate("cmthandlerroot_prepare") async {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                    at (gr) @Immediate("cmthandlerroot_prepare_response") async {
                        gr().release();
                    }
                }
            };
            
            rCond.run(closure);
            
            if (rCond.failed()) {
                ex = new DeadPlaceException(slave); //dead slave not fatal
            }
            rCond.forget();
        }
    }
}