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

public abstract class ResilientCommitHandler[K] {K haszero} extends CommitHandler[K] {
   
    private val root = GlobalRef[ResilientCommitHandler[K]](this);
    protected var nonFatalDeadPlace:Boolean = false;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, mapName:String, members:TxMembers) {
        super(plh, id, mapName, members);
    }
    
    protected abstract def abort_resilient(recovery:Boolean):void;
    protected abstract def commit_resilient(commitRecovery:Boolean):void;
    
    public def abort(recovery:Boolean){
        try {
            abort_resilient(recovery);
        }
        finally {
            (root as GlobalRef[ResilientCommitHandler[K]]{self.home == here}).forget();
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
            (root as GlobalRef[ResilientCommitHandler[K]]{self.home == here}).forget();
        }
    }
    
    protected def executeFlatSlaves(deadMasters:ArrayList[Place], closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, deleteTxDesc:Boolean) {
        for (p in deadMasters) {
            val slave = plh().getSlave(p);
            async at (slave) {
                closure(plh, id);
            }
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
    }

    protected def getDeadPlaces(mulExp:MultipleExceptions) {
        
        val list = new ArrayList[Place]();
        var str:String = "";
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                list.add(dpe.place);
                str += " " + dpe.place;
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " getDeadPlaces result {"+str+"} ...");
        return list;
    }
    
    protected def executeRecursivelyResilient(master_closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void,
            slave_closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, places:ArrayList[Place]) {
        var str:String = "";
        for (x in places) {
            str += x + " , " ;
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient started, places are ["+str+"]  ...");
        var completed:Boolean = false;
        var masterType:Boolean = true; 
        val parents = new HashSet[Long]();
        var ex:Exception = null;
        while (!completed) {
            try {
                val masterVal = masterType;
                finish for (master in places) {
                    val rootPlace:Place;
                    if (!masterVal) {
                        rootPlace = plh().getSlave(master);
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient NEW PHASE with slave "+rootPlace+" ...");
                    }
                    else {
                        rootPlace = master;
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient PHASE with master "+rootPlace+" ...");
                    }
                    
                    at (rootPlace) async {
                        if (masterVal) {
                            master_closure(plh, id);
                            parents.add(plh().getVirtualPlaceId());
                        }
                        else {
                            slave_closure(plh, id);
                            parents.add(plh().getPreviousVirtualPlaceId());
                        }
                        
                        val childrenVirtual = plh().txDescManager.getVirtualMembers(id, masterVal);
                        if (childrenVirtual != null) {
                            val physical = plh().getTxMembers( childrenVirtual , true).places;
                            for (var i:Long = 0; i < childrenVirtual.size; i++) {
                                val p = physical(i);
                                if (!parents.contains(childrenVirtual(i))) {
                                    at (p) async {
                                        executeRecursively(master_closure, parents, false);
                                    }
                                }
                                else
                                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient ignoring ["+p+"] already a parent ...");
                                
                            }
                        }
                        else {
                            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " NO_CHILDREN ...");
                        }
                    }
                }
                completed = true;
            } catch(dpe:DeadPlaceException) {
                if (TxConfig.get().TM_DEBUG) {
                    Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient phase finished with error ...");
                    dpe.printStackTrace();
                }
                places.clear();
                places.add(dpe.place);
                masterType = false;
                ex = dpe;
            } catch(mulExp:MultipleExceptions) {
                if (TxConfig.get().TM_DEBUG) {
                    Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient phase finished with error ...");
                    mulExp.printStackTrace();
                }
                places.clear();
                places.addAll(getDeadPlaces(mulExp));
                masterType = false;
                ex = mulExp;
            }
        }
        if (ex != null)
            throw ex;
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursivelyResilient ended ...");
    }    
 
    protected def abort_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        var ex:Exception = null;
        if (!plh().getMasterStore().isReadOnlyTransaction(id)) {
            try {
                at (plh().slave) {
                    plh().slaveStore.abort(id);
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
            
        plh().getMasterStore().abort(id);
        
        if (ex != null) {
            at (root) async {
                root().nonFatalDeadPlace = true;
            }
        }
    }
    
    protected def commit_local_resilient(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        var ex:Exception = null;
        if (!plh().getMasterStore().isReadOnlyTransaction(id)) {
            try {
                at (plh().slave)  {
                    plh().slaveStore.commit(id);
                }
            }catch (e:Exception) {
                ex = e; //dead slave not fatal
            }
        }
        
        plh().getMasterStore().commit(id);
        
        if (ex != null) {
            at (root) async {
                root().nonFatalDeadPlace = true;
            }
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
                at (plh().slave) {
                    plh().slaveStore.prepare(id, log, ownerPlaceIndex);
                }
            }catch(e:Exception) {
                ex = e;
            }
            
            if (ex != null) {
                at (root) async {
                    root().nonFatalDeadPlace = true;
                }
            }
        }
    }
}