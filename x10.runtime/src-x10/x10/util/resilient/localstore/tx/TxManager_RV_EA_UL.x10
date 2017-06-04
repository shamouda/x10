package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Validation
 * Acquire: Early Acquire
 * Write: Undo Logging (must use early acquire)
 **/
public class TxManager_RV_EA_UL[K] {K haszero} extends TxManager[K]  {

    public def this(data:MapData[K], immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_RV_EA_UL");
    }
    
    public def get(id:Long, key:K):Cloneable {
        return get_RV_UL(id, key);
    }
    
    public def put(id:Long, key:K, value:Cloneable):Cloneable {
        return put_RV_EA_UL(id, key, value, false, false);
    }
    
    public def delete(id:Long, key:K, txDesc:Boolean):Cloneable {
        return put_RV_EA_UL(id, key, null, true, txDesc);
    }
    
    public def validate(log:TxLog[K]) {
        validate_RV_EA(log);
    }
    
    public def commit(log:TxLog[K]) {
        commit_UL(log);
    }
    
    public def abort(log:TxLog[K]) {
        abort_UL(log);
    }

    public def lockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
}