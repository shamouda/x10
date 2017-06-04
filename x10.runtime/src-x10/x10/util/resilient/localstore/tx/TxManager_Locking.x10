package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

public class TxManager_Locking[K] {K haszero} extends TxManager[K]  {

    public def this(data:MapData[K], immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_Locking");
    }
    
    public def get(id:Long, key:K):Cloneable {
        return getLocked(id, key);
    }
    
    public def put(id:Long, key:K, value:Cloneable):Cloneable {
        return putLocked(id, key, value);
    }
    
    public def delete(id:Long, key:K, txDesc:Boolean):Cloneable {
        throw new Exception("operation not supported for lock based tx manager");
    }
    
    
    public def lockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        super.lock(id, start, opPerPlace, keys, readFlags);
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        super.unlock(id, start, opPerPlace, keys, readFlags);
    }
    
    public def commit(log:TxLog[K]) {
        throw new Exception("operation not supported for lock based tx manager");
    }
    
    public def abort(log:TxLog[K]) {
        throw new Exception("operation not supported for lock based tx manager");
    }

    public def validate(log:TxLog[K]) {
        throw new Exception("operation not supported for lock based tx manager");
    }
}