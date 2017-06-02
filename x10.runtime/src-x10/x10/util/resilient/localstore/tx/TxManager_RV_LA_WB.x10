package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Validation
 * Acquire: Late Acquire
 * Write: Write Buffering
 **/
public class TxManager_RV_LA_WB[K] {K haszero} extends TxManager[K]  {
    
    public def this(data:MapData[K], immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_RV_LA_WB");
    }
    
    public def get(id:Long, key:K):Cloneable {
        return get_RV_WB(id, key);
    }
    
    public def put(id:Long, key:K, value:Cloneable):Cloneable {
        return put_LA_WB(id, key, value, false, false);
    }
    
    public def delete(id:Long, key:K, txDesc:Boolean):Cloneable {
        return put_LA_WB(id, key, null, true, txDesc);
    }
    
    public def validate(log:TxLog[K]) {
        validate_RV_LA_WB(log);
    }
    
    public def commit(log:TxLog[K]) {
        commit_WB(log);
    }
    
    public def abort(log:TxLog[K]) {
        abort_WB(log);
    }
    
    public def lockRead(id:Long, key:K) {
        throw new Exception("lockRead not supported");
    }
    public def lockWrite(id:Long, key:K) {
        throw new Exception("lockWrite not supported for lock based tx manager");
    }
    public def unlockRead(id:Long, key:K) {
        throw new Exception("unlockRead not supported for lock based tx manager");
    }
    public def unlockWrite(id:Long, key:K) {
        throw new Exception("unlockWrite not supported for lock based tx manager");
    }
    public def lockRead(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported");
    }
    public def lockWrite(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported");
    }
    public def unlockRead(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported");
    }
    public def unlockWrite(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported");
    }
}