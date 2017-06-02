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
    
    public def lockRead(id:Long, key:K) {
        lockRead_Locking(id, key);
    }
        
    public def lockWrite(id:Long, key:K) {
        lockWrite_Locking(id, key);
    }
    
    public def unlockRead(id:Long, key:K) {
        unlockRead_Locking(id, key);
    }
    
    public def unlockWrite(id:Long, key:K) {
        unlockWrite_Locking(id, key);
    }
    
    public def lockRead(id:Long, keys:ArrayList[K]) {
        lockRead_Locking(id, keys);
    }
    public def lockWrite(id:Long, keys:ArrayList[K]) {
        lockWrite_Locking(id, keys);
    }
    public def unlockRead(id:Long, keys:ArrayList[K]) {
        unlockRead_Locking(id, keys);
    }
    public def unlockWrite(id:Long, keys:ArrayList[K]) {
        unlockWrite_Locking(id, keys);
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