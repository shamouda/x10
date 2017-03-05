package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;

public class TxManager_LockBased extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_LockBased");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return getLocked(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return putLocked(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return putLocked(id, key, null);
    }
    
    public def lockRead(id:Long, key:String) {
        lockRead_Locking(id, key);
    }
        
    public def lockWrite(id:Long, key:String) {
        lockWrite_Locking(id, key);
    }
    
    public def unlockRead(id:Long, key:String) {
        unlockRead_Locking(id, key);
    }
    
    public def unlockWrite(id:Long, key:String) {
        unlockWrite_Locking(id, key);
    }
    
    public def commit(log:TxLog) {
        throw new Exception("operation not supported for lock based tx manager");
    }
    
    public def abort(log:TxLog) {
        throw new Exception("operation not supported for lock based tx manager");
    }

    public def validate(log:TxLog) {
        throw new Exception("operation not supported for lock based tx manager");
    }
}