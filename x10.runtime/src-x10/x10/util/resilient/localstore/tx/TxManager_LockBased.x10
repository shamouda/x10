package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;

public class TxManager_LockBased extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_LockBased");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_LockBased(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_LockBased(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return put_LockBased(id, key, null);
    }
    
    public def lockRead(id:Long, key:String) {
        lockRead_LockBased(id, key);
    }
        
    public def lockWrite(id:Long, key:String) {
        lockWrite_LockBased(id, key);
    }
    
    public def unlockRead(id:Long, key:String) {
        unlockRead_LockBased(id, key);
    }
    
    public def unlockWrite(id:Long, key:String) {
        unlockWrite_LockBased(id, key);
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