package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;

public class TxManager_LockFree extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_LockFree");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_LockFree(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_LockFree(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return put_LockFree(id, key, null);
    }
    
    public def commit(log:TxLog) {
        throw new Exception("commit not supported for lock based tx manager");
    }
    
    public def abort(log:TxLog) {
        throw new Exception("abort not supported for lock based tx manager");
    }
    
    public def validate(log:TxLog) {
        throw new Exception("validate not supported for lock based tx manager");
    }
    public def lockRead(id:Long, key:String) {
        throw new Exception("lockRead not supported");
    }
    public def lockWrite(id:Long, key:String) {
        throw new Exception("lockWrite not supported for lock based tx manager");
    }
    public def unlockRead(id:Long, key:String) {
        throw new Exception("unlockRead not supported for lock based tx manager");
    }
    public def unlockWrite(id:Long, key:String) {
        throw new Exception("unlockWrite not supported for lock based tx manager");
    }
}