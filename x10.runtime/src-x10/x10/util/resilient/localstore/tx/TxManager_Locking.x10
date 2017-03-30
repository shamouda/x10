package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

public class TxManager_Locking extends TxManager {

    public def this(data:MapData, immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_Locking");
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
    
    public def lockRead(id:Long, keys:ArrayList[String]) {
        lockRead_Locking(id, keys);
    }
    public def lockWrite(id:Long, keys:ArrayList[String]) {
        lockWrite_Locking(id, keys);
    }
    public def unlockRead(id:Long, keys:ArrayList[String]) {
        unlockRead_Locking(id, keys);
    }
    public def unlockWrite(id:Long, keys:ArrayList[String]) {
        unlockWrite_Locking(id, keys);
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