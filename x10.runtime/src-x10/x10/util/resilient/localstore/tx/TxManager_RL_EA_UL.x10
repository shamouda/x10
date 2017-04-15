package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Locking
 * Acquire: Early Acquire
 * Write: Undo Logging
 * */
public class TxManager_RL_EA_UL extends TxManager {

    public def this(data:MapData, immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_RL_EA_UL");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_RL_UL(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_RL_EA_UL(id, key, value, false, false);
    }
    
    public def delete(id:Long, key:String, txDesc:Boolean):Cloneable {
        return put_RL_EA_UL(id, key, null, true, txDesc);
    }
    
    public def commit(log:TxLog) {
        commit_UL(log);
    }
    
    public def abort(log:TxLog) {
        abort_UL(log);
    }

    public def validate(log:TxLog) {
        throw new Exception("validate() is not needed for TxManager_RL_EA_UL");
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
    public def lockRead(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported");
    }
    public def lockWrite(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported");
    }
    public def unlockRead(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported");
    }
    public def unlockWrite(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported");
    }
}