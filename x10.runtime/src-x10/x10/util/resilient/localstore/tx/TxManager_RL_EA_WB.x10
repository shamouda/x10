package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Locking
 * Acquire: Early Acquire
 * Write: Write Buffering
 * */
public class TxManager_RL_EA_WB extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_RL_EA_WB");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_RL_WB(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_EA_WB(id, key, value);
    }

    public def delete(id:Long, key:String):Cloneable {
        return delete_EA_WB(id, key);
    }
    
    public def validate(log:TxLog) {
        validate_RL_EA(log);
    }
    
    public def commit(log:TxLog) {
        commit_WB(log);
    }
    
    public def abort(log:TxLog) {
        abort_WB(log);
    }
}