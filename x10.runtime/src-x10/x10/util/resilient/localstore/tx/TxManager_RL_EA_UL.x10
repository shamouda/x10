package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Locking
 * Acquire: Early Acquire
 * Write: Undo Logging
 * */
public class TxManager_RL_EA_UL extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_RL_EA_UL");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_RL_UL(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_RL_EA_UL(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return put_RL_EA_UL(id, key, null);
    }
    
    public def commit(log:TxLog) {
        commit_UL(log);
    }
    
    public def abort(log:TxLog) {
        abort_UL(log);
    }

    public def validate(log:TxLog) {
        validate_RL_EA(log);
    }
}