package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Validation
 * Acquire: Early Acquire
 * Write: Undo Logging (must use early acquire)
 **/
public class TxManager_RV_EA_UL extends TxManager {

    public def this(data:MapData) {
        super(data);
        if (here.id == 0) Console.OUT.println("TxManager_RV_EA_UL");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_RV_UL(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_RV_EA_UL(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return put_RV_EA_UL(id, key, null);
    }
    
    public def validate(log:TxLog) {
        validate_RV_EA_UL(log);
    }
    
    public def commit(log:TxLog) {
        commit_UL(log);
    }
    
    public def abort(log:TxLog) {
        abort_UL(log);
    }

}