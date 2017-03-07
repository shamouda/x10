package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

/*
 * Concurrent Transaction Implementation with the following algorithms:
 * Read: Read Validation
 * Acquire: Early Acquire
 * Write: Write Buffering
 **/
public class TxManager_RV_EA_WB extends TxManager {

    public def this(data:MapData) {
        super(data);
        if (here.id == 0) Console.OUT.println("TxManager_RV_EA_WB");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return get_RV_WB(id, key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return put_RV_EA_WB(id, key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return put_RV_EA_WB(id, key, null);
    }
    
    public def validate(log:TxLog) {
        validate_RV_EA(log);
    }
    
    public def commit(log:TxLog) {
        commit_WB(log);
    }
    
    public def abort(log:TxLog) {
        abort_WB(log);
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