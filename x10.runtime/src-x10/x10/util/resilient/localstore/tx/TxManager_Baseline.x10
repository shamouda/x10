package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

public class TxManager_Baseline extends TxManager {

    public def this(map:MapData) {
        super(map);
        if (here.id == 0) Console.OUT.println("TxManager_Baseline");
    }
    
    public def get(id:Long, key:String):Cloneable {
        return data.baselineGetValue(key);
    }
    
    public def put(id:Long, key:String, value:Cloneable):Cloneable {
        return data.baselinePutValue(key, value);
    }
    
    public def delete(id:Long, key:String):Cloneable {
        return data.baselinePutValue(key, null);
    }
    
    public def lockRead(id:Long, key:String) {
        throw new Exception("operation not supported for baseline tx manager");
    }
        
    public def lockWrite(id:Long, key:String) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockRead(id:Long, key:String) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockWrite(id:Long, key:String) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def lockRead(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def lockWrite(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockRead(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockWrite(id:Long, keys:ArrayList[String]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def commit(log:TxLog) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def abort(log:TxLog) {
        throw new Exception("operation not supported for baseline tx manager");
    }

    public def validate(log:TxLog) {
        throw new Exception("operation not supported for baseline tx manager");
    }
}