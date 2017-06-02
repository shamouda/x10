package x10.util.resilient.localstore.tx;

import x10.util.Set;
import x10.util.resilient.localstore.Cloneable;
import x10.util.ArrayList;

public class TxManager_Baseline[K] {K haszero} extends TxManager[K] {

    public def this(data:MapData[K], immediateRecovery:Boolean) {
        super(data, immediateRecovery);
        if (here.id == 0) Console.OUT.println("TxManager_Baseline");
    }
    
    public def get(id:Long, key:K):Cloneable {
        return data.baselineGetValue(key);
    }
    
    public def put(id:Long, key:K, value:Cloneable):Cloneable {
        return data.baselinePutValue(key, value);
    }
    
    public def delete(id:Long, key:K, txDesc:Boolean):Cloneable {
        return data.baselinePutValue(key, null);
    }
    
    public def lockRead(id:Long, key:K) {
        throw new Exception("operation not supported for baseline tx manager");
    }
        
    public def lockWrite(id:Long, key:K) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockRead(id:Long, key:K) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockWrite(id:Long, key:K) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def lockRead(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def lockWrite(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockRead(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockWrite(id:Long, keys:ArrayList[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def commit(log:TxLog[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def abort(log:TxLog[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }

    public def validate(log:TxLog[K]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
}