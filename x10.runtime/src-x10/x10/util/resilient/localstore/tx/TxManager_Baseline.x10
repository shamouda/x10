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

    public def lockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
        throw new Exception("operation not supported for baseline tx manager");
    }
    
    public def unlockAll(id:Long, start:Long, opPerPlace:Long, keys:Rail[K],readFlags:Rail[Boolean]) {
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