import x10.util.concurrent.Future;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.NestingTx;
import x10.util.resilient.localstore.tx.ConflictException;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.CloneableLong;

/*
class ResilientNativeMap {
    public abstract def isRecoveryRequired():Boolean; //true if one or more active places are dead
}
class Tx {
    
    //***************** Starting (nested) transactions ********************
    //these 2 functions replace old asyncAt, and syncAt functions//
    public static abstract def executeTransaction(target:Place, closure:(tx:Tx)=>Any):Any; //starts a (nested) transaction synchronously at a remote place
    public static abstract def executeAsyncTransaction(target:Place, closure:(tx:Tx)=>Any):Future[Any]; //starts a (nested) transaction asynchronously at a remote place
        
    //***************** Commit/Abort ********************
    
    private abstract def commit():void;  //called inside the startTransaction(...) closure.
    private abstract def abort():void;   //called inside the startTransaction(...) closure.
    
    //***************** Get ********************
    public abstract def get(key:String):Cloneable; //local get
    
    //***************** PUT ********************
    public abstract def put(key:String, value:Cloneable):Cloneable; //local put
    
    //***************** Delete ********************
    public abstract def delete(key:String):Cloneable; //local delete
    
}

*/

/*** Example program ***/
public class Nesting {
    
    public static def main(args:Rail[String]) {
        val mgr = new PlaceManager(0, false);
        val store = ResilientStore.make(mgr.activePlaces());
        val map = store.makeMap("map");
        
        while (true) {
            try {
                outer(map);
                break;
            } catch(ex:ConflictException) {
                //repeat
            }
        }
    }
    
    public static def outer(map:ResilientNativeMap) {
        map.executeTransaction(here , (tx:NestingTx)=> {
            val bFuture = inner(map);
            val cTmp = tx.get("c");
            val c = cTmp == null? null : cTmp as CloneableLong ;            
            val b = bFuture.force() as CloneableLong;
            
            map.executeAsyncTransaction(Place(3), (tx:NestingTx)=> {
                return tx.put("d", b + c);
            });
            
            map.executeAsyncTransaction(Place(4), (tx:NestingTx)=> {
                return tx.put("e", b - c);
            });
            
            return null;
        });
    }
    
    public static def inner(map:ResilientNativeMap) {
        val future = map.executeAsyncTransaction(Place(1), (tx:NestingTx)=> {
            val aTmp = innerInner(map);
            val a = aTmp == null? new CloneableLong(0) : aTmp as CloneableLong;
            val b = a + new CloneableLong(2);
            tx.put("b", b);
            return b;
        });
        return future;
    }
    
    public static def innerInner(map:ResilientNativeMap) {
        val result = map.executeTransaction(Place(2), (tx:NestingTx)=> {
            val aTmp = tx.get("a");
            val a = aTmp == null? null : aTmp as CloneableLong;
            return a; 
        });
        return result;
    }
    
}