class ResilientNativeMap {
    
    public abstract def isRecoveryRequired():Boolean; //true if one or more active places are dead
}

class Tx {
    
    /***************** Starting (nested) transactions ********************/
    //these 2 functions replace old asyncAt, and syncAt functions//
    public static abstract def executeTransaction(target:Place, closure:()=>Any):Any; //starts a (nested) transaction synchronously at a remote place
    public static abstract def executeAsyncTransaction(target:Place, closure:()=>Any):Future[Any]; //starts a (nested) transaction asynchronously at a remote place
        
    /***************** Commit/Abort ********************/
    
    private abstract def commit():void;  //called inside the startTransaction(...) closure.
    private abstract def abort():void;   //called inside the startTransaction(...) closure.
    
    /***************** Get ********************/
    public abstract def get(key:String):Cloneable; //local get
    
    public abstract def getRemote(dest:Place, key:String):Cloneable; //remote get
    
    public abstract def asyncGetRemote(dest:Place, key:String):Future[Cloneable]; //async remote get
    
    /***************** PUT ********************/
    public abstract def put(key:String, value:Cloneable):Cloneable; //local put
    
    public abstract def putRemote(dest:Place, key:String, value:Cloneable):Cloneable; //remote put
    
    public abstract def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Cloneable]; //async remote put
    
    /***************** Delete ********************/
    public abstract def delete(key:String):Cloneable; //local delete
    
    public abstract def deleteRemote(dest:Place, key:String):Cloneable; //remote delete
    
    public abstract def asyncDeleteRemote(dest:Place, key:String):Future[Cloneable]; //async remote delete
    
}

/*** Example program ***/
public class Nesting {
    
    public static def main(args:Rail[String]) {
        val mgr = new PlaceManager(0, false);
        val store = ResilientStore.make(mgr.activePlaces());
        val map = store.makeMap("map");
        
        while (true) {
            try {
                outer();
                break;
            } catch(ex:ConflictException) {
                //repeat
            }
        }
    }
    
    public static def outer() {
        Tx.executeTransaction(here , (tx:Tx)=> {
            val bFuture = inner();
            val c = Tx.get("c");            
            val b = bFuture.force();
            tx.asyncPut(Place(3), "d", b + c);
            tx.asyncPut(Place(4), "e", b - c);
            return null;
        });
    }
    
    public static def inner() {
        val future = Tx.executeAsyncTransaction(Place(1), (tx:Tx)=> {
            val a = innerInner();
            val b = a+2;
            tx.put("b", b);
            return b;
        });
        return future;
    }
    
    public static def innerInner(tx:Tx) {
        val result = Tx.executeTransaction(Place(2), ()=> {
            val a = tx.get("a");
            return a; 
        });
        return result;
    }
    
}