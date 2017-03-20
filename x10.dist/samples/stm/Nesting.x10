class ResilientNativeMap {
    public abstract def createTransaction():Tx; //creates a transaction reference. we did not have this function before.
    public abstract def isRecoveryRequired():Boolean; //true if one or more active places are dead
}

class Tx {
    
    /***************** Starting (nested) transactions ********************/
    //these 2 functions replace old asyncAt, and syncAt functions//
    public abstract def startTransaction(target:Place, closure:()=>Any):Any; //starts a (nested) transaction synchronously at a remote place
    public abstract def startAsyncTransaction(target:Place, closure:()=>Any):Future[Any]; //starts a (nested) transaction asynchronously at a remote place
        
    /***************** Commit/Abort ********************/
    
    public abstract def commit():void;  //called inside the startTransaction(...) closure.
    public abstract def abort():void;   //called inside the startTransaction(...) closure.
    
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
                val tx = map.createTransaction();
                outer(tx);
                break;
            } catch(cex:ConflictException) {
                //repeat
            }
        }
    }
    
    public static def outer(tx:Tx) {
        tx.startTransaction(here , ()=> {
            val bFuture = inner(tx);
            
            val c = tx.get("c");            
            val b = bFuture.force();
            
            val dFuture = tx.asyncPut(Place(3), "d", b + c);
            val eFuture = tx.asyncPut(Place(4), "e", b - c);
            
            tx.commit(); //implicitly waits for local futures, before asking parent to commit
        });
    }
    
    public static def inner(tx:Tx) {
        val future = tx.startAsyncTransaction(Place(1), ()=> {
            val a = innerInner(tx);
            val b = a+2;
            tx.put("b", b);
            
            tx.commit(); // asks parent to commit
            return b;
        });
        return future;
    }
    
    public static def innerInner(tx:Tx) {
        val result = tx.startTransaction(Place(2), ()=> {
            val a = tx.get("a");
            tx.commit(); //asks parent to commit
            return a; 
        });
        return result;
    }
    
}