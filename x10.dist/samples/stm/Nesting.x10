struct TxResult {
    output:Any;
    success:Int; // SUCCESS | SUCCESS_RECOVER_STORE (when a slave dies)
};

class ResilientNativeMap {
	/*This was not existing in the old APIs*/
    public abstract def createTransaction():Tx;
}

class Tx {
    
    /***************** Starting (nested) transactions ********************/
    //these 2 functions replace old asyncAt, and syncAt functions//
    public abstract def startTransaction(target:Place, closure:()=>Any):Any;
    public abstract def startAsyncTransaction(target:Place, closure:()=>Any):Future[Any]; //waiting for futures!!!
        
    /***************** Commit/Abort ********************/
    
    public abstract def commit():TxResult;   //called inside the startTransaction(...) closure.
    public abstract def abort():void;   //called inside the startTransaction(...) closure.
    
    /***************** Get ********************/
    public abstract def get(key:String):Cloneable;
    
    public abstract def getRemote(dest:Place, key:String):Cloneable;
    
    public abstract def asyncGetRemote(dest:Place, key:String):Future[Any];
    
    /***************** PUT ********************/
    public abstract def put(key:String, value:Cloneable):Cloneable;
    
    public abstract def putRemote(dest:Place, key:String, value:Cloneable):Cloneable;
    
    public abstract def asyncPutRemote(dest:Place, key:String, value:Cloneable):Future[Any];
    
    /***************** Delete ********************/
    public abstract def delete(key:String):Cloneable;
    
    public abstract def deleteRemote(dest:Place, key:String):Cloneable;
    
    public abstract def asyncDeleteRemote(dest:Place, key:String):Future[Any];
    
}

/*** Example program ***/
public class Nesting {
    
    public static def main(args:Rail[String]) {            
        val mgr = new PlaceManager(0, false);
        val store = ResilientStore.make(mgr.activePlaces());
        val map = store.makeMap("map");
        
        val tx = map.createTransaction();    
        
        outer(tx);
    }
    
    public static def outer(tx:Tx) {
        tx.startTransaction(here , ()=> {
            val bFuture = inner(tx);
            
            val c = tx.get("c");            
            val b = bFuture.force();
            
            val dFuture = tx.asyncPut(Place(3), "d", b + c);
            
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