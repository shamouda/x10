import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.tx.ConflictException;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.TxFuture;


public class STMAppUtils {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def getPlace(accId:Long, activePG:PlaceGroup, accountPerPlace:Long):Place{
        return activePG(accId/accountPerPlace);
    }
    
    public static def restoreProgress(map:ResilientNativeMap, placeIndex:Long, defaultProg:Long){
        val tx = map.startLocalTransaction();
        try {
            val cl = tx.get("p"+placeIndex);
            tx.commitIgnoreDeadSlave();
            var res:Long = defaultProg;
            if (cl != null)
                res = (cl as CloneableLong).v;
            
            if (TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress here["+here+"] placeIndex["+placeIndex+"] progress["+res+"]");
            return res;
        }
        catch(ex:Exception) {
            if (TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress failed, exception ["+ex.getMessage()+"]");
            throw ex;
        }
    }
    
    public static def createGroup(p1:Place) {
        val rail = new Rail[Place](1);
        rail(0) = p1;
        return new SparsePlaceGroup(rail);
    }
    
    public static def createGroup(p1:Place, p2:Place) {
        if (p1.id == p2.id)
            return createGroup(p1);
        val rail = new Rail[Place](2);
        rail(0) = p1;
        rail(1) = p2;
        return new SparsePlaceGroup(rail);
    }
    
    public static def createGroup(p1:Place, p2:Place, p3:Place) {
        if (p1.id == p2.id && p2.id == p3.id)
            return createGroup(p1);
        
        if (p1.id == p2.id)
            return createGroup(p1, p3);
        else if (p2.id == p3.id || p1.id == p3.id)
            return createGroup(p1, p2);
        val rail = new Rail[Place](3);
        rail(0) = p1;
        rail(1) = p2;
        rail(2) = p3;
        return new SparsePlaceGroup(rail);
    }
    
    public static def sumAccounts(map:ResilientNativeMap, activePG:PlaceGroup){
        var sum:Long = 0;
        val list = new ArrayList[TxFuture]();
        val tx = map.startGlobalTransaction(activePG);
        for (p in activePG) {
            val f = tx.asyncAt(p, () => {
                var localSum:Long = 0;
                val set = tx.keySet();
                val iter = set.iterator();
                while (iter.hasNext()) {
                    val accId  = iter.next();
                    val obj = tx.get(accId);
                    if (obj != null  && obj instanceof BankAccount) {
                        localSum += (obj as BankAccount).account;
                    }
                }
                return localSum;
            });
            list.add(f);
        }
        for (f in list)
            sum += f.waitV() as Long;
        tx.commit();
        return sum;
    }
}

class RecoverDataStoreException(place:Place) extends Exception{
    public def this(message:String, place:Place) {
        super(message);
        property(place);
    }
}