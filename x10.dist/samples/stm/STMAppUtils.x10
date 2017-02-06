import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.tx.ConflictException;
import x10.util.ArrayList;
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.LockManager;

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
        val list = new ArrayList[Future[Any]]();
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
            sum += f.force() as Long;
        tx.commit();
        return sum;
    }
    
    public static def sumAccountsLocking(locker:LockManager, activePG:PlaceGroup){
        var sum:Long = 0;
        val list = new ArrayList[Future[Any]]();
        for (p in activePG) {
            val f = locker.asyncAt(p, () => {
                var localSum:Long = 0;
                val set = locker.keySet();
                val iter = set.iterator();
                while (iter.hasNext()) {
                    val accId  = iter.next();
                    val obj = locker.getLocked(accId) as BankAccount;
                    var value:Long = 0;
                    if (obj != null) {
                        value = obj.account;
                    }
                    localSum += value;
                }
                return localSum;
            });
            list.add(f);
        }
        for (f in list)
            sum += f.force() as Long;
        return sum;
    }
    
    
    public static def printBenchmarkStartingMessage(name:String, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long, sparePlaces:Long) {
        Console.OUT.println("Running "+name+" Benchmark. "
        	+ " Places["+Place.numPlaces() +"] "
            + " AccountsPerPlace["+accountsPerPlace +"] "
            + " ActionsPerPlace["+transfersPerPlace+"] "
            + " DebugProgress["+debugProgress+"] "
            + " SparePlaces["+sparePlaces+"] ");
        
        Console.OUT.println("X10_NUM_IMMEDIATE_THREADS="+System.getenv("X10_NUM_IMMEDIATE_THREADS"));
        Console.OUT.println("X10_NTHREADS="+System.getenv("X10_NTHREADS"));
        Console.OUT.println("X10_RESILIENT_MODE="+System.getenv("X10_RESILIENT_MODE"));
        Console.OUT.println("TM="+System.getenv("TM"));
        Console.OUT.println("KILL_PLACES="+System.getenv("KILL_PLACES"));
        Console.OUT.println("KILL_TIMES="+System.getenv("KILL_TIMES"));
    }
    
}

class RecoverDataStoreException(place:Place) extends Exception{
    public def this(message:String, place:Place) {
        super(message);
        property(place);
    }
}