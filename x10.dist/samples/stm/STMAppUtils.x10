import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.tx.ConflictException;
import x10.util.resilient.localstore.Tx;
import x10.util.ArrayList;
import x10.util.concurrent.Future;
import x10.util.Random;
import x10.util.resilient.localstore.LockingRequest;
import x10.util.resilient.localstore.LockingRequest.KeyInfo;
import x10.util.resilient.localstore.LockingTx;
import x10.util.resilient.localstore.TxConfig;

public class STMAppUtils {
    
    public static def createVirtualMembersRail(p1:Long, p2:Long) {
        if (p1 == p2){
            val rail = new Rail[Long](1);
            rail(0) = p1;
            return rail;
        }
        else {
            val rail = new Rail[Long](2);
            rail(0) = p1;
            rail(1) = p2;
            return rail;
        }
    }
    public static def restoreProgress(map:ResilientNativeMap, placeIndex:Long, defaultProg:Long){
        val tx = map.startLocalTransaction();
        try {
            val cl = tx.get("p"+placeIndex);
            tx.commit();
            var res:Long = defaultProg;
            if (cl != null)
                res = (cl as CloneableLong).v;
            
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress here["+here+"] p"+placeIndex+" progress["+res+"]");
            return res;
        }
        catch(ex:Exception) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress failed, exception ["+ex.getMessage()+"]");
            throw ex;
        }
    }
    
    public static def sumAccounts(map:ResilientNativeMap, activePG:PlaceGroup){
        val members = new Rail[Long](activePG.size(), (i:Long)=> i);
        val result = map.executeTransaction(members, (tx:Tx) => {
            var sum:Long = 0;
	        for (p in members) {
	            sum += tx.evalAt(p, () => {
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
	            }) as Long;
	        }
	        return sum;
        });
        return result.output;
    }
    
    public static def sumAccountsLocking(map:ResilientNativeMap, activePG:PlaceGroup){
		val result = map.executeLockingTransaction(new ArrayList[LockingRequest](), (tx:LockingTx) => {
			var sum:Long = 0;
	        for (var p:Long = 0; p < activePG.size(); p++) {
	            sum += tx.evalAt(p, () => {
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
	            }) as Long;
	        }
	        
	        return sum;
		});
        return result.output;
    }
    
    
    public static def printBenchmarkStartingMessage(name:String, accountsPerPlace:Long, 
    		transfersPerPlace:Long, debugProgress:Long, sparePlaces:Long, readPercentage:Float) {
        Console.OUT.println("Running "+name+" Benchmark. "
        	+ " Places["+Place.numPlaces() +"] "
            + " AccountsPerPlace["+accountsPerPlace +"] "
            + " ActionsPerPlace["+transfersPerPlace+"] "
            + " DebugProgress["+debugProgress+"] "
            + " SparePlaces["+sparePlaces+"] "
            + " ReadPercentage["+readPercentage+"] ");
        
        printEnv();
    }
    
    
    public static def printEnv() {
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