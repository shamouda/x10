import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.HashMap;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.LockManager;

public class IntSet2AsyncLocking {
	private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");

	public static def main(args:Rail[String]) {
        if (args.size != 4) {
            Console.OUT.println("Parameters missing: exp_accounts_per_place(2^N) exp_operations_per_place(2^N) debugProgress read_percentage(float)");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expOperations = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val readPercentage = Float.parseFloat(args(3));
        if (readPercentage < 0 || readPercentage > 1.0) {
        	Console.OUT.println("read_percentage must have a value between 0.0 and 1.0");
            return;
        }
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val operationsPerPlace = Math.ceil(Math.pow(2, expOperations)) as Long;
        val sparePlaces = 0;
        
        STMAppUtils.printBenchmarkStartingMessage("IntSet2Async", accountsPerPlace, operationsPerPlace, debugProgress, sparePlaces, readPercentage);       
        val start = System.nanoTime();

        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        val activePG = mgr.activePlaces();
        
        val accountsMAX = accountsPerPlace * activePG.size();
        val requestsMap = new HashMap[Long,PlaceRandomRequests]();
        for (p in activePG) {
            val x = new PlaceRandomRequests(operationsPerPlace, 2, readPercentage);
            x.initRandom(accountsMAX, accountsPerPlace);
            requestsMap.put(p.id, x);
        }
        val map = store.makeMap("mapA");
        val locker = map.getLockManager();
        try {
            val startProc = System.nanoTime();
            processTransactions(locker, mgr.activePlaces(), accountsPerPlace, operationsPerPlace, debugProgress, requestsMap);
            val endProc = System.nanoTime();
            
            map.printTxStatistics();
            
            val sum2 = STMAppUtils.sumAccountsLocking(locker, mgr.activePlaces());
            
            val end = System.nanoTime();
            if (sum2 == 0) {
                val initTime = (startProc-start)/1e9;
                val transferTime = (endProc-startProc)/1e9;
                val endTime = (end-endProc)/1e9;
                Console.OUT.println("InitTime:" + initTime + " seconds");
                Console.OUT.println("ProcessingTime:" + transferTime + " seconds");
                Console.OUT.println("Printing and Validation Time:" + endTime + " seconds");
                Console.OUT.println("+++++ Test Succeeded +++++");
            }
            else
                Console.OUT.println(" ! Test Failed ! sum2["+sum2+"] != 0");
            
            
        }catch(ex:Exception) {
            Console.OUT.println(" ! Test Failed ! Exception thrown  ["+ex.getMessage()+"] ");
            ex.printStackTrace();
        }
    }

	public static def processTransactions(locker:LockManager, activePG:PlaceGroup, accountsPerPlace:Long, 
    		operationsPerPlace:Long, debugProgress:Long, requestsMap:HashMap[Long,PlaceRandomRequests]){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) {
        	val placeIndex = activePG.indexOf(p);
            val requests = requestsMap.getOrThrow(placeIndex);
            at (p) async {
            	for (i in 1..operationsPerPlace) {
            		if (i%debugProgress == 0)
            			Console.OUT.println(here + " progress " + i);
            		
            		val txId = ( (here.id + 1) * 1000000) + i;  //for debuging only
 	            	
            		val key1 = "acc"+requests.keys1(i-1);
	                val p1 = STMAppUtils.getPlace(requests.keys1(i-1), activePG, accountsPerPlace);
	                val val1 = requests.values1(i-1);
	                
	                val key2 = "acc"+requests.keys2(i-1);
	                val p2 = STMAppUtils.getPlace(requests.keys2(i-1), activePG, accountsPerPlace);
	                val val2 = requests.values2(i-1);
	                
	                val read = requests.isRead(i-1);
	                if (TM_DEBUG) Console.OUT.println(here + " OP["+i+"] Start{{ keys["+key1+","+key2+"] places["+p1+","+p2+"] values["+val1+","+val2+"] read["+read+"] ");
                    if (read)
                    	locker.lockRead(p1, key1, p2, key2, txId); //sort and lock
                    else
                    	locker.lockWrite(p1, key1, p2, key2, txId); //sort and lock
	                
                    val f1 = locker.asyncAt(p1, () => {
	                   	if (read)
	                   		locker.getLocked(key1);
	                   	else
	                   		locker.putLocked(key1, new CloneableLong(val1));
	                });
                    
                    val f2 = locker.asyncAt(p2, () => {
                    	if (read)
                    		locker.getLocked(key2);
                    	else
                    		locker.putLocked(key2, new CloneableLong(-1 * val1));
	                });
	                
	                f1.force();
	                f2.force();
	                
	                if (read)
	                	locker.unlockRead(p1, key1, p2, key2, txId);
                    else
                    	locker.unlockWrite(p1, key1, p2, key2, txId);
	                
	                if (TM_DEBUG) Console.OUT.println(here + " OP["+i+"] End}} keys["+key1+","+key2+"] places["+p1+","+p2+"] values["+val1+","+val2+"] read["+read+"] ");                
	            }
            }
        }
    }
    
}