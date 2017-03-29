import x10.util.Random;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.Timer;

public class RAAsync {
    
    public static def main(args:Rail[String]) {
        if (args.size != 3) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_updates_per_place progress");
            return;
        }
        
        val expAccounts = Long.parseLong(args(0));
        val expUpdates = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts) ) as Long;
        val updatesPerPlace = Math.ceil(Math.pow(2, expUpdates) ) as Long;
    	val sparePlaces = 0;
    	STMAppUtils.printBenchmarkStartingMessage("RAAsync", accountsPerPlace, updatesPerPlace, debugProgress, sparePlaces, -1F);
        val start = System.nanoTime();

        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        val activePG = mgr.activePlaces();
        
        val accountsMAX = accountsPerPlace * activePG.size();
        val requestsMap = new HashMap[Long,PlaceRandomRequests]();
        var expectedSum:Long = 0;
        for (p in activePG) {
            val x = new PlaceRandomRequests(updatesPerPlace, 1, -1F);
            x.initRandom(accountsMAX, accountsPerPlace, p.id);
            requestsMap.put(p.id, x);
            expectedSum += x.valuesSum1;
        }
        
        val map = store.makeMap("mapA");
        try {
            val startTransfer = System.nanoTime();
            randomUpdate(map, mgr.activePlaces(), accountsPerPlace, updatesPerPlace, debugProgress, requestsMap);
            val endTransfer = System.nanoTime();
            
            map.printTxStatistics();
            
            val actualSum = STMAppUtils.sumAccounts(map, mgr.activePlaces());
            
            val end = System.nanoTime();
            if (actualSum == expectedSum) {
                val initTime = (startTransfer-start)/1e9;
                val transferTime = (endTransfer-startTransfer)/1e9;
                val endTime = (end-endTransfer)/1e9;
                Console.OUT.println("InitTime:" + initTime + " seconds");
                Console.OUT.println("ProcessingTime:" + transferTime + " seconds");
                Console.OUT.println("Printing and Validation Time:" + endTime + " seconds");
                Console.OUT.println("+++++ Test Succeeded +++++");
            }
            else
                Console.OUT.println(" ! Test Failed ! actualSum["+actualSum+"] != expectedSum["+expectedSum+"]");
        }catch(ex:Exception) {
            Console.OUT.println(" ! Test Failed ! Exception thrown");
            ex.printStackTrace();
        }
    }
    
    public static def randomUpdate(map:ResilientNativeMap, activePG:PlaceGroup, accountsPerPlace:Long, 
            updatesPerPlace:Long, debugProgress:Long, requestsMap:HashMap[Long,PlaceRandomRequests]){
        finish for (p in activePG) {
            val requests = requestsMap.getOrThrow(p.id);
            at (p) async {
                for (i in 1..updatesPerPlace) {
                    if (i%debugProgress == 0)
                        Console.OUT.println(here + " progress " + i);
                    val rand1 = requests.keys1(i-1);
                    val p1 = STMAppUtils.getPlace(rand1, activePG, accountsPerPlace);
                    
                    val randAcc = "acc"+rand1;
                    val amount = requests.values1(i-1);
                    val members = STMAppUtils.createGroup(p1);
                    map.executeTransaction( members, (tx:Tx) => {                        
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] TXSTARTED accounts["+randAcc+"] place["+p1+"] amount["+amount+"]");
                        val f1 = tx.asyncAt(p1, () => {
                            val obj = tx.get(randAcc);
                            var acc:BankAccount = null;
                            if (obj == null)
                                acc = new BankAccount(0);
                            else
                                acc = obj as BankAccount;
                            acc.account += amount;
                            tx.put(randAcc, acc);
                        });
                        val startWait = Timer.milliTime();
	                    f1.force();	                    
	                    tx.setWaitElapsedTime(Timer.milliTime() - startWait);
	                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] waitForFutures ["+ tx.waitForFuturesElapsedTime +"] ms");
                    });
                }
            }
        }
    }
}