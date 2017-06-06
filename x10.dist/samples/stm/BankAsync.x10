import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.Timer;
import x10.util.resilient.localstore.TxConfig;


public class BankAsync {
    public static def main(args:Rail[String]) {
        if (args.size != 4) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress optimized");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        val optimized = Long.parseLong(args(3)) == 1;
        
        val sparePlaces = 0;
        STMAppUtils.printBenchmarkStartingMessage("BankAsync", accountsPerPlace, transfersPerPlace, debugProgress, sparePlaces, -1F, optimized);
        val start = System.nanoTime();
        
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make[String](mgr.activePlaces(), false);
        val map = store.makeMap();
        try {
            val startTransfer = System.nanoTime();
            randomTransfer(map, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress, optimized);
            val endTransfer = System.nanoTime();
            
            map.printTxStatistics();
            
            val sum2 = STMAppUtils.sumAccounts(map, mgr.activePlaces());
            
            val end = System.nanoTime();
            if (sum2 == 0) {
                val initTime = (startTransfer-start)/1e9;
                val transferTime = (endTransfer-startTransfer)/1e9;
                val endTime = (end-endTransfer)/1e9;
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
    
    public static def randomTransfer(map:ResilientNativeMap[String], activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long, optimized:Boolean){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val rand = new Random(p.id);
            for (i in 1..transfersPerPlace) {
                if (i%debugProgress == 0)
                    Console.OUT.println(here + " progress " + i);
                val rand1 = Math.abs(rand.nextLong()% accountsMAX);
                val p1 = rand1/accountsPerPlace;
                
                var rand2:Long = Math.abs(rand.nextLong()% accountsMAX);
                var tmpP2:Long = rand2/accountsPerPlace;
                while (rand1 == rand2 || p1 == tmpP2) {
                    rand2 = Math.abs(rand.nextLong()% accountsMAX);
                    tmpP2 = rand2/accountsPerPlace;
                }
                val p2=  tmpP2;
                val randAcc1 = "acc"+rand1;
                val randAcc2 = "acc"+rand2;
                val amount = Math.abs(rand.nextLong()%100);
                
                val bankClosure = (tx:Tx[String]) => {
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] TXSTARTED accounts["+randAcc1+","+randAcc2+"] places["+p1+","+p2+"] amount["+amount+"] ");
                    
                    tx.asyncAt(p1, () => {
                        var acc1:BankAccount = tx.get(randAcc1) as BankAccount;
                        if (acc1 == null)
                            acc1 = new BankAccount(0);
                        acc1.account -= amount;
                        tx.put(randAcc1, acc1);
                        
                        tx.asyncAt(p2, () => {
                            var acc2:BankAccount = tx.get(randAcc2) as BankAccount;
                            if (acc2 == null)
                                acc2 = new BankAccount(0);
                            acc2.account += amount;
                            tx.put(randAcc2, acc2);
                        });
                    });
                    
                    
                    return null;
                };
                if (optimized) {
                    val members = STMAppUtils.createVirtualMembersRail(p1, p2);
                    map.executeTransaction(members, bankClosure, -1, -1);
                }
                else
                    map.executeTransaction(bankClosure, -1);
            }
        }
    }
    
}