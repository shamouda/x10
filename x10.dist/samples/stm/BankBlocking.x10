import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.TxFuture;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;

public class BankBlocking {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 3) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress");
            return;
        }
        val disableNonBlocking = System.getenv("DISABLE_NON_BLOCKING");
        if (disableNonBlocking == null || !disableNonBlocking.equals("1")) {
            Console.OUT.println("Blocking mode required, must export DISABLE_NON_BLOCKING=1");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        
        Console.OUT.println("Running Bank (Blocking) Benchmark. Places["+Place.numPlaces()
                +"] Accounts["+(accountsPerPlace*Place.numPlaces()) +"] AccountsPerPlace["+accountsPerPlace
                +"] Transfers["+(transfersPerPlace*Place.numPlaces()) +"] TransfersPerPlace["+transfersPerPlace+"] "
                +" PrintProgressEvery["+debugProgress+"] iterations");
        val start = System.nanoTime();
        
        val sparePlaces = 0;
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());

        val map = store.makeMap("mapA");
        try {
            val startTransfer = System.nanoTime();
            randomTransfer(map, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress);
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
    
    public static def randomTransfer(map:ResilientNativeMap, activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val rand = new Random(System.nanoTime());
            for (i in 1..transfersPerPlace) {
                if (i%debugProgress == 0)
                    Console.OUT.println(here + " progress " + i);
                val rand1 = Math.abs(rand.nextLong()% accountsMAX);
                val p1 = STMAppUtils.getPlace(rand1, activePG, accountsPerPlace);
                
                var rand2:Long = Math.abs(rand.nextLong()% accountsMAX);
                var tmpP2:Place = STMAppUtils.getPlace(rand2, activePG, accountsPerPlace);
                while (rand1 == rand2 || p1.id == tmpP2.id) {
                    rand2 = Math.abs(rand.nextLong()% accountsMAX);
                    tmpP2 = STMAppUtils.getPlace(rand2, activePG, accountsPerPlace);
                }
                val p2 = tmpP2;
                val randAcc1 = "acc"+rand1;
                val randAcc2 = "acc"+rand2;
                //val amount = Math.abs(rand.nextLong()%100);
                val members = STMAppUtils.createGroup(p1, p2);
                map.executeTransaction( () => {
                    val tx = map.startGlobalTransaction(members);
                    val txId = tx.id;
                    val amount = txId;
                    if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] TXSTART accounts["+randAcc1+","+randAcc2+"] places["+p1+","+p2+"] amount["+ amount + "]");
                    var acc1:BankAccount = tx.getRemote(p1, randAcc1) as BankAccount;
                    var acc2:BankAccount = tx.getRemote(p2, randAcc2) as BankAccount;
                    if (acc1 == null)
                        acc1 = new BankAccount(0);
                    if (acc2 == null)
                        acc2 = new BankAccount(0);
                    acc1.account -= amount;
                    acc2.account += amount;
                    tx.putRemote(p1, randAcc1, acc1);
                    tx.putRemote(p2, randAcc2, acc2);
                    tx.commit();
                });
            }
        }
    }
    
}