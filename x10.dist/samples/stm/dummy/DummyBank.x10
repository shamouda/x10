import x10.util.Random;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.tx.TxFuture;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;

public class DummyBank {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 3) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        
        Console.OUT.println("Running BankTransfer Benchmark. Places["+Place.numPlaces()
                +"] Accounts["+(accountsPerPlace*Place.numPlaces()) +"] AccountsPerPlace["+accountsPerPlace
                +"] Transfers["+(transfersPerPlace*Place.numPlaces()) +"] TransfersPerPlace["+transfersPerPlace+"] "
                +" PrintProgressEvery["+debugProgress+"] iterations");
        val start = System.nanoTime();
        
        val sparePlaces = 0;
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        
        try {
            val startTransfer = System.nanoTime();
            randomTransfer(mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress);
            val endTransfer = System.nanoTime();
            
            val initTime = (startTransfer-start)/1e9;
            val transferTime = (endTransfer-startTransfer)/1e9;
            Console.OUT.println("Init Time:" + initTime + " seconds");
            Console.OUT.println("Transfer Time:" + transferTime + " seconds");
            Console.OUT.println("+++++ Test Succeeded +++++");
        }catch(ex:Exception) {
            Console.OUT.println(" ! Test Failed ! Exception thrown");
            ex.printStackTrace();
        }
    }
    
    public static def randomTransfer(activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            var id:Long = (p.id +1) *10000;
            val rand = new Random(System.nanoTime());
            for (i in 1..transfersPerPlace) {
                id++;
                if (i%debugProgress == 0)
                    Console.OUT.println(here + " progress " + i);
                val rand1 = Math.abs(rand.nextLong()% accountsMAX);
                val p1 = getPlace(rand1, activePG, accountsPerPlace);
                
                var rand2:Long = Math.abs(rand.nextLong()% accountsMAX);
                var p2:Place = getPlace(rand2, activePG, accountsPerPlace);
                while (rand1 == rand2 || p1.id == p2.id) {
                    rand2 = Math.abs(rand.nextLong()% accountsMAX);
                    p2 = getPlace(rand2, activePG, accountsPerPlace);
                }
                val randAcc1 = "acc"+rand1;
                val randAcc2 = "acc"+rand2;
                val amount = Math.abs(rand.nextLong()%100);
                val members = STMResilientAppUtils.createGroup(p1, p2);
                var trial:Long = -1;
                var sleepTime:Long = 10;
                do {
                    var txId:Long = -1;
                    try {
                        val tx = new DummyTx(id, "mapA", members);
                        txId = tx.id;
                        
                        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] TXSTART trial["+trial+"] accounts["+randAcc1+","+randAcc2+"] places["+p1+","+p2+"]");
                        val f1 = tx.asyncAt(p1, () => {
                            var acc1:BankAccount = tx.get(randAcc1) as BankAccount;
                            if (acc1 == null) {
                                acc1 = new BankAccount(0);
                            }
                            acc1.account -= amount;
                            tx.put(randAcc1, acc1);
                        });
                        val f2 = tx.asyncAt(p2, () => {
                            var acc2:BankAccount = tx.get(randAcc2) as BankAccount;
                            if (acc2 == null) {
                                acc2 = new BankAccount(0);
                            }
                            acc2.account += amount;
                            tx.put(randAcc2, acc2);
                        });
                        tx.commit();
                        break;
                    }catch(ex:Exception) {
                        trial++;
                        if (TM_DEBUG) {
                            Console.OUT.println("Tx["+txId+"] ApplicationException["+ex.getMessage()+"] ");
                            ex.printStackTrace();
                        }
                    }
                } while(true);
            }
        }
    }
    
    public static def getPlace(accId:Long, activePG:PlaceGroup, accountPerPlace:Long):Place{
        return activePG(accId/accountPerPlace);
    }
    
}