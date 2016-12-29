import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.LockManager;
import x10.util.resilient.localstore.tx.TxFuture;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;

public class BankLocking {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 3) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress");
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
        val store = ResilientStore.make(mgr.activePlaces());

        val map = store.makeMap("mapA");
        val locker = map.getLockManager();
        try {
            val startTransfer = System.nanoTime();
            randomTransfer(locker, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress);
            val endTransfer = System.nanoTime();
            
            val sum2 = sumAccounts(locker, mgr.activePlaces());
            
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
            Console.OUT.println(" ! Test Failed ! Exception thrown");
            ex.printStackTrace();
        }
    }
    
    public static def randomTransfer(locker:LockManager, activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val rand = new Random(System.nanoTime());
            for (i in 1..transfersPerPlace) {
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

                locker.lock(p1, randAcc1, p2, randAcc2); //sort and lock
                
                val f1 = locker.asyncAt(p1, () => {
                    var acc1:BankAccount = locker.getLocked(randAcc1) as BankAccount;
                    if (acc1 == null) {
                        acc1 = new BankAccount(0);
                    }
                    acc1.account -= amount;
                    locker.putLocked(randAcc1, acc1);
                });
                
                val f2 = locker.asyncAt(p2, () => {
                    var acc2:BankAccount = locker.getLocked(randAcc2) as BankAccount;
                    if (acc2 == null) {
                        acc2 = new BankAccount(0);
                    }
                    acc2.account += amount;
                    locker.putLocked(randAcc2, acc2);
                });
                
                f1.waitV();
                f2.waitV();
                
                locker.unlock(p1, randAcc1, p2, randAcc2);
            }
        }
    }
    
    public static def sumAccounts(locker:LockManager, activePG:PlaceGroup){
        var sum:Long = 0;
        val list = new ArrayList[TxFuture]();
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
            sum += f.waitV() as Long;
        return sum;
    }
    
    public static def getPlace(accId:Long, activePG:PlaceGroup, accountPerPlace:Long):Place{
        return activePG(accId/accountPerPlace);
    }
    
}