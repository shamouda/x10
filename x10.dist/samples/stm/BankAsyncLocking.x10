import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.LockManager;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.concurrent.Future;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.Timer;

public class BankAsyncLocking {
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
        STMAppUtils.printBenchmarkStartingMessage("BankAsyncLocking", accountsPerPlace, transfersPerPlace, debugProgress, 0, -1F);
        val start = System.nanoTime();
        
        val sparePlaces = 0;
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());

        val map = store.makeMap();
        val locker = map.getLockManager();
        try {
            val startTransfer = System.nanoTime();
            randomTransfer(locker, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress);
            val endTransfer = System.nanoTime();
            
            locker.printTxStatistics();
            
            val sum2 = STMAppUtils.sumAccountsLocking(locker, mgr.activePlaces());
            
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
            val rand = new Random(p.id);
            for (i in 1..transfersPerPlace) {
                if (i%debugProgress == 0)
                    Console.OUT.println(here + " progress " + i);
                val rand1 = Math.abs(rand.nextLong()% accountsMAX);
                val p1 = STMAppUtils.getPlace(rand1, activePG, accountsPerPlace);
                
                var rand2:Long = Math.abs(rand.nextLong()% accountsMAX);
                var p2:Place = STMAppUtils.getPlace(rand2, activePG, accountsPerPlace);
                while (rand1 == rand2 || p1.id == p2.id) {
                    rand2 = Math.abs(rand.nextLong()% accountsMAX);
                    p2 = STMAppUtils.getPlace(rand2, activePG, accountsPerPlace);
                }
                val randAcc1 = "acc"+rand1;
                val randAcc2 = "acc"+rand2;
                val amount = Math.abs(rand.nextLong()%100);

                val startLock = Timer.milliTime();
                val tx = locker.startBlockingTransaction();
        		val txId = tx.id;
                locker.lockWrite(p1, randAcc1, p2, randAcc2, txId); //sort and lock
                tx.lockingElapsedTime = Timer.milliTime() - startLock;

                val startProc = Timer.milliTime();
                val f1 = locker.asyncAt(p1, () => {
                    var acc1:BankAccount = locker.getLocked(randAcc1, txId) as BankAccount;
                    if (acc1 == null) {
                        acc1 = new BankAccount(0);
                    }
                    acc1.account -= amount;
                    locker.putLocked(randAcc1, acc1, txId);
                });
                
                val f2 = locker.asyncAt(p2, () => {
                    var acc2:BankAccount = locker.getLocked(randAcc2, txId) as BankAccount;
                    if (acc2 == null) {
                        acc2 = new BankAccount(0);
                    }
                    acc2.account += amount;
                    locker.putLocked(randAcc2, acc2, txId);
                });
                tx.processingElapsedTime = Timer.milliTime() - startProc;
                
                val startWait = Timer.milliTime();
                f1.force();
                f2.force();
                tx.waitElapsedTime = Timer.milliTime() - startWait;
                
                val startUnlock = Timer.milliTime();
                locker.unlockWrite(p1, randAcc1, p2, randAcc2, txId);
                tx.unlockingElapsedTime = Timer.milliTime() - startUnlock;
                
                tx.totalElapsedTime = Timer.milliTime() - startLock;
            }
        }
    }
    

    
}