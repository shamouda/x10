import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.LockManager;
import x10.util.resilient.localstore.ResilientStore;

public class IncrementSyncLocking {
    
    public static def main(args:Rail[String]) {
    	val sparePlaces = 0;
        STMAppUtils.printBenchmarkStartingMessage("IncrementSyncLocking", -1, -1, -1, sparePlaces);
        val start = System.nanoTime();
        
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        try {
            val map = store.makeMap("map");
            val locker = map.getLockManager();
            increment(locker, mgr.activePlaces(), start);
        }catch(e:Exception) {
        	if (e.getMessage() == null)
            Console.OUT.println("Test failed, you must set TM_DISABLED=1");
        }
    }
    
    public static def increment(locker:LockManager, activePG:PlaceGroup, start:Long) {
        val startProc = System.nanoTime();
        
        finish for (p in activePG) at (p) async {
            locker.syncAt(Place(2), () => {
                locker.lockWrite("X");
                var acc1:BankAccount = locker.getLocked("X") as BankAccount;
                if (acc1 == null)
                    acc1 = new BankAccount(0);
                val oldv = acc1.account;
                acc1.account ++;
                val newv = acc1.account;
                locker.putLocked("X", acc1);
                locker.unlockWrite("X");
            });
        }
        val endProc = System.nanoTime();
        

        val acc = at (Place(2)) {
            locker.getLocked("X") as BankAccount
        };
        
        if (acc.account != activePG.size())
            throw new Exception("!! Failed !!  account:" + acc.account + " places:" + activePG.size());
        
        val initTime = (startProc-start)/1e9;
        val processingTime = (endProc-startProc)/1e9;
        Console.OUT.println("InitTime:" + initTime + " seconds");
        Console.OUT.println("ProcessingTime:" + processingTime + " seconds");
        Console.OUT.println("+++++ Test Succeeded +++++");
    }
    
}