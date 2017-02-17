import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.LocalTx;
import x10.util.resilient.localstore.ResilientStore;

public class IncrementSyncLocal {
    
    public static def main(args:Rail[String]) {
        val sparePlaces = 0;
        STMAppUtils.printBenchmarkStartingMessage("IncrementSyncLocal", -1, -1, -1, sparePlaces);
        val start = System.nanoTime();
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        
        try {
            val map = store.makeMap("map");
            increment(map, mgr.activePlaces(), start);
        }catch(e:Exception) {
            Console.OUT.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static def increment(map:ResilientNativeMap, activePG:PlaceGroup, start:Long) {
        val startProc = System.nanoTime();
        finish for (p in activePG) at (p) async {        	
        	map.executeLocalTransaction( Place(2), (tx:LocalTx) => {
                var acc1:BankAccount = tx.get("X") as BankAccount;
                if (acc1 == null)
                    acc1 = new BankAccount(0);
                val oldv = acc1.account;
                acc1.account ++;
                val newv = acc1.account;
                tx.put("X", acc1);
                //Console.OUT.println("App-Tx["+tx.id+"] changing from ["+oldv+"] to ["+newv+"]");
        	});
        }
        val endProc = System.nanoTime();
        map.printTxStatistics();

        val acc = at (Place(2)) map.get("X") as BankAccount;
        if (acc.account != activePG.size()) {
        	Console.OUT.println("!! Failed !!  actual:" + acc.account + " expected:" + activePG.size());
        	return;
        }
        
        
        val initTime = (startProc-start)/1e9;
        val processingTime = (endProc-startProc)/1e9;
        Console.OUT.println("InitTime:" + initTime + " seconds");
        Console.OUT.println("ProcessingTime:" + processingTime + " seconds");
        Console.OUT.println("+++++ Test Succeeded +++++");
        
    }
    
}