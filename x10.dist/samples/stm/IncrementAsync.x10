import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;

public class IncrementAsync {
    
    public static def main(args:Rail[String]) {
        val sparePlaces = 0;
        STMAppUtils.printBenchmarkStartingMessage("Increment", -1, -1, -1, sparePlaces);
        val start = System.nanoTime();
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        
        try {
            val map = store.makeMap("map");
            increment(map, mgr.activePlaces(), start);
        }catch(e:Exception) {
            Console.OUT.println(e.getMessage());
        }
    }
    
    public static def increment(map:ResilientNativeMap, activePG:PlaceGroup, start:Long) {
        val startProc = System.nanoTime();
        val members = STMAppUtils.createGroup(Place(2));
        
        finish for (p in activePG) at (p) async {        	
        	map.executeTransaction( members, (tx:Tx) => { 
        		tx.asyncAt(Place(2), () => {
                    var acc1:BankAccount = tx.get("X") as BankAccount;
                    if (acc1 == null)
                        acc1 = new BankAccount(0);
                    val oldv = acc1.account;
                    acc1.account ++;
                    val newv = acc1.account;
                    tx.put("X", acc1);
                    //Console.OUT.println("App-Tx["+tx.id+"] changing from ["+oldv+"] to ["+newv+"]");
                });
        	});
        }
        val endProc = System.nanoTime();
        map.printTxStatistics();
        
        
        val tx = map.startGlobalTransaction(members);
        val acc = tx.getRemote(Place(2), "X") as BankAccount;
        if (acc.account != activePG.size()) {
        	Console.OUT.println("!! Failed !!  actual:" + acc.account + " expected:" + activePG.size());
        	return;
        }
        tx.commit();
        
        val initTime = (startProc-start)/1e9;
        val processingTime = (endProc-startProc)/1e9;
        Console.OUT.println("InitTime:" + initTime + " seconds");
        Console.OUT.println("ProcessingTime:" + processingTime + " seconds");
        Console.OUT.println("+++++ Test Succeeded +++++");
        
    }
    
}