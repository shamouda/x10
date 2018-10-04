import x10.util.ArrayList;

import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.CloneableLong;
import x10.xrx.txstore.TxConfig;
import x10.xrx.TxStoreFatalException;
import x10.xrx.MasterWorkerApp;
import x10.xrx.MasterWorkerExecutor;
import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.xrx.TxLocking;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;
import x10.xrx.Runtime;
import x10.util.HashMap;
import x10.util.Timer;
import x10.util.Option;
import x10.util.OptionsParser;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.HashSet;
import x10.compiler.Uncounted;
import x10.util.Team;

import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.GrowableRail;

public class Bank(places:Long, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long) 
    implements MasterWorkerApp {
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
        
        val start = System.nanoTime();
        val sparePlaces = 0;
        val supportShrinking = false;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val activePlaces = mgr.activePlaces();
        val app = new Bank(activePlaces.size(), accountsPerPlace, transfersPerPlace, debugProgress);
        val executor = MasterWorkerExecutor.make(activePlaces, app);
        try {
            val startTransfer = System.nanoTime();
            executor.run();
            val endTransfer = System.nanoTime();
            validateSum(executor.store(), activePlaces.size(), accountsPerPlace);
            val initTime = (startTransfer-start)/1e9;
            val transferTime = (endTransfer-startTransfer)/1e9;
            Console.OUT.println("InitTime:" + initTime + " seconds");
            Console.OUT.println("ProcessingTime:" + transferTime + " seconds");
            Console.OUT.println("+++++ Test Succeeded +++++");
        }catch(ex:Exception) {
            Console.OUT.println(" ! Test Failed ! Exception thrown  ["+ex.getMessage()+"] ");
            ex.printStackTrace();
        }
    }
    
    public def execWorker(vid:Long, store:TxStore, recovery:Boolean):Any {
        val accountsMAX = accountsPerPlace * places;
        val rand = new Random(vid);
        for (i in 1..transfersPerPlace) {
            if (debugProgress > 0 && i%debugProgress == 0)
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
            
            val distClosure = (tx:Tx) => {
                tx.asyncAt(p1, () => {
                    val oldV = tx.get(randAcc1) == null ? 0 : (tx.get(randAcc1) as CloneableLong).v;
                    tx.put(randAcc1, new CloneableLong(oldV - amount));
                    tx.asyncAt(p2, () => {
                        val oldV2 = tx.get(randAcc2) == null ? 0 : (tx.get(randAcc2) as CloneableLong).v;
                        tx.put(randAcc2, new CloneableLong(oldV2 + amount));
                    });
                });
            };
            store.executeTransaction(distClosure);
        }
        return null;
    }
    
    private static def validateSum(store:TxStore, places:Long, keysPerPlace:Long) {
        val activePlaces = store.fixAndGetActivePlaces();
        val distClosure = (tx:Tx) => {
            var totalSum:Long = 0;
            for (var p:Long = 0; p < places; p++) {
                val dest = p;
                val sum:Long = at (activePlaces(dest)) {
                    val baseKey = dest * keysPerPlace;
                    var locSum:Long = 0;
                    for (var i:Long = 0; i < keysPerPlace; i++) {
                        val k = "acc"+(baseKey + i);
                        val v = tx.get(k) == null ? 0 : (tx.get(k) as CloneableLong).v;
                        locSum += v;
                    }
                    locSum
                };
                totalSum += sum;
            }
            if (totalSum != 0) {
                throw new Exception("Total sum != Zero");
            }
        };
        store.executeTransaction(distClosure);
    }
}

