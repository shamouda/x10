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
import x10.util.resilient.iterative.SimplePlaceHammer;
import x10.util.resilient.localstore.CloneableLong;

public class RAResilient {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val DISABLE_CKPT = System.getenv("DISABLE_CKPT") != null && System.getenv("DISABLE_CKPT").equals("1");
    
    
    public static def main(args:Rail[String]) {
        if (args.size != 4) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_updates_per_place progress spare");
            return;
        }
        
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n)
            Console.OUT.println("Running in non-resilient mode");
        else
            Console.OUT.println("Running in resilient mode");
        
        val start = System.nanoTime();
        val expAccounts = Long.parseLong(args(0));
        val expUpdates = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts) ) as Long;
        val updatesPerPlace = Math.ceil(Math.pow(2, expUpdates) ) as Long;
        val sparePlaces = Long.parseLong(args(3));
        val supportShrinking = false;
        val recoveryTimes = new ArrayList[Long]();
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n || !DISABLE_CKPT) {
            val hammer = new SimplePlaceHammer();
            hammer.scheduleTimers();
        }
        Console.OUT.println("Running Resilient RandomAccess (Non Blocking) Benchmark. Places["+Place.numPlaces()
                +"] Accounts["+(accountsPerPlace*Place.numPlaces()) +"] AccountsPerPlace["+accountsPerPlace
                +"] Updates["+(updatesPerPlace*Place.numPlaces()) +"] UpdatesPerPlace["+updatesPerPlace+"] "
                +" PrintProgressEvery["+debugProgress+"] iterations sparePlaces["+sparePlaces+"] ");
        
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
        val activePG = mgr.activePlaces();
        
        val accountsMAX = accountsPerPlace * activePG.size();
        val requestsMap = new HashMap[Long,PlaceUpdateRequests]();
        var expectedSum:Long = 0;
        for (p in activePG) {
            val x = new PlaceUpdateRequests(updatesPerPlace);
            x.initRandom(accountsMAX);
            requestsMap.put(p.id, x);
            expectedSum += x.amountsSum;
        }
        
        val map = store.makeMap("mapA");
        try {
            val startTransfer = System.nanoTime();
            var recover:Boolean = false;
            do {
                if (recover) {
                    Console.OUT.println("started recovery");
                    val startRecovery = System.nanoTime();
                    val changes = mgr.rebuildActivePlaces();
                    store.updateForChangedPlaces(changes);
                    Console.OUT.println("finished recovery");
                    recoveryTimes.add(System.nanoTime() - startRecovery);
                }
                try {
                    randomUpdate(map, mgr.activePlaces(), accountsPerPlace, updatesPerPlace, debugProgress, requestsMap, recover);
                    break;
                } catch(mulExp:MultipleExceptions) {
                    mulExp.printStackTrace();
                    recover = true;
                } catch (ex:Exception) {
                    throw ex;  
                }
            }while(true);
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
                var str:String = "";
                for (t in recoveryTimes) {
                    str += t/1e9 + ":";
                }
                Console.OUT.println("RecoveryTimesSeconds:" + str);
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
            updatesPerPlace:Long, debugProgress:Long, requestsMap:HashMap[Long,PlaceUpdateRequests], recover:Boolean){
        finish for (p in activePG) {
            val placeIndex = activePG.indexOf(p);
            val requests = requestsMap.getOrThrow(placeIndex);
            at (p) async {
                var start:Long = 1;
                if (recover) {
                    start = STMAppUtils.restoreProgress(map, placeIndex, 0)+1;
                    Console.OUT.println(here + " continue transfering from " + start + "   slave:" + map.store.plh().slave);
                }
                val rand = new Random(System.nanoTime());
                for (i in start..updatesPerPlace) {
                    val rand1 = requests.accountsRail(i-1);
                    val p1 = STMAppUtils.getPlace(rand1, activePG, accountsPerPlace);
                    val randAcc = "acc"+rand1;
                    val amount = requests.amountsRail(i-1);
                    
                    var pg:PlaceGroup;
                    if (DISABLE_CKPT)
                        pg = STMAppUtils.createGroup(p1);
                    else
                        pg = STMAppUtils.createGroup(here,p1);
                    val members = pg;
                    map.executeTransaction( () => {
                        val tx = map.startGlobalTransaction(members);
                        val txId = tx.id;
                        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] TXSTART accounts["+randAcc+"] place["+p1+"] amount["+amount+"]");
                        tx.syncAt(p1, () => {
                            val obj = tx.get(randAcc);
                            var acc:BankAccount = null;
                            if (obj == null)
                                acc = new BankAccount(0);
                            else
                                acc = obj as BankAccount;
                            acc.account += amount;
                            tx.put(randAcc, acc);
                        });
                        if (!DISABLE_CKPT)
                            tx.put("p"+placeIndex, new CloneableLong(i));
                        val success = tx.commit();
                        if (success == Tx.SUCCESS_RECOVER_STORE) {
                            throw new RecoverDataStoreException("RecoverDataStoreException", here);
                        }
                        if (i%debugProgress == 0)
                            Console.OUT.println(here + " progress " + i);
                    });
                }
            }
        }
    }    
    
}

class PlaceUpdateRequests(size:Long){
    val accountsRail:Rail[Long];
    val amountsRail:Rail[Long];
    var amountsSum:Long = 0;
    public def this (s:Long) {
        property(s);
        accountsRail = new Rail[Long](s);
        amountsRail = new Rail[Long](s);
    }
    
    public def initRandom(accountsMAX:Long) {
        val rand = new Random(System.nanoTime());
        for (var i:Long = 0; i < size; i += 2) {
            accountsRail(i) = Math.abs(rand.nextLong()% accountsMAX);
            amountsRail(i) = Math.abs(rand.nextLong()%1000);
            
            accountsRail(i+1) = Math.abs(rand.nextLong()% accountsMAX);
            amountsRail(i+1) = -1*amountsRail(i);
            
            amountsSum+= amountsRail(i) ;
            amountsSum+= amountsRail(i+1);
        }
    }
    
}