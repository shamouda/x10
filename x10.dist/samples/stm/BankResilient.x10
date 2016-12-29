import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.tx.TxFuture;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.resilient.iterative.SimplePlaceHammer;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.tx.ConflictException;

public class BankResilient {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val DISABLE_CKPT = System.getenv("DISABLE_CKPT") != null && System.getenv("DISABLE_CKPT").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 4) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress spare");
            return;
        }
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n)
            Console.OUT.println("Running in non-resilient mode");
        else
            Console.OUT.println("Running in resilient mode");
        
        val start = System.nanoTime();
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        val sparePlaces = Long.parseLong(args(3));
        val supportShrinking = false;
        val recoveryTimes = new ArrayList[Long]();
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n || !DISABLE_CKPT) {
            val hammer = new SimplePlaceHammer();
            hammer.scheduleTimers();
        }
        Console.OUT.println("Running Resilient Bank (Non Blocking) Benchmark. Places["+ (Place.numPlaces() - sparePlaces)
                +"] Accounts["+(accountsPerPlace*Place.numPlaces()) +"] AccountsPerPlace["+accountsPerPlace
                +"] Transfers["+(transfersPerPlace*Place.numPlaces()) +"] TransfersPerPlace["+transfersPerPlace+"] "
                +" PrintProgressEvery["+debugProgress+"] iterations sparePlaces["+sparePlaces+"] ");
        
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val store = ResilientStore.make(mgr.activePlaces());
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
                    randomTransfer(map, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress, recover);
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
            
            val sum2 = sumAccounts(map, mgr.activePlaces());

            val end = System.nanoTime();
            if (sum2 == 0) {
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
                Console.OUT.println(" ! Test Failed ! sum2["+sum2+"] != 0");
        }catch(ex:Exception) {
            Console.OUT.println(" ! Test Failed ! Exception thrown");
            ex.printStackTrace();
        }
    }
    
    public static def randomTransfer(map:ResilientNativeMap, activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long, recover:Boolean){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val placeIndex = activePG.indexOf(p);
            var start:Long = 1;
            if (recover) {
                start = STMResilientAppUtils.restoreProgress(map, placeIndex, 0) + 1;
                Console.OUT.println(here + " continue transfering from " + start + "   slave:" + map.store.plh().slave);
            }
            val rand = new Random(System.nanoTime());
            for (i in start..transfersPerPlace) {
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
                var members:PlaceGroup;
                if (DISABLE_CKPT)
                    members = STMResilientAppUtils.createGroup(p1, p2);
                else
                    members = STMResilientAppUtils.createGroup(here, p1, p2);
                var trial:Long = -1;
                var sleepTime:Long = 10;
                do {
                    var txId:Long = -1;
                    try {
                        val tx = map.startGlobalTransaction(members);
                        txId = tx.id;
                        
                        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] here["+here+"] TXSTART"+ (recover?"RECOVER":"")+" trial["+trial+"] accounts["+randAcc1+","+randAcc2+"] places["+p1+","+p2+"] amounts["+amount+"]");
                        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] here["+here+"] p1["+here+"] p2["+p1+"] p3["+p2+"]");
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
                        
                        if (!DISABLE_CKPT)
                            tx.put("p"+placeIndex, new CloneableLong(i));
                        
                        val success = tx.commit();
                        if (success == Tx.SUCCESS_RECOVER_STORE) {
                            if (TM_DEBUG || recover) 
                                Console.OUT.println("Tx["+txId+"] here["+here+"] success["+success+"]");
                            throw new RecoverDataStoreException("RecoverDataStoreException", here);
                        }
                        if (i%debugProgress == 0)
                            Console.OUT.println(here + " progress " + i);
                        break;
                    } catch(ex:Exception) {
                        trial = STMResilientAppUtils.processException(txId, ex, trial);
                    }
                } while(true);
            }
        }
    }
    
    public static def sumAccounts(map:ResilientNativeMap, activePG:PlaceGroup){
        var sum:Long = 0;
        val list = new ArrayList[TxFuture]();
        val tx = map.startGlobalTransaction(activePG);
        for (p in activePG) {
            val f = tx.asyncAt(p, () => {
                var localSum:Long = 0;
                val set = tx.keySet();
                val iter = set.iterator();
                while (iter.hasNext()) {
                    val accId  = iter.next();
                    val obj = tx.get(accId);
                    
                    if (obj != null && obj instanceof BankAccount) {
                        val v = (obj as BankAccount).account;
                        localSum += v;
                        if (TM_DEBUG) Console.OUT.println("get("+accId+") === " + v );
                    }
                }
                return localSum;
            });
            list.add(f);
        }
        for (f in list)
            sum += f.waitV() as Long;
        tx.commit();
        return sum;
    }
        
    public static def getPlace(accId:Long, activePG:PlaceGroup, accountPerPlace:Long):Place{
        return activePG(accId/accountPerPlace);
    }
    
    private static def getImpactedPlaces(mulExp:MultipleExceptions) {
        var str:String = "";
        val list = new ArrayList[Place]();
        val exList = mulExp.getExceptionsOfType[RecoverDataStoreException]();
        if (exList != null) {
            for (e in exList) {
                list.add(e.place);
                str += e.place + " ";
            }
        }
        Console.OUT.println("Application Failure, impacted  places: " + str);
        return new SparsePlaceGroup(list.toRail());
    }
}