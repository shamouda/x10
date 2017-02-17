import x10.util.Random;
import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.resilient.iterative.SimplePlaceHammer;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.tx.ConflictException;

public class BankAsyncResilient {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val DISABLE_CKPT = System.getenv("DISABLE_CKPT") != null && System.getenv("DISABLE_CKPT").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 4) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress spare");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        val sparePlaces = Long.parseLong(args(3));        
        STMAppUtils.printBenchmarkStartingMessage("BankAsyncResilient", accountsPerPlace, transfersPerPlace, debugProgress, sparePlaces);
        val start = System.nanoTime();
        
        
        val supportShrinking = false;
        val recoveryTimes = new ArrayList[Long]();
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n || !DISABLE_CKPT) {
            val hammer = new SimplePlaceHammer();
            hammer.scheduleTimers();
        }
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
            
            val sum2 = STMAppUtils.sumAccounts(map, mgr.activePlaces());

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
            Console.OUT.println(" ! Test Failed ! Exception thrown  ["+ex.getMessage()+"] ");
            ex.printStackTrace();
        }
    }
    
    public static def randomTransfer(map:ResilientNativeMap, activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long, recovered:Boolean){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val placeIndex = activePG.indexOf(p);
            var start:Long = 1;
            if (recovered) {
                start = STMAppUtils.restoreProgress(map, placeIndex, 0) + 1;
                Console.OUT.println(here + " continue transfering from " + start + "   slave:" + map.store.plh().slave);
            }
            val rand = new Random(System.nanoTime());
            for (i in start..transfersPerPlace) {
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
                val amount = Math.abs(rand.nextLong()%100);
                var pg:PlaceGroup;
                if (DISABLE_CKPT)
                    pg = STMAppUtils.createGroup(p1, p2);
                else
                    pg = STMAppUtils.createGroup(here, p1, p2);
                
                val members = pg;
                val success = map.executeTransaction( members, (tx:Tx) => {
                    if (TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] here["+here+"] TXSTART"+ (recovered?"RECOVER":"")+" accounts["+randAcc1+","+randAcc2+"] places["+p1+","+p2+"] amounts["+amount+"]");
                    val f1 = tx.asyncAt(p1, () => {
                        var acc1:BankAccount = tx.get(randAcc1) as BankAccount;
                        if (acc1 == null)
                            acc1 = new BankAccount(0);
                        acc1.account -= amount;
                        tx.put(randAcc1, acc1);
                    });
                    val f2 = tx.asyncAt(p2, () => {
                        var acc2:BankAccount = tx.get(randAcc2) as BankAccount;
                        if (acc2 == null)
                            acc2 = new BankAccount(0);
                        acc2.account += amount;
                        tx.put(randAcc2, acc2);
                    });
                    if (!DISABLE_CKPT)
                        tx.put("p"+placeIndex, new CloneableLong(i));
                    f1.force();
                    f2.force();
                } );
                
                if (success == Tx.SUCCESS_RECOVER_STORE)
                    throw new RecoverDataStoreException("RecoverDataStoreException", here);
            }
        }
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