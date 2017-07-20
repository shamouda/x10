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
import x10.util.Timer;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.TxResult;
import x10.util.HashSet;
// TM_DEBUG=0 TM=RV_LA_WB KILL_PLACES=2,5,10 KILL_TIMES=2,2,10 X10_NPLACES=13 X10_RESILIENT_MODE=1 TM_REP=lazy ./BankAsyncResilient.o 10 10 200 3

public class BankAsyncResilient {
    private static val DISABLE_CKPT = System.getenv("DISABLE_CKPT") != null && System.getenv("DISABLE_CKPT").equals("1");
    
    public static def main(args:Rail[String]) {
        if (args.size != 5) {
            Console.OUT.println("Parameters missing exp_accounts_per_place exp_transfers_per_place progress spare optimized");
            return;
        }
        val expAccounts = Long.parseLong(args(0));
        val expTransfers = Long.parseLong(args(1));
        val debugProgress = Long.parseLong(args(2));
        val accountsPerPlace = Math.ceil(Math.pow(2, expAccounts)) as Long;
        val transfersPerPlace = Math.ceil(Math.pow(2, expTransfers)) as Long;
        val sparePlaces = Long.parseLong(args(3));
        val optimized = Long.parseLong(args(4)) == 1;
        STMAppUtils.printBenchmarkStartingMessage("BankAsyncResilient", accountsPerPlace, transfersPerPlace, debugProgress, sparePlaces, -1F, optimized);
        val start = System.nanoTime();
        
        
        val supportShrinking = false;
        val recoveryTimes = new ArrayList[Long]();
        if (x10.xrx.Runtime.RESILIENT_MODE == 0n || !DISABLE_CKPT) {
            val hammer = new SimplePlaceHammer();
            hammer.scheduleTimers();
        }
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        val immediateRecovery = false;
        val store = ResilientStore.make[String](mgr.activePlaces(), immediateRecovery);
        val map = store.makeMap();
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
                    randomTransfer(map, mgr.activePlaces(), accountsPerPlace, transfersPerPlace, debugProgress, recover, optimized);
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
    
    public static def randomTransfer(map:ResilientNativeMap[String], activePG:PlaceGroup, accountsPerPlace:Long, transfersPerPlace:Long, debugProgress:Long, recovered:Boolean, optimized:Boolean){
        val accountsMAX = accountsPerPlace * activePG.size();
        finish for (p in activePG) at (p) async {
            val placeIndex = activePG.indexOf(p);
            var start:Long = 1;
            if (recovered) {
                start = STMAppUtils.restoreProgress(map, placeIndex, 0) + 1;
                var str:String = "";
                for (pp in activePG)
                    str += pp.id + ", ";
                Console.OUT.println(here + " continue transfering from " + start + "   slave:" + map.plh().slave + "   active list {"+str+"} ");
            }
            val rand = new Random(placeIndex);
            for (i in start..transfersPerPlace) {
            	if (i%debugProgress == 0) {
                    Console.OUT.println(here + " progress " + i);
            	}
            	var randomAccounts:Rail[Long];
            	var randomPlaces:Rail[Long];
            	var index:Long = 0;
                val set = new HashSet[Long]();
                val LIMIT:Long;
            	if (DISABLE_CKPT) {
            	    randomAccounts = new Rail[Long](3);
                    randomPlaces = new Rail[Long](3);
                    LIMIT = 3;
            	}
            	else {
            	    randomAccounts = new Rail[Long](4);
                    randomPlaces = new Rail[Long](4);
                    set.add(placeIndex);
                    randomAccounts(3) = -1;
                    randomPlaces(3) = placeIndex;
                    LIMIT = 4;
            	}
            	
                while (set.size() < LIMIT) {
                    val oldSize = set.size();
                    val randNum = Math.abs(rand.nextLong()% accountsMAX);
                    val randPl = randNum/accountsPerPlace;
                    set.add(randPl);
                    if (set.size() == oldSize)
                        continue;
                    randomAccounts(index) = randNum;
                    randomPlaces(index) = randPl;
                    index++;
                }
                
                val members = randomPlaces;
                val p1 = members(0);
                val p2 = members(1);
                val p3 = members(2);
                val acc1Key = "acc"+randomAccounts(0);
                val acc2Key = "acc"+randomAccounts(1);
                val acc3Key = "acc"+randomAccounts(2);
                
                val bankClosure = (tx:Tx[String]) => {
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] here["+here+"] TXSTART"+ (recovered?"RECOVER":"")+" accounts["+acc1Key+","+acc2Key+","+acc3Key+"] places["+p1+","+p2+","+p3+"]");
                    val txId = tx.id;
                    val placeId =  ((txId >> 32) as Int);
                    val txSeq = (txId as Int);
                    var m:Long = (placeId*100000 + txSeq);
                    if (m%2 == 1)
                        m++;
                    val amount = m;
                    val halfAmount = amount/2;
                    
                    tx.asyncAt(p1, () => {
                        val obj = tx.get(acc1Key);
                        var acc1:BankAccount;
                        if (obj == null) 
                            acc1 = new BankAccount(0);
                        else
                            acc1 = obj as BankAccount;
                        acc1.account -= amount;
                        tx.put(acc1Key, acc1);
                        
                        tx.asyncAt(p2, () => {
                            val obj = tx.get(acc2Key);
                            var acc2:BankAccount;
                            if (obj == null) 
                                acc2 = new BankAccount(0);
                            else
                                acc2 = obj as BankAccount;
                            acc2.account += halfAmount;
                            tx.put(acc2Key, acc2);
                            
                            tx.asyncAt(p3, () => {
                                val obj = tx.get(acc3Key);
                                var acc3:BankAccount;
                                if (obj == null) 
                                    acc3 = new BankAccount(0);
                                else
                                    acc3 = obj as BankAccount;
                                acc3.account += halfAmount;
                                tx.put(acc3Key, acc3);
                            });
                        });
                    });
                    
                    if (!DISABLE_CKPT)
                        tx.put("p"+placeIndex, new CloneableLong(i));
                    
                    return null;
                };
                var res:TxResult = null;
                if (optimized)
                    res = map.executeTransaction( members, bankClosure , -1, -1);
                else
                    res = map.executeTransaction( bankClosure, -1);    
                
                if (res.commitStatus == Tx.SUCCESS_RECOVER_STORE)
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