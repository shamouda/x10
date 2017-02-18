package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;

public class ResilientNativeMap (name:String, store:ResilientStore) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public val list:PlaceLocalHandle[TransactionsList];
    
    public def this(name:String, store:ResilientStore, list:PlaceLocalHandle[TransactionsList]) {
        property(name, store);
        this.list = list;
    }
    
    /**
     * Get the value of key k in the resilient map.
     */
    public def get(k:String) {
        val trans = startLocalTransaction();
        val v = trans.get(k);
        trans.commit();
        return v;
    }

    /**
     * Associate value v with key k in the resilient map.
     */
    public def set(k:String, v:Cloneable) {
        val trans = startLocalTransaction();
        trans.put(k, v);
        trans.commit();
    }

    /**
     * Remove any value associated with key k from the resilient map.
     */
    public def delete(k:String) {
        val trans = startLocalTransaction();
        trans.delete(k);
        trans.commit();
    }

    public def keySet():Set[String] {
        val trans = startLocalTransaction();
        val set = trans.keySet();
        trans.commit();
        return set;
    }
    
    public def setAll(data:HashMap[String,Cloneable]) {
        if (data == null)
            return;        
        val trans = startLocalTransaction();
        val iter = data.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();    
            trans.put(k, data.getOrThrow(k));
        }
        trans.commit();
    }
    
    public def set2(key:String, value:Cloneable, place:Place, key2:String, value2:Cloneable) {
        assert(here.id != place.id);
        val rail = new Rail[Place](2);
        rail(0) = here; 
        rail(1) = place;
        val members = new SparsePlaceGroup(rail);
        executeTransaction (members, (tx:Tx)=>{
            tx.asyncAt(place, ()=> {tx.put(key2, value2);});
            tx.put(key, value);
        });
    }
    
    public def startLocalTransaction():LocalTx {
        assert(store.plh().virtualPlaceId != -1);
        val id = store.plh().masterStore.getNextTransactionId();
        val tx = new LocalTx(store.plh, id, name);
        list().addLocalTx(tx);
        return tx;
    }
    
    
    private def startGlobalTransaction(members:PlaceGroup):Tx {
        assert(store.plh().virtualPlaceId != -1);
        val id = store.plh().masterStore.getNextTransactionId();
        if (resilient) {
            val localTx = store.txDescMap.startLocalTransaction();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] startGlobalTransaction localTx["+localTx.id+"] started ...");
            localTx.put("tx"+id, new TxDesc(id, name, getMembersIndices(members), TxDesc.STARTED));
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] startGlobalTransaction localTx["+localTx.id+"] completed ...");
        }
        val tx = new Tx(store.plh, id, name, members, store.activePlaces, store.txDescMap);
        list().addGlobalTx(tx);
        return tx;
    }
    
    public def executeTransaction(members:PlaceGroup, closure:(Tx)=>void):Int {
        do {
            val tx = startGlobalTransaction(members);
            var excpt:Exception = null;
            var pastFinish:Boolean = false;
            try {
                finish closure(tx);
                if (TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} succeeded ");
                pastFinish = true;
                return tx.commit();
            } catch(ex:Exception) {
                if (TM_DEBUG) {
                    Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} failed with Error ["+ex.getMessage()+"] pastFinish["+pastFinish+"] ");
                    ex.printStackTrace();
                }
                if (!pastFinish)
                    tx.abort(excpt); // tx.commit() aborts automatically if needed
                
                throwIfNotConflictException(ex);
            }
        }while(true);
    }
    
    public def executeLocalTransaction(target:Place, closure:(LocalTx)=>void):Int {
        val txResult = at (target) {
            var result:Int = -1n;
            try {
                Runtime.increaseParallelism();
                while(true) {
                    try {
                        val tx = startLocalTransaction();
                        closure(tx);
                        result = tx.commit();
                        break;
                    } catch(ex:Exception) {
                        throwIfNotConflictException(ex);
                    }
                    System.threadSleep(0);
                }
            }finally {
                Runtime.decreaseParallelism(1n);
            }
            result
        };
        return txResult;
    }
    
    private static def throwIfNotConflictException(ex:Exception) {
        if (ex instanceof MultipleExceptions) {
            val deadExList = (ex as MultipleExceptions).getExceptionsOfType[DeadPlaceException]();
            if (deadExList != null && deadExList.size != 0)
                throw ex;
        } 
        else if (!(ex instanceof ConflictException))
            throw ex;
    }
    
    public def restartGlobalTransaction(txDesc:TxDesc):Tx {
        assert(store.plh().virtualPlaceId != -1);
        val tx = new Tx(store.plh, txDesc.id, name, getMembers(txDesc.members), store.activePlaces, store.txDescMap);
        list().addGlobalTx(tx);
        return tx;
    }
    
    public def getMembersIndices(members:PlaceGroup):Rail[Long] {
        val rail = new Rail[Long](members.size());
        val activePG = store.activePlaces;
        for (var i:Long = 0; i <  members.size(); i++)
            rail(i) = activePG.indexOf(members(i));
        return rail;
    }
    
    public def getMembers(members:Rail[Long]):PlaceGroup {
        val list = new ArrayList[Place]();
        val activePG = store.activePlaces;
        for (var i:Long = 0; i < members.size; i++) {
            if (!activePG(members(i)).isDead())
                list.add(activePG(members(i)));
        }
        return new SparsePlaceGroup(list.toRail());
    }
    
    public def getLockManager():LockManager {
        assert(store.plh().virtualPlaceId != -1);
        val id = store.plh().masterStore.getNextTransactionId();
        return new LockManager(store.plh, id, name);
    }
    
    public def printTxStatistics() {
        val pl_stat = new ArrayList[TxPlaceStatistics]();
        for (p in store.activePlaces) {
            val pstat = at (p) {
                //####         Global TX         ####//
                val g_commitList = new ArrayList[Double]();
                val g_abortList = new ArrayList[Double]();
                for (tx in list().globalTx) {
                    if (tx.commitTime != -1)
                        g_commitList.add(tx.commitTime - tx.startTime);
                    else if (tx.abortTime != -1)
                        g_abortList.add(tx.abortTime - tx.startTime);
                }
                //####         Local TX         ####//
                val l_commitList = new ArrayList[Double]();
                val l_abortList = new ArrayList[Double]();
                for (tx in list().localTx) {
                    if (tx.commitTime != -1)
                        l_commitList.add(tx.commitTime - tx.startTime);
                    else if (tx.abortTime != -1)
                        l_abortList.add(tx.abortTime - tx.startTime);
                }
                 
                new TxPlaceStatistics(here, g_commitList, g_abortList, l_commitList, l_abortList)
            };
            pl_stat.add(pstat);
        }
        
        val g_allCommitList = new ArrayList[Double]();
        val g_allAbortList = new ArrayList[Double]();
        val l_allCommitList = new ArrayList[Double]();
        val l_allAbortList = new ArrayList[Double]();
        
        var g_cPlaces:Long = 0;
        var g_aPlaces:Long = 0;
        
        var l_cPlaces:Long = 0;
        var l_aPlaces:Long = 0;
        
        for (pstat in pl_stat) {
            Console.OUT.println(pstat);
            g_allCommitList.addAll(pstat.g_commitList);
            if (pstat.g_commitList.size() > 0)
                g_cPlaces ++;
            
            g_allAbortList.addAll(pstat.g_abortList);
            if (pstat.g_abortList.size() > 0)
                g_aPlaces ++;
            
            l_allCommitList.addAll(pstat.l_commitList);
            if (pstat.l_commitList.size() > 0)
                l_cPlaces ++;
            
            l_allAbortList.addAll(pstat.l_abortList);
            if (pstat.l_abortList.size() > 0)
                l_aPlaces ++;
        }
        
        val g_cCnt = g_allCommitList.size();
        val g_aCnt = g_allAbortList.size();
        val l_cCnt = l_allCommitList.size();
        val l_aCnt = l_allAbortList.size();
        
        val g_cMean  = TxPlaceStatistics.mean(g_allCommitList);
        val g_cSTDEV = TxPlaceStatistics.stdev(g_allCommitList, g_cMean);
        val g_aMean  = TxPlaceStatistics.mean(g_allAbortList);
        val g_aSTDEV = TxPlaceStatistics.stdev(g_allAbortList, g_aMean);
        
        val l_cMean  = TxPlaceStatistics.mean(l_allCommitList);
        val l_cSTDEV = TxPlaceStatistics.stdev(l_allCommitList, l_cMean);
        val l_aMean  = TxPlaceStatistics.mean(l_allAbortList);
        val l_aSTDEV = TxPlaceStatistics.stdev(l_allAbortList, l_aMean);
        
        if (g_cCnt > 0) {
            Console.OUT.println("Summary:GLOBAL_TX:committed:"+ g_cCnt + ":commitMeanMS:" + g_cMean + ":commitSTDEV:" + g_cSTDEV + ":committedPlaces:" + g_cPlaces
                                            +":aborted:"  + g_aCnt + ":abortMeanMS:"  + g_aMean + ":abortSTDEV:"  + g_aSTDEV + ":abortedPlaces:"   + g_aPlaces);
            Console.OUT.println("g_commitsList:" + TxPlaceStatistics.listToString(g_allCommitList));
            Console.OUT.println("g_abortsList:" + TxPlaceStatistics.listToString(g_allAbortList));
        }
        
        if (l_cCnt > 0) {
            Console.OUT.println("Summary:LOCAL_TX:committed:"+ l_cCnt + ":commitMeanMS:" + l_cMean + ":commitSTDEV:" + l_cSTDEV + ":committedPlaces:" + l_cPlaces
                                           +":aborted:"  + l_aCnt + ":abortMeanMS:"  + l_aMean + ":abortSTDEV:"  + l_aSTDEV + ":abortedPlaces:"   + l_aPlaces);
            Console.OUT.println("l_commitsList:" + TxPlaceStatistics.listToString(l_allCommitList));
            Console.OUT.println("l_abortsList:" + TxPlaceStatistics.listToString(l_allAbortList));
        }
    }
    
    public def resetTxStatistics() {
        finish for (p in store.activePlaces) at (p) async {
            store.plh().masterStore.resetState(name);
            list().globalTx.clear();
            list().localTx.clear();
        }
    }
    
}

class TxPlaceStatistics(p:Place, g_commitList:ArrayList[Double], g_abortList:ArrayList[Double], l_commitList:ArrayList[Double], l_abortList:ArrayList[Double]) {
    public def toString() {
        
        val g_commitMean = mean(g_commitList);
        val g_commitSTDEV = stdev(g_commitList, g_commitMean);
        val g_abortMean = mean(g_abortList);
        val g_abortSTDEV = stdev(g_abortList, g_abortMean);
        
        val l_commitMean = mean(l_commitList);
        val l_commitSTDEV = stdev(l_commitList, l_commitMean);
        val l_abortMean = mean(l_abortList);
        val l_abortSTDEV = stdev(l_abortList, l_abortMean);
        
        var str:String = "";
        if (g_commitList.size() > 0)
            str += p + ":GLOBAL_TX:commitCount:"+g_commitList.size()+":commitMeanMS:"+g_commitMean+":commitSTDEV:"+ g_commitSTDEV+
                                 ":abortCount:" +g_abortList.size() +":abortMeanMS:" +g_abortMean +":abortSTDEV:" + g_abortSTDEV + 
                                 "\n:commitsList{"+listToString(g_commitList) +"}\n:abortsList{"+listToString(g_abortList) +"}"+"\n";
        
        if (l_commitList.size() > 0)
            str += p + ":LOCAL_TX:commitCount:"+l_commitList.size()+":commitMeanMS:"+l_commitMean+":commitSTDEV:"+ l_commitSTDEV+
                                 ":abortCount:" +l_abortList.size() +":abortMeanMS:" +l_abortMean +":abortSTDEV:" + l_abortSTDEV + 
                                 "\n:commitsList{"+listToString(l_commitList) +"}\n:abortsList{"+listToString(l_abortList) +"}"+"\n";
        
        return str;
    }
    
    public static def mean(values:ArrayList[Double]) {
        if (values.size() == 0)
            return 0.0;
        
        var sum:Double = 0;
        for (x in values)
            sum += x;
        return sum / values.size();
    }
    
    public static def stdev(values:ArrayList[Double], mean:Double) {
        if (values.size() == 0)
            return 0.0;
        
        var sum:Double = 0;
        for (x in values) {
            sum += Math.pow( x - mean , 2);
        }
        return Math.sqrt(sum / (values.size() -1) ); // divide by N-1 because this is just a sample, not the whole population
    }
    
    public static def listToString[T](r:ArrayList[T]):String {
        if (r == null)
            return "";
        var str:String = "";
        for (x in r)
            str += x + ",";
        return str;
    }
    
}

class TransactionsList {
    val globalTx:ArrayList[Tx];
    val localTx:ArrayList[LocalTx];
    private val listLock = new Lock();

    public def this(){
        globalTx = new ArrayList[Tx]();
        localTx = new ArrayList[LocalTx]();
    }
    
    public def addLocalTx(tx:LocalTx) {
        listLock.lock();
        localTx.add(tx);
        listLock.unlock();
    }
    public def addGlobalTx(tx:Tx) {
        listLock.lock();
        globalTx.add(tx);
        listLock.unlock();
    }
}