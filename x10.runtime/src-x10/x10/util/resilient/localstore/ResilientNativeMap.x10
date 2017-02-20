package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;
import x10.util.Timer;

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
            var commitCalled:Boolean = false;
            val start = Timer.milliTime();
            try {
                finish closure(tx);
                tx.setPreCommitTime(Timer.milliTime()-start);
                
                if (TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} succeeded  preCommitTime["+tx.preCommitTime+"] ms");
                commitCalled = true;
                return tx.commit();
            } catch(ex:Exception) {                
                if (!commitCalled) {
                	tx.setPreCommitTime(Timer.milliTime()-start);
                    tx.abort(excpt); // tx.commit() aborts automatically if needed
                }
                
                if (TM_DEBUG) {
                    Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} failed with Error ["+ex.getMessage()+"] commitCalled["+commitCalled+"] preCommitTime["+tx.preCommitTime+"] ms");
                    ex.printStackTrace();
                }
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
                	val tx = startLocalTransaction();
                	var commitCalled:Boolean = false;
                	val start = Timer.milliTime();
                    try {
                        closure(tx);
                        tx.setPreCommitTime(Timer.milliTime()-start);
                        commitCalled = true;
                        result = tx.commit();
                        break;
                    } catch(ex:Exception) {
                    	if (!commitCalled) {
                        	tx.setPreCommitTime(Timer.milliTime()-start);
                            //no need to call abort, abort occurs automatically in local tx all the time
                        }
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
        Console.OUT.println("Calculating execution statistics ...");
        val pl_stat = new ArrayList[TxPlaceStatistics]();
        for (p in store.activePlaces) {
            val pstat = at (p) {
                //####         Global TX         ####//
                val g_commitList = new ArrayList[Double]();
                val g_preCommitList = new ArrayList[Double]();
                
                val g_abortList = new ArrayList[Double]();
                val g_preAbortList = new ArrayList[Double]();
                
                for (tx in list().globalTx) {
                    if (tx.commitTime != -1) {
                        g_commitList.add(tx.commitTime - tx.startTime);
                        g_preCommitList.add(tx.preCommitTime);
                    }
                    else if (tx.abortTime != -1) {
                        g_abortList.add(tx.abortTime - tx.startTime);
                        g_preAbortList.add(tx.preCommitTime);
                    }
                }
                //####         Local TX         ####//
                val l_commitList = new ArrayList[Double]();
                val l_preCommitList = new ArrayList[Double]();
                
                val l_abortList = new ArrayList[Double]();
                val l_preAbortList = new ArrayList[Double]();
                
                for (tx in list().localTx) {
                    if (tx.commitTime != -1) {
                        l_commitList.add(tx.commitTime - tx.startTime);
                        l_preCommitList.add(tx.preCommitTime);
                    }
                    else if (tx.abortTime != -1) {
                        l_abortList.add(tx.abortTime - tx.startTime);
                        l_preAbortList.add(tx.preCommitTime);
                    }
                }
                 
                new TxPlaceStatistics(here, g_commitList, g_preCommitList, g_abortList, g_preAbortList, 
                		                    l_commitList, l_preCommitList, l_abortList, l_preAbortList)
            };
            pl_stat.add(pstat);
        }
        
        val g_allCommitList = new ArrayList[Double]();
        val g_allPreCommitList = new ArrayList[Double]();
        val g_allAbortList = new ArrayList[Double]();
        val g_allPreAbortList = new ArrayList[Double]();
        var g_cPlaces:Long = 0;
        var g_aPlaces:Long = 0;
        
        val l_allCommitList = new ArrayList[Double]();
        val l_allPreCommitList = new ArrayList[Double]();
        val l_allAbortList = new ArrayList[Double]();
        val l_allPreAbortList = new ArrayList[Double]();
        var l_cPlaces:Long = 0;
        var l_aPlaces:Long = 0;
        
        for (pstat in pl_stat) {
            val str = pstat.toString();
            if (!str.equals(""))
                Console.OUT.println(str);
            g_allCommitList.addAll(pstat.g_commitList);
            g_allPreCommitList.addAll(pstat.g_preCommitList);
            if (pstat.g_commitList.size() > 0)
                g_cPlaces ++;
            
            g_allAbortList.addAll(pstat.g_abortList);
            g_allPreAbortList.addAll(pstat.g_preAbortList);
            if (pstat.g_abortList.size() > 0)
                g_aPlaces ++;
            
            l_allCommitList.addAll(pstat.l_commitList);
            l_allPreCommitList.addAll(pstat.l_preCommitList);
            if (pstat.l_commitList.size() > 0)
                l_cPlaces ++;
            
            l_allAbortList.addAll(pstat.l_abortList);
            l_allPreAbortList.addAll(pstat.l_preAbortList);
            if (pstat.l_abortList.size() > 0)
                l_aPlaces ++;
        }
        
        val g_cCnt   = g_allCommitList.size();
        val g_cMean  = TxStatistics.mean(g_allCommitList);
        val g_cSTDEV = TxStatistics.stdev(g_allCommitList, g_cMean);
        val g_cBox   = TxStatistics.boxPlot(g_allCommitList);
        val g_cPreMean  = TxStatistics.mean(g_allPreCommitList);
        val g_cPreSTDEV = TxStatistics.stdev(g_allPreCommitList, g_cPreMean);
        val g_cPreBox   = TxStatistics.boxPlot(g_allPreCommitList);
        
        val g_aCnt   = g_allAbortList.size();
        val g_aMean  = TxStatistics.mean(g_allAbortList);
        val g_aSTDEV = TxStatistics.stdev(g_allAbortList, g_aMean);
        val g_aBox   = TxStatistics.boxPlot(g_allAbortList);
        val g_aPreMean  = TxStatistics.mean(g_allPreAbortList);
        val g_aPreSTDEV = TxStatistics.stdev(g_allPreAbortList, g_aPreMean);
        val g_aPreBox   = TxStatistics.boxPlot(g_allPreAbortList);
        
        val l_cCnt   = l_allCommitList.size();
        val l_cMean  = TxStatistics.mean(l_allCommitList);
        val l_cSTDEV = TxStatistics.stdev(l_allCommitList, l_cMean);
        val l_cBox   = TxStatistics.boxPlot(l_allCommitList);
        val l_cPreMean  = TxStatistics.mean(l_allPreCommitList);
        val l_cPreSTDEV = TxStatistics.stdev(l_allPreCommitList, l_cPreMean);
        val l_cPreBox   = TxStatistics.boxPlot(l_allPreCommitList);
        
        val l_aCnt   = l_allAbortList.size();
        val l_aMean  = TxStatistics.mean(l_allAbortList);
        val l_aSTDEV = TxStatistics.stdev(l_allAbortList, l_aMean);
        val l_aBox   = TxStatistics.boxPlot(l_allAbortList);
        val l_aPreMean  = TxStatistics.mean(l_allPreAbortList);
        val l_aPreSTDEV = TxStatistics.stdev(l_allPreAbortList, l_aPreMean);
        val l_aPreBox   = TxStatistics.boxPlot(l_allPreAbortList);
        
        
        if (g_cCnt > 0) {
            Console.OUT.println("Summary:GLOBAL_TX:committedTxs:"+ g_cCnt + ":committedPlaces:" + g_cPlaces + ":commitMeanMS:" + g_cMean + ":commitSTDEV:" + g_cSTDEV + ":commitBox:(:" + g_cBox + ":)" + ":preCommitMeanMS:" + g_cPreMean + ":preCommitSTDEV:" + g_cPreSTDEV + ":preCommitBox:(:" + g_cPreBox + ":)");
            Console.OUT.println("Summary:GLOBAL_TX:abortedTxs:"  + g_aCnt + ":abortedPlaces:"   + g_aPlaces + ":abortMeanMS:"  + g_aMean + ":abortSTDEV:"  + g_aSTDEV + ":abortBox:(:"  + g_aBox + ":)" + ":preAbortMeanMS:"  + g_aPreMean + ":preAbortSTDEV:"  + g_aPreSTDEV + ":preAbortBox:(:"  + g_aPreBox + ":)" );
        }
        
        if (l_cCnt > 0) {
        	Console.OUT.println("Summary:LOCAL_TX:committedTxs:"+ l_cCnt + ":committedPlaces:" + l_cPlaces + ":commitMeanMS:" + l_cMean + ":commitSTDEV:" + l_cSTDEV + ":commitBox:(:" + l_cBox + ":)" + ":preCommitMeanMS:" + l_cPreMean + ":preCommitSTDEV:" + l_cPreSTDEV + ":preCommitBox:(:" + l_cPreBox + ":)");
            Console.OUT.println("Summary:LOCAL_TX:abortedTxs:"  + l_aCnt + ":abortedPlaces:"   + l_aPlaces + ":abortMeanMS:"  + l_aMean + ":abortSTDEV:"  + l_aSTDEV + ":abortBox:(:"  + l_aBox + ":)" + ":preAbortMeanMS:"  + l_aPreMean + ":preAbortSTDEV:"  + l_aPreSTDEV + ":preAbortBox:(:"  + l_aPreBox + ":)" );
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

class TxPlaceStatistics(p:Place, g_commitList:ArrayList[Double], g_preCommitList:ArrayList[Double], g_abortList:ArrayList[Double], g_preAbortList:ArrayList[Double], 
		                         l_commitList:ArrayList[Double], l_preCommitList:ArrayList[Double], l_abortList:ArrayList[Double], l_preAbortList:ArrayList[Double]) {
    public def toString() {
        
        val g_commitMean     = TxStatistics.mean(g_commitList);
        val g_commitSTDEV    = TxStatistics.stdev(g_commitList, g_commitMean);
        val g_commitBox      = TxStatistics.boxPlot(g_commitList);        
        val g_preCommitMean  = TxStatistics.mean(g_preCommitList);
        val g_preCommitSTDEV = TxStatistics.stdev(g_preCommitList, g_preCommitMean);
        val g_preCommitBox   = TxStatistics.boxPlot(g_preCommitList);
        
        val g_abortMean     = TxStatistics.mean(g_abortList);
        val g_abortSTDEV    = TxStatistics.stdev(g_abortList, g_abortMean);
        val g_abortBox      = TxStatistics.boxPlot(g_abortList);
        val g_preAbortMean  = TxStatistics.mean(g_preAbortList);
        val g_preAbortSTDEV = TxStatistics.stdev(g_preAbortList, g_preAbortMean);
        val g_preAbortBox   = TxStatistics.boxPlot(g_preAbortList);
        
        val l_commitMean     = TxStatistics.mean(l_commitList);
        val l_commitSTDEV    = TxStatistics.stdev(l_commitList, l_commitMean);
        val l_commitBox      = TxStatistics.boxPlot(l_commitList);        
        val l_preCommitMean  = TxStatistics.mean(l_preCommitList);
        val l_preCommitSTDEV = TxStatistics.stdev(l_preCommitList, l_preCommitMean);
        val l_preCommitBox   = TxStatistics.boxPlot(l_preCommitList);
        
        val l_abortMean     = TxStatistics.mean(l_abortList);
        val l_abortSTDEV    = TxStatistics.stdev(l_abortList, l_abortMean);
        val l_abortBox      = TxStatistics.boxPlot(l_abortList);
        val l_preAbortMean  = TxStatistics.mean(l_preAbortList);
        val l_preAbortSTDEV = TxStatistics.stdev(l_preAbortList, l_preAbortMean);
        val l_preAbortBox   = TxStatistics.boxPlot(l_preAbortList);
        
        var str:String = "";
        if (g_commitList.size() > 0)
            str += p + ":GLOBAL_TX:commitCount:"+ g_commitList.size() + ":commitMeanMS:"    + g_commitMean    + ":commitSTDEV:"    + g_commitSTDEV    + ":commitBox:(:" + g_commitBox + ":)" + ":preCommitMeanMS:" + g_preCommitMean + ":preCommitSTDEV:" + g_preCommitSTDEV + ":preCommitBox:(:" + g_preCommitBox + ":)" + 
                                 ":abortCount:" + g_abortList.size()  + ":abortMeanMS:"     + g_abortMean     + ":abortSTDEV:"     + g_abortSTDEV     + ":abortBox:(:"  + g_abortBox  + ":)" + ":preAbortMeanMS:"  + g_preAbortMean  + ":preAbortSTDEV:"  + g_preAbortSTDEV  + ":preAbortBox:(:"  + g_preAbortBox + ":)\n";
        
        if (l_commitList.size() > 0)
            str += p + ":LOCAL_TX:commitCount:"+ l_commitList.size() + ":commitMeanMS:"    + l_commitMean    + ":commitSTDEV:"    + l_commitSTDEV    + ":commitBox:(:" + l_commitBox + ":)" + ":preCommitMeanMS:" + l_preCommitMean + ":preCommitSTDEV:" + l_preCommitSTDEV + ":preCommitBox:(:" + l_preCommitBox + ":)" + 
  							     ":abortCount:" + l_abortList.size()  + ":abortMeanMS:"     + l_abortMean     + ":abortSTDEV:"     + l_abortSTDEV     + ":abortBox:(:" + l_abortBox + ":)"   + ":preAbortMeanMS:"  + l_preAbortMean  + ":preAbortSTDEV:"  + l_preAbortSTDEV  + ":preAbortBox:(:"  + l_preAbortBox + ":)" ;

        
        return str;
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