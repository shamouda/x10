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
                var g_cSum:Double = 0;
                var g_cCnt:Long = 0;
                var g_aSum:Double = 0;
                var g_aCnt:Long = 0;
                for (tx in list().globalTx) {
                    if (tx.commitTime != -1) {
                    	val cTime = tx.commitTime - tx.startTime;
                        g_cSum += cTime;
                        g_cCnt++;
                        g_commitList.add(cTime);
                    }
                    else if (tx.abortTime != -1) {
                    	val aTime = tx.abortTime - tx.startTime;
                        g_aSum += aTime;
                        g_aCnt++;
                        g_abortList.add(aTime);
                    }
                }
                var g_commitMeanNS:Double = -1.0;
                if (g_cCnt > 0)
                    g_commitMeanNS = (g_cSum/g_cCnt);
                
                var g_abortMeanNS:Double = -1.0;
                if (g_aCnt > 0)
                    g_abortMeanNS = (g_aSum/g_aCnt);

                val g_commitSTDEV = calStdDev(g_commitList, g_commitMeanNS);
                val g_abortSTDEV  = calStdDev(g_abortList, g_abortMeanNS);
                                
                //####         Local TX         ####//
                val l_commitList = new ArrayList[Double]();
            	val l_abortList = new ArrayList[Double]();
                var l_cSum:Double = 0;
                var l_cCnt:Long = 0;
                var l_aSum:Double = 0;
                var l_aCnt:Long = 0;
                for (tx in list().localTx) {
                    if (tx.commitTime != -1) {
                    	val cTime = tx.commitTime - tx.startTime; 
                        l_cSum += cTime;
                        l_cCnt++;
                        l_commitList.add(cTime);
                    }
                    else if (tx.abortTime != -1) {
                    	val aTime = tx.abortTime - tx.startTime; 
                        l_aSum += aTime;
                        l_aCnt++;
                        l_abortList.add(aTime);
                    }
                }
                var l_commitMeanNS:Double = -1.0;
                if (l_cCnt > 0)
                    l_commitMeanNS = (l_cSum/l_cCnt);
                
                var l_abortMeanNS:Double = -1.0;
                if (l_aCnt > 0)
                    l_abortMeanNS = (l_aSum/l_aCnt);

                val l_commitSTDEV = calStdDev(l_commitList, l_commitMeanNS);
                val l_abortSTDEV  = calStdDev(l_abortList, l_abortMeanNS);
                
                new TxPlaceStatistics(here, g_cCnt, g_commitMeanNS, g_commitSTDEV, g_aCnt, g_abortMeanNS, g_abortSTDEV,
                		                    l_cCnt, l_commitMeanNS, l_commitSTDEV, l_aCnt, l_abortMeanNS, l_abortSTDEV)
            };
            pl_stat.add(pstat);
        }
        
        var g_cCnt:Double = 0.0;
        var g_cAvgTimeTotal:Double = 0;
        var g_cCntPlaces:Long = 0;
        var g_aCnt:Double = 0.0;
        var g_aAvgTimeTotal:Double = 0;
        var g_aCntPlaces:Long = 0;
        
        var l_cCnt:Double = 0.0;
        var l_cAvgTimeTotal:Double = 0;
        var l_cCntPlaces:Long = 0;
        var l_aCnt:Double = 0.0;
        var l_aAvgTimeTotal:Double = 0;
        var l_aCntPlaces:Long = 0;
        
        for (pstat in pl_stat) {
            Console.OUT.println(pstat);
            g_cCnt += pstat.g_commitCount;
            g_aCnt += pstat.g_abortCount;
            if (pstat.g_commitCount > 0) {
                g_cAvgTimeTotal += pstat.g_commitMeanNS;
                g_cCntPlaces++;
            }
            if (pstat.g_abortCount > 0) {
                g_aAvgTimeTotal += pstat.g_abortMeanNS;
                g_aCntPlaces++;
            }
            
            l_cCnt += pstat.l_commitCount;
            l_aCnt += pstat.l_abortCount;
            if (pstat.l_commitCount > 0) {
                l_cAvgTimeTotal += pstat.l_commitMeanNS;
                l_cCntPlaces++;
            }
            if (pstat.l_abortCount > 0) {
                l_aAvgTimeTotal += pstat.l_abortMeanNS;
                l_aCntPlaces++;
            }
        }
        
        val g_cPerPlace = g_cCnt / pl_stat.size();
        val g_aPerPlace = g_aCnt / pl_stat.size();
        
        var g_cAvg:Double = 0.0;
        if (g_cCntPlaces > 0)
            g_cAvg = g_cAvgTimeTotal / g_cCntPlaces;
        
        var g_aAvg:Double = 0.0;
        if (g_aCntPlaces > 0)
            g_aAvg = g_aAvgTimeTotal / g_aCntPlaces;
        
        val l_cPerPlace = l_cCnt / pl_stat.size();
        val l_aPerPlace = l_aCnt / pl_stat.size();
        
        var l_cAvg:Double = 0.0;
        if (l_cCntPlaces > 0)
            l_cAvg = l_cAvgTimeTotal / l_cCntPlaces;
        
        var l_aAvg:Double = 0.0;
        if (l_aCntPlaces > 0)
            l_aAvg = l_aAvgTimeTotal / l_aCntPlaces;
        
        Console.OUT.println("Summary:GLOBAL_TX:committed:"+ g_cCnt + ":commitsPerPlace:" + g_cPerPlace + ":commitAvgTimeMS:" + (g_cAvg/1e6) + ":committedPlaces:" + g_cCntPlaces
                                            +":aborted:"  + g_aCnt + ":abortsPerPlace:"  + g_aPerPlace + ":abortAvgTimeMS:"  + (g_aAvg/1e6) + ":abortedPlaces:"   + g_aCntPlaces);
        Console.OUT.println("Summary:LOCAL_TX:committed:" + l_cCnt + ":commitsPerPlace:" + l_cPerPlace + ":commitAvgTimeMS:" + (l_cAvg/1e6) + ":committedPlaces:" + l_cCntPlaces
                                            +":aborted:"  + l_aCnt + ":abortsPerPlace:"  + l_aPerPlace + ":abortAvgTimeMS:"  + (l_aAvg/1e6) + ":abortedPlaces:"   + l_aCntPlaces);
    }
    
    public def resetTxStatistics() {
        finish for (p in store.activePlaces) at (p) async {
            store.plh().masterStore.resetState(name);
            list().globalTx.clear();
            list().localTx.clear();
        }
    }
    
    
    private static def calStdDev(values:ArrayList[Double], mean:Double) {
    	var sum:Double = 0;
        for (x in values) {
        	sum += Math.pow( x - mean , 2);
        }
        return Math.sqrt(sum / values.size());
    }
    
}

class TxPlaceStatistics(p:Place, g_commitCount:Long, g_commitMeanNS:Double, g_commitSTDEV:Double, g_abortCount:Long, g_abortMeanNS:Double, g_abortSTDEV:Double
		                       , l_commitCount:Long, l_commitMeanNS:Double, l_commitSTDEV:Double, l_abortCount:Long, l_abortMeanNS:Double, l_abortSTDEV:Double) {
    public def toString() {
        var str:String = "";
    	if (g_commitCount > 0)
    		str += p + ":GLOBAL_TX:commitCount:"+ g_commitCount +":commitMean(ns):" + g_commitMeanNS + ":commitSTDEV:" + g_commitSTDEV
        		                +":abortCount:" + g_abortCount  +":abortMean(ns):"  + g_abortMeanNS  + ":abortSTDEV:"  + g_abortSTDEV + "\n" ;
    	if (l_commitCount > 0)
    		str += p + ":LOCAL_TX:commitCount:" + l_commitCount +":commitMean(ns):" + l_commitMeanNS + ":commitSTDEV:" + l_commitSTDEV
        	               +":abortCount:"  + l_abortCount  +":abortMean(ns):"  + l_abortMeanNS  + ":abortSTDEV:"  + l_abortSTDEV ;
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