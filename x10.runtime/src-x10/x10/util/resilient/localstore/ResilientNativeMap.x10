package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
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
        list().localTx.add(tx);
        return tx;
    }
    
    
    public def startGlobalTransaction(members:PlaceGroup):Tx {
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
        list().globalTx.add(tx);
        return tx;
    }
    
    public def executeTransaction(members:PlaceGroup, closure:(Tx)=>void):Int {
        do {
            try {
                val tx = startGlobalTransaction(members);
                closure(tx);
                return tx.commit();
            } catch(ex:Exception) {
                processException(ex);
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
		                processException(ex);
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
    
    private static def processException(ex:Exception) {
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
        list().globalTx.add(tx);
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
                var g_cSum:Double = 0;
                var g_cCnt:Long = 0;
                var g_aSum:Double = 0;
                var g_aCnt:Long = 0;
                for (tx in list().globalTx) {
                    if (tx.commitTime != -1) {
                        g_cSum += (tx.commitTime - tx.startTime);
                        g_cCnt++;
                    }
                    else if (tx.abortTime != -1) {
                        g_aSum += (tx.abortTime - tx.startTime);
                        g_aCnt++;
                    }
                }
                var g_avgCommitTimeNS:Double = -1.0;
                if (g_cCnt > 0)
                    g_avgCommitTimeNS = (g_cSum/g_cCnt);
                
                var g_avgAbortTimeNS:Double = -1.0;
                if (g_aCnt > 0)
                    g_avgAbortTimeNS = (g_aSum/g_aCnt);
                
                //####         Local TX         ####//
                var l_cSum:Double = 0;
                var l_cCnt:Long = 0;
                var l_aSum:Double = 0;
                var l_aCnt:Long = 0;
                for (tx in list().localTx) {
                    if (tx.commitTime != -1) {
                        l_cSum += (tx.commitTime - tx.startTime);
                        l_cCnt++;
                    }
                    else if (tx.abortTime != -1) {
                        l_aSum += (tx.abortTime - tx.startTime);
                        l_aCnt++;
                    }
                }
                var l_avgCommitTimeNS:Double = -1.0;
                if (l_cCnt > 0)
                    l_avgCommitTimeNS = (l_cSum/l_cCnt);
                
                var l_avgAbortTimeNS:Double = -1.0;
                if (l_aCnt > 0)
                    l_avgAbortTimeNS = (l_aSum/l_aCnt);
                
                new TxPlaceStatistics(here, g_cCnt, g_avgCommitTimeNS, g_aCnt, g_avgAbortTimeNS,
                		                    l_cCnt, l_avgCommitTimeNS, l_aCnt, l_avgAbortTimeNS)
            };
            pl_stat.add(pstat);
        }
        
        var g_cCntTotal:Double = 0.0;
        var g_cAvgTimeTotal:Double = 0;
        var g_cCntPlaces:Long = 0;
        var g_aCntTotal:Double = 0.0;
        var g_aAvgTimeTotal:Double = 0;
        var g_aCntPlaces:Long = 0;
        
        var l_cCntTotal:Double = 0.0;
        var l_cAvgTimeTotal:Double = 0;
        var l_cCntPlaces:Long = 0;
        var l_aCntTotal:Double = 0.0;
        var l_aAvgTimeTotal:Double = 0;
        var l_aCntPlaces:Long = 0;
        
        for (pstat in pl_stat) {
            Console.OUT.println(pstat);
            g_cCntTotal += pstat.g_commitCount;
            g_aCntTotal += pstat.g_abortCount;
            if (pstat.g_commitCount > 0) {
                g_cAvgTimeTotal += pstat.g_avgCommitTimeNS;
                g_cCntPlaces++;
            }
            if (pstat.g_abortCount > 0) {
                g_aAvgTimeTotal += pstat.g_avgAbortTimeNS;
                g_aCntPlaces++;
            }
            
            l_cCntTotal += pstat.l_commitCount;
            l_aCntTotal += pstat.l_abortCount;
            if (pstat.l_commitCount > 0) {
                l_cAvgTimeTotal += pstat.l_avgCommitTimeNS;
                l_cCntPlaces++;
            }
            if (pstat.l_abortCount > 0) {
                l_aAvgTimeTotal += pstat.l_avgAbortTimeNS;
                l_aCntPlaces++;
            }
        }
        
        val g_cPerPlace = g_cCntTotal / pl_stat.size();
        val g_aPerPlace = g_aCntTotal / pl_stat.size();
        
        var g_cAvg:Double = 0.0;
        if (g_cCntPlaces > 0)
            g_cAvg = g_cAvgTimeTotal / g_cCntPlaces;
        
        var g_aAvg:Double = 0.0;
        if (g_aCntPlaces > 0)
            g_aAvg = g_aAvgTimeTotal / g_aCntPlaces;
        
        val l_cPerPlace = l_cCntTotal / pl_stat.size();
        val l_aPerPlace = l_aCntTotal / pl_stat.size();
        
        var l_cAvg:Double = 0.0;
        if (l_cCntPlaces > 0)
            l_cAvg = l_cAvgTimeTotal / l_cCntPlaces;
        
        var l_aAvg:Double = 0.0;
        if (l_aCntPlaces > 0)
            l_aAvg = l_aAvgTimeTotal / l_aCntPlaces;
        
        Console.OUT.println("Summary:GLOBAL_TX:committed:"+g_cCntTotal+":commitsPerPlace:"+g_cPerPlace+":commitAvgTimeMS:"+(g_cAvg/1e6)+":committedPlaces:"+g_cCntPlaces
                                  +":aborted:" +g_aCntTotal+":abortsPerPlace:" +g_aPerPlace+":abortAvgTimeMS:" +(g_aAvg/1e6)+":abortedPlaces:"+g_aCntPlaces);
        Console.OUT.println("Summary:LOCAL_TX:committed:"+l_cCntTotal+":commitsPerPlace:"+l_cPerPlace+":commitAvgTimeMS:"+(l_cAvg/1e6)+":committedPlaces:"+l_cCntPlaces
                                  +":aborted:" +l_aCntTotal+":abortsPerPlace:" +l_aPerPlace+":abortAvgTimeMS:" +(l_aAvg/1e6)+":abortedPlaces:"+l_aCntPlaces);
    }
    
    public def resetTxStatistics() {
        finish for (p in store.activePlaces) at (p) async {
            store.plh().masterStore.resetState(name);
            list().globalTx.clear();
            list().localTx.clear();
        }
    }
    
}

class TxPlaceStatistics(p:Place, g_commitCount:Long, g_avgCommitTimeNS:Double, g_abortCount:Long, g_avgAbortTimeNS:Double
		                       , l_commitCount:Long, l_avgCommitTimeNS:Double, l_abortCount:Long, l_avgAbortTimeNS:Double) {
    public def toString() {
        return p + ":GLOBAL_TX:commitCount:"+g_commitCount+":avgCommitTimeNS:"+g_avgCommitTimeNS+":abortCount:"+g_abortCount+":avgAbortTimeNS:"+g_avgAbortTimeNS + "\n" +
        	   p + ":LOCAL_TX:commitCount:"+l_commitCount+":avgCommitTimeNS:"+l_avgCommitTimeNS+":abortCount:"+l_abortCount+":avgAbortTimeNS:"+l_avgAbortTimeNS ;
    }
}

class TransactionsList {
	val globalTx:ArrayList[Tx];
    val localTx:ArrayList[LocalTx];

	public def this(){
		globalTx = new ArrayList[Tx]();
		localTx = new ArrayList[LocalTx]();
	}
}