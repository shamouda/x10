package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;

public class ResilientNativeMap (name:String, store:ResilientStore) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public val plh:PlaceLocalHandle[ArrayList[Tx]];
    
    public def this(name:String, store:ResilientStore, plh:PlaceLocalHandle[ArrayList[Tx]]) {
        property(name, store);
        this.plh = plh;
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
        return  new LocalTx(store.plh, id, name);
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
        plh().add(tx);
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
	        do {
	            try {
	            	val tx = startLocalTransaction();
	            	closure(tx);
	            	result = tx.commit();
	            	break;
	            } catch(ex:Exception) {
	                processException(ex);
	            }
	            System.threadSleep(0);
	        }while(true);
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
        plh().add(tx);
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
        val list = new ArrayList[TxPlaceStatistics]();
        for (p in store.activePlaces) {
            val pstat = at (p) {
                var cSum:Double = 0;
                var cCnt:Long = 0;
                var aSum:Double = 0;
                var aCnt:Long = 0;
                for (tx in plh()) {
                    if (tx.commitTime != -1) {
                        cSum += (tx.commitTime - tx.startTime);
                        cCnt++;
                    }
                    else if (tx.abortTime != -1) {
                        aSum += (tx.abortTime - tx.startTime);
                        aCnt++;
                    }
                }
                var avgCommitTimeNS:Double = -1.0;
                if (cCnt > 0)
                    avgCommitTimeNS = (cSum/cCnt);
                
                var avgAbortTimeNS:Double = -1.0;
                if (aCnt > 0)
                    avgAbortTimeNS = (aSum/aCnt);
                
                new TxPlaceStatistics(here, cCnt, avgCommitTimeNS, aCnt, avgAbortTimeNS)
            };
            list.add(pstat);
        }
        
        var cCntTotal:Double = 0.0;
        var cAvgTimeTotal:Double = 0;
        var cCntPlaces:Long = 0;
        
        var aCntTotal:Double = 0.0;
        var aAvgTimeTotal:Double = 0;
        var aCntPlaces:Long = 0;
        
        
        for (pstat in list) {
            Console.OUT.println(pstat);
            
            cCntTotal += pstat.commitCount;
            aCntTotal += pstat.abortCount;
            
            if (pstat.commitCount > 0) {
                cAvgTimeTotal += pstat.avgCommitTimeNS;
                cCntPlaces++;
            }
            
            if (pstat.abortCount > 0) {
                aAvgTimeTotal += pstat.avgAbortTimeNS;
                aCntPlaces++;
            }
        }
        
        val cPerPlace = cCntTotal / list.size();
        val aPerPlace = aCntTotal / list.size();
        
        var cAvg:Double = -1.0;
        if (cCntPlaces > 0)
            cAvg = cAvgTimeTotal / cCntPlaces;
        
        var aAvg:Double = -1.0;
        if (aCntPlaces > 0)
            aAvg = aAvgTimeTotal / aCntPlaces;
        
        Console.OUT.println("Summary:totalCommitedTxs:"+cCntTotal+":commitsPerPlace:"+cPerPlace+":globalCommitAvgTimeMS:"+(cAvg/1e6)+":committedPlaces:"+cCntPlaces
                                  +":totalAbortedTxs:" +aCntTotal+":abortsPerPlace:" +aPerPlace+":globalAbortAvgTimeMS:" +(aAvg/1e6)+":abortedPlaces:"+aCntPlaces);
    }
    
    public def resetTxStatistics() {
        finish for (p in store.activePlaces) at (p) async {
            store.plh().masterStore.resetState(name);
            plh().clear();
        }
    }
    
}

class TxPlaceStatistics(p:Place, commitCount:Long, avgCommitTimeNS:Double, abortCount:Long, avgAbortTimeNS:Double) {
    public def toString() {
        return p + ":commitCount:"+commitCount+":avgCommitTimeNanoSec:"+avgCommitTimeNS+":abortCount:"+abortCount+":avgAbortTimeNanoSec:"+avgAbortTimeNS;
    }
}