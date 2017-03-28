package x10.util.resilient.localstore;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.tx.TxDesc;

public class DistributedRecoveryHelper {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def recover(plh:PlaceLocalHandle[LocalStore], deadSlave:Place, oldActivePlaces:PlaceGroup):void {
    	try {
	    	plh().lock();
	    	
	    	val deadPlaceVirtualPlaceId = oldActivePlaces.indexOf(deadSlave);
	    	val spare = allocateSparePlace(plh, deadPlaceVirtualPlaceId, oldActivePlaces);
	    	val newActivePlaces = generateNewActivePlaces(deadSlave, spare, oldActivePlaces);
	    	val masterOfDeadSlave = oldActivePlaces.prev(deadSlave);
	    	
	        recoverTransactions(plh, spare, oldActivePlaces);
	        
	        recoverMasters(plh, spare, newActivePlaces);
	        
	        recoverSlaves(plh, spare, masterOfDeadSlave);
	        
	        val otherPlaces = excludePlaces(newActivePlaces, here, spare);
	        otherPlaces.broadcastFlat(()=> {plh().replace(deadSlave, spare);} );
	        
	        plh().activePlaces = newActivePlaces;
    	} finally {
    		plh().unlock();
    	}
    }
    
    private static def allocateSparePlace(plh:PlaceLocalHandle[LocalStore], deadPlaceVirtualPlaceId:Long, oldActivePlaces:PlaceGroup) {
        try {
            plh().lock();
            val nPlaces = Place.numAllPlaces();
            val nActive = plh().activePlaces.size();
            var placeIndx:Long = -1;
            for (var i:Long = nActive; i < nPlaces; i++) {
                if (oldActivePlaces.contains(Place(i)))
                    continue;
                
                var allocated:Boolean = false;
                try {
                    allocated = at (Place(i)) allocate(plh, deadPlaceVirtualPlaceId);
                }catch(ex:Exception) {
                }
                
                if (!allocated)
                    Console.OUT.println(here + " - Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
                else {
                    Console.OUT.println(here + " - Succeeded to allocate " + Place(i) );
                    placeIndx = i;
                    break;
                }
            }
            assert(placeIndx != -1) : here + " no available spare places to allocate ";
            return Place(placeIndx);
        }
        finally {
            plh().unlock();
        }   
    }
    
    
    private static def allocate(plh:PlaceLocalHandle[LocalStore], vPlace:Long) {
        try {
            plh().lock();
            Console.OUT.println(here + " received allocation request to replace virtual place ["+vPlace+"] ");
            if (plh().virtualPlaceId == -1) {
                plh().virtualPlaceId = vPlace;
                Console.OUT.println(here + " allocation request succeeded");
                return true;
            }
            Console.OUT.println(here + " allocation request failed, already allocated for virtual place ["+plh().virtualPlaceId+"] ");
            return false;
        }
        finally {
            plh().unlock();
        }
    }
    
    private static def generateNewActivePlaces(deadPlace:Place, newPlace:Place, oldActivePlaces:PlaceGroup) {
        val rail = new Rail[Place]();
        for (var i:Long = 0; i < oldActivePlaces.size(); i++) {
            if (oldActivePlaces(i).id == deadPlace.id)
                rail(i) = newPlace;
        }
        return new SparsePlaceGroup(rail);
    }
    
    
    private static def recoverTransactions(plh:PlaceLocalHandle[LocalStore], spare:Place,oldActivePlaces:PlaceGroup) {
        Console.OUT.println(here + " - recoverTransactions started");
        finish {
            Console.OUT.println(here + " - recoverTransactions deadPlace["+plh().slaveStore.master+"] moving to its slave["+here+"] ");
            val txDescMap = plh().slaveStore.getSlaveMasterState();
            if (txDescMap != null) {
                val set = txDescMap.keySet();
                val iter = set.iterator();
                while (iter.hasNext()) {
                    val txId = iter.next();
                    if (txId.contains("_TxDesc_")) {
                        val obj = txDescMap.get(txId);
                        if (obj != null) {
                            val txDesc = obj as TxDesc;
                            val map = new ResilientNativeMap(txDesc.mapName, plh);
                            if (TM_DEBUG) Console.OUT.println(here + " - recovering txdesc " + txDesc);
                            val tx = map.restartGlobalTransaction(txDesc);
                            if (txDesc.status == TxDesc.COMMITTING) {
                                if (TM_DEBUG) Console.OUT.println(here + " - recovering Tx["+tx.id+"] commit it");
                                tx.commit(true); //ignore phase one
                            }
                            else if (txDesc.status == TxDesc.STARTED) {
                                if (TM_DEBUG) Console.OUT.println(here + " - recovering Tx["+tx.id+"] abort it");
                                tx.abort();
                            }
                        }
                    }
                }
            }
            
            if (TxConfig.getInstance().TM_REP.equals("lazy"))
                applySlaveTransactions(plh, oldActivePlaces);
        }
        Console.OUT.println("recoverTransactions completed");
    }
    
    private static def recoverMasters(plh:PlaceLocalHandle[LocalStore], spare:Place, newActivePlaces:PlaceGroup) {
        finish {
            val map = plh().slaveStore.getSlaveMasterState();
            at (spare) async {
                plh().joinAsMaster(newActivePlaces, map);
            }
        }
    }

    private static def recoverSlaves(plh:PlaceLocalHandle[LocalStore], spare:Place, masterOfDeadSlave:Place) {
        finish {
            at (masterOfDeadSlave) async {
                val masterState = plh().masterStore.getState().getKeyValueMap();
                at (spare) {
                    plh().slaveStore.addMasterPlace(masterState);
                }
                plh().slave = spare;
            }
        }
    }

    private static def applySlaveTransactions(plh:PlaceLocalHandle[LocalStore], oldActivePlaces:PlaceGroup) {
        val committed = GlobalRef(new ArrayList[Long]());
        val committedLock = GlobalRef(new Lock());
        val root = here;
        
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            val placeTxsMap = plh().slaveStore.clusterTransactions();
            finish {
                val iter = placeTxsMap.keySet().iterator();
                while (iter.hasNext()) {
                    val placeIndex = iter.next();
                    val txList = placeTxsMap.getOrThrow(placeIndex);
                    var pl:Place = oldActivePlaces(placeIndex);
                    var master:Boolean = true;
                    if (pl.isDead()){
                        pl = oldActivePlaces.next(pl);
                        master = false;
                    }
                    val isMaster = master;
                    at (pl) async {
                        var committedList:ArrayList[Long]; 
                        if (isMaster)
                            committedList = plh().masterStore.filterCommitted(txList);
                        else
                            committedList = plh().slaveStore.filterCommitted(txList);
                        
                        val cList = committedList;
                        at (root) {
                            committedLock().lock();
                            committed().addAll(cList);
                            committedLock().unlock();
                        }
                    }
                }
            }
        }
        
        val orderedTx = plh().slaveStore.getPendingTransactions();
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            val commitTxOrdered = new ArrayList[Long]();
            for (val tx in orderedTx){
                if (committed().contains(tx))
                    commitTxOrdered.add(tx);
            }
            plh().slaveStore.commitAll(commitTxOrdered);
        }
        else
            plh().slaveStore.commitAll(orderedTx);
    }
    
    
    private static def excludePlaces(pg:PlaceGroup, pl1:Place, pl2:Place) {
    	assert(pg.contains(pl1) && pg.contains(pl2));
    	val rail = new Rail[Place](pg.size() - 2);
    	var i:Long = 0;
    	for (p in pg) {
    		if (p.id != pl1.id && p.id != pl2.id)
    			rail(i++) = p;
    	}
    	return new SparsePlaceGroup(rail);
    }
}