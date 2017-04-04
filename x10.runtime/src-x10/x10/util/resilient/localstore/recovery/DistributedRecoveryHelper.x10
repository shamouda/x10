package x10.util.resilient.localstore.recovery;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.tx.TxDesc;
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.*;
import x10.xrx.Runtime;
/**
 * Should be called by the local store when it detects that its slave has died.
 **/
public class DistributedRecoveryHelper {
    
    public static def recoverSlave(plh:PlaceLocalHandle[LocalStore]) {
    	Console.OUT.println(here + " DistributedRecoveryHelper.recoverSlave: started ...");
    	val start = System.nanoTime();
    	val deadSlave = plh().slave;
    	val oldActivePlaces = plh().getActivePlaces();
    	val deadPlaceVirtualPlaceId = oldActivePlaces.indexOf(deadSlave);
    	val spare = allocateSparePlace(plh, deadPlaceVirtualPlaceId, oldActivePlaces);
    	recoverSlave(plh, spare, start);
    }
    
    public static def recoverSlave(plh:PlaceLocalHandle[LocalStore], spare:Place, timeStartRecoveryNS:Long) {
        val startTimeNS = timeStartRecoveryNS == -1? System.nanoTime() : timeStartRecoveryNS;
        Console.OUT.println(here + " DistributedRecoveryHelper.recoverSlave: started given already allocated spare " + spare);
        val deadSlave = plh().slave;
        val oldActivePlaces = plh().getActivePlaces();
        val deadPlaceVirtualPlaceId = oldActivePlaces.indexOf(deadSlave);
        
        val slaveOfDeadMaster = oldActivePlaces.next(deadSlave);
        if (slaveOfDeadMaster.isDead())
            throw new Exception(here + " Fatal error, two consecutive places died : " + deadSlave + "  and " + slaveOfDeadMaster);
        
        finish {
            at (slaveOfDeadMaster) async createMasterStoreAtSpare(plh, spare, oldActivePlaces);
            async createSlaveStoreAtSpare(plh, spare);
        }
        
        Console.OUT.println(here + " DistributedRecoveryHelper.recoverSlave: now spare place has all needed data, let it handshake with other places ...");
        val newActivePlaces = computeNewActivePlaces(oldActivePlaces, deadPlaceVirtualPlaceId, spare);
        finish at (spare) async {
            plh().handshake(newActivePlaces, deadPlaceVirtualPlaceId);
        }
        //the application layer can now recognize a change in the places configurations
        plh().replace(deadPlaceVirtualPlaceId, spare);
        Console.OUT.println(here + " DistributedRecoveryHelper.recoverSlave: completed successfully, recoveryTime:" + ((System.nanoTime()-startTimeNS)/1e9)+" seconds");
    }
    
    
    private static def createMasterStoreAtSpare(plh:PlaceLocalHandle[LocalStore], spare:Place, oldActivePlaces:PlaceGroup) {
    	completeInitiatedTransactions(plh);
        
        if (TxConfig.get().TM_REP.equals("lazy"))
        	updateSlaveData(plh, oldActivePlaces);
        else
        	plh().slaveStore.waitUntilPaused();
        
        val map = plh().slaveStore.getSlaveMasterState();
        val me = here;
        at (spare) async {
        	plh().masterStore = new MasterStore(map, plh().immediateRecovery);
        	plh().slave = me;
        }
    }
    
    private static def createSlaveStoreAtSpare(plh:PlaceLocalHandle[LocalStore], spare:Place) {
    	plh().masterStore.waitUntilPaused();
    	val masterState = plh().masterStore.getState().getKeyValueMap();
        at (spare) {
            plh().slaveStore = new SlaveStore(masterState);
        }
        plh().slave = spare;
        plh().masterStore.reactivate();
    }
    
    private static def allocateSparePlace(plh:PlaceLocalHandle[LocalStore], deadPlaceVirtualPlaceId:Long, oldActivePlaces:PlaceGroup) {
        val nPlaces = Place.numAllPlaces();
        val nActive = oldActivePlaces.size();
        var placeIndx:Long = -1;
        for (var i:Long = nActive; i < nPlaces; i++) {
            if (oldActivePlaces.contains(Place(i)))
                continue;
            
            var allocated:Boolean = false;
            try {
                allocated = at (Place(i)) plh().allocate(deadPlaceVirtualPlaceId);
            }catch(ex:Exception) {
            }
            
            if (!allocated)
                Console.OUT.println(here + " Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
            else {
                Console.OUT.println(here + " Succeeded to allocate " + Place(i) );
                placeIndx = i;
                break;
            }
        }
        assert(placeIndx != -1) : here + " No available spare places to allocate ";
        return Place(placeIndx);
    }
    
    private static def completeInitiatedTransactions(plh:PlaceLocalHandle[LocalStore]) {
    	Console.OUT.println(here + " slave acting as master to complete dead master's transactions ...");
    	val txDescs = plh().slaveStore.getTransDescriptors();
    	for (txDesc in txDescs) {
    	    val map = new ResilientNativeMap(txDesc.mapName, plh);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " recovering txdesc " + txDesc);
            val tx = map.restartGlobalTransaction(txDesc);
            if (txDesc.status == TxDesc.COMMITTING) {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] commit it");
                tx.commit(true); //ignore phase one
            }
            else if (txDesc.status == TxDesc.STARTED) {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] abort it");
                tx.abortRecovery();
            }
    	}
        Console.OUT.println(here + " slave acting as master to complete dead master's transactions done ...");
    }
    
    private static def updateSlaveData(plh:PlaceLocalHandle[LocalStore], oldActivePlaces:PlaceGroup) {
    	Console.OUT.println(here+ " UpdateSlaveData: slave["+here+"] is asking other masters about the status of prepared transactions ...");
        val committed = GlobalRef(new ArrayList[Long]());
        val committedLock = GlobalRef(new Lock());
        
        //In RL_EA_*, validation is not required. Thus any transaction reaching the slav, is a committed transaction
        //In the other schemes, a transaction reaching the slave may or may not be committed, we need to find out the status of 
        //these transactions from their coordinators
        if (TxConfig.get().VALIDATION_REQUIRED) {
            val placeTxsMap = plh().slaveStore.clusterTransactions(); //get the transactions at the slave, clustered by their coordinator place
            finish {
                val iter = placeTxsMap.keySet().iterator();
                while (iter.hasNext()) {
                    val ownerPlaceIndex = iter.next();
                    val txList = placeTxsMap.getOrThrow(ownerPlaceIndex);
                    var pl:Place = oldActivePlaces(ownerPlaceIndex);
                    var master:Boolean = true;
                    if (pl.isDead()){
                        pl = oldActivePlaces.next(pl);
                        master = false;
                    }
                    val isMaster = master;
                    val x = pl;
                    at (pl) async {
                        var committedList:ArrayList[Long]; 
                        if (isMaster)
                            committedList = plh().masterStore.filterCommitted(txList);
                        else
                            committedList = plh().slaveStore.filterCommitted(txList);
                        val cList = committedList;
                        at (committed) {
                            committedLock().lock();
                            committed().addAll(cList);
                            committedLock().unlock();
                        }
                    }
                }
            }
        }
        
        Console.OUT.println(here + " updateSlaveData: slave["+here+"] got full knowledge of committed transactions ...");
        
        val orderedTx = plh().slaveStore.getPendingTransactions();
        if (TxConfig.get().VALIDATION_REQUIRED) {
            val commitTxOrdered = new ArrayList[Long]();
            for (val tx in orderedTx){
                if (committed().contains(tx))
                    commitTxOrdered.add(tx);
            }
            plh().slaveStore.commitAll(commitTxOrdered);
        }
        else
            plh().slaveStore.commitAll(orderedTx);
        
        Console.OUT.println(here + " updateSlaveData: slave["+here+"] done ...");
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
    
    private static def computeNewActivePlaces(oldActivePlaces:PlaceGroup, virtualId:Long, spare:Place) {
        val size = oldActivePlaces.size();
        val rail = new Rail[Place](size);
        for (var i:Long = 0; i< size; i++) {
            if (virtualId == i)
                rail(i) = spare;
            else
                rail(i) = oldActivePlaces(i);
        }
        return new SparsePlaceGroup(rail);
    }
    
}