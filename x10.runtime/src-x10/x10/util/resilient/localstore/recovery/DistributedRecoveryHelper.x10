package x10.util.resilient.localstore.recovery;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.*;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.logging.TxDesc;

/**
 * Should be called by the local store when it detects that its slave has died.
 **/
public class DistributedRecoveryHelper[K] {K haszero} {
    
    public static def recoverSlave[K](plh:PlaceLocalHandle[LocalStore[K]]) {K haszero} {
    	debug("Recovering " + here + " DistributedRecoveryHelper.recoverSlave: started ...");
    	val start = System.nanoTime();
    	val deadPlace = plh().slave;
    	val oldActivePlaces = plh().getActivePlaces();
    	val deadVirtualId = oldActivePlaces.indexOf(deadPlace);
    	val spare = allocateSparePlace(plh, deadVirtualId, oldActivePlaces);
    	recoverSlave(plh, deadPlace, spare, start);
    }
    
    public static def recoverSlave[K](plh:PlaceLocalHandle[LocalStore[K]], deadPlace:Place, spare:Place, timeStartRecoveryNS:Long) {K haszero} {
        assert (timeStartRecoveryNS != -1);
        val startTimeNS = System.nanoTime();
        debug("Recovering " + here + " DistributedRecoveryHelper.recoverSlave: started given already allocated spare " + spare);
        val deadMaster = here;
        val oldActivePlaces = plh().getActivePlaces();
        val deadVirtualId = oldActivePlaces.indexOf(deadPlace);
        
        val newActivePlaces = computeNewActivePlaces(oldActivePlaces, deadVirtualId, spare);
        
        if (deadVirtualId == -1)
            throw new Exception(here + " Fatal error, slave index cannot be found in oldActivePlaces");
        
        val deadPlaceSlave = oldActivePlaces.next(deadPlace);
        if (deadPlaceSlave.isDead())
            throw new Exception(here + " Fatal error, two consecutive places died : " + deadPlace + "  and " + deadPlaceSlave);
        
        finish {
            at (deadPlaceSlave) async createMasterStoreAtSpare(plh, spare, deadPlace, deadVirtualId, newActivePlaces, deadMaster);
            createSlaveStoreAtSpare(plh, spare, deadPlace, deadVirtualId);
        }
        
        plh().replace(deadPlace, spare);
        plh().slave = spare;
        plh().getMasterStore().reactivate();

        val recoveryTime = System.nanoTime()-timeStartRecoveryNS;
        val spareAllocTime = startTimeNS - timeStartRecoveryNS;
        Console.OUT.printf("Recovering " + here + " DistributedRecoveryHelper.recoverSlave completed successfully: spareAllocTime %f seconds, totalRecoveryTime %f seconds\n" , (spareAllocTime/1e9), (recoveryTime/1e9));
    }
    
    private static def createMasterStoreAtSpare[K](plh:PlaceLocalHandle[LocalStore[K]], spare:Place, deadPlace:Place, deadVirtualId:Long, newActivePlaces:PlaceGroup, deadMaster:Place) {K haszero} {
        debug("Recovering " + here + " Slave of the dead master ...");
    	actAsCoordinator(plh, deadPlace);
    	
        plh().slaveStore.waitUntilPaused();
        
        val map = plh().slaveStore.getSlaveMasterState();
        debug("Recovering " + here + " Slave prepared a consistent master replica to the spare master spare=["+spare+"] ...");
        
        val deadPlaceSlave = here;
        at (spare) async {
            debug("Recovering " + here + " Spare received the master replica from slave ["+deadPlaceSlave+"] ...");    
        	plh().setMasterStore (new MasterStore(map, plh().immediateRecovery));
        	plh().getMasterStore().pausing();
        	plh().getMasterStore().paused();
        	
        	waitForSlaveStore(plh, deadMaster);
        	
        	plh().initSpare(newActivePlaces, deadVirtualId, deadPlace, deadPlaceSlave);
            plh().getMasterStore().reactivate();
            
            at (deadPlaceSlave) async {
                plh().replace(deadPlace, spare);
            }
        }
    }
    
    private static def createSlaveStoreAtSpare[K](plh:PlaceLocalHandle[LocalStore[K]], spare:Place, deadPlace:Place, deadVirtualId:Long) {K haszero} {
        debug("Recovering " + here + " Master of the dead slave, prepare a slave replica for the spare place ...");
    	plh().getMasterStore().waitUntilPaused();
    	val masterState = plh().getMasterStore().getState().getKeyValueMap();
    	debug("Recovering " + here + " Master prepared a consistent slave replica to the spare slave ...");
    	val me = here;
        at (spare) async {
            debug("Recovering " + here + " Spare received the slave replica from master ["+me+"] ...");    
            plh().slaveStore = new SlaveStore(masterState);
        }        
    }
    
    private static def waitForSlaveStore[K](plh:PlaceLocalHandle[LocalStore[K]], sender:Place) {K haszero}  {
        if (plh().slaveStoreExists())
            return;
        try {
            Runtime.increaseParallelism();
            
            while (!plh().slaveStoreExists()) {
                if (sender.isDead())
                    throw new DeadPlaceException(sender);
                TxConfig.waitSleep();
            }
        }
        finally {
            Runtime.decreaseParallelism(1n);
        }
    }
    
    private static def allocateSparePlace[K](plh:PlaceLocalHandle[LocalStore[K]], deadVirtualId:Long, oldActivePlaces:PlaceGroup) {K haszero} {
        val nPlaces = Place.numAllPlaces();
        val nActive = oldActivePlaces.size();
        var placeIndx:Long = -1;
        for (var i:Long = nActive; i < nPlaces; i++) {
            if (oldActivePlaces.contains(Place(i)))
                continue;
            
            var allocated:Boolean = false;
            try {
                debug("Recovering " + here + " Try to allocate " + Place(i) );
                allocated = at (Place(i)) plh().allocate(deadVirtualId);
            }catch(ex:Exception) {
            }
            
            if (!allocated)
                debug("Recovering " + here + " Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
            else {
                debug("Recovering " + here + " Succeeded to allocate " + Place(i) );
                placeIndx = i;
                break;
            }
        }
        if(placeIndx == -1)
            throw new Exception(here + " No available spare places to allocate ");         
            
        return Place(placeIndx);
    }
    
    private static def actAsCoordinator[K](plh:PlaceLocalHandle[LocalStore[K]], deadPlace:Place) {K haszero} {
    	debug("Recovering " + here + " slave acting as master to complete dead master's transactions ...");
    	val txDescs = plh().slaveStore.getTransDescriptors(deadPlace);
    	for (txDesc in txDescs) {
    	    val map = new ResilientNativeMap(plh);
            debug("Recovering " + here + " recovering txdesc " + txDesc);
            val tx = map.restartGlobalTransaction(txDesc);
            try {
                if (txDesc.status == TxDesc.COMMITTING) {
                    debug("Recovering " + here + " recovering Tx["+tx.id+"] commit it");
                    tx.commitRecovery();
                    debug("Recovering " + here + " recovering Tx["+tx.id+"] commit done");
                }
                else if (txDesc.status == TxDesc.STARTED) {
                    //if (TxConfig.get().TM_DEBUG) 
                    debug("Recovering " + here + " recovering Tx["+tx.id+"] abort it");
                    tx.abortRecovery();
                    debug("Recovering " + here + " recovering Tx["+tx.id+"] abort done");
                }
            }catch(ex:Exception) {
                debug("Recovering " + here + " recovering Tx["+tx.id+"] failed with exception ["+ex.getMessage()+"] ");        
            }
    	}
        debug("Recovering " + here + " slave acting as master to complete dead master's transactions done ...");
    }
    
    private static def updateSlaveData[K](plh:PlaceLocalHandle[LocalStore[K]], oldActivePlaces:PlaceGroup) {K haszero} {
    	debug(here+ " UpdateSlaveData: slave["+here+"] is asking other masters about the status of prepared transactions ...");
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
                            committedList = plh().getMasterStore().filterCommitted(txList);
                        else
                            committedList = plh().slaveStore.filterCommitted(txList);
                        val cList = committedList;
                        at (committed) async {
                            committedLock().lock();
                            committed().addAll(cList);
                            committedLock().unlock();
                        }
                    }
                }
            }
        }
        
        debug(here + " updateSlaveData: slave["+here+"] got full knowledge of committed transactions ...");
        
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
        
        debug(here + " updateSlaveData: slave["+here+"] done ...");
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
    
    private static def debug(msg:String) {
    	if (TxConfig.get().TMREC_DEBUG) Console.OUT.println( msg );
    }
    
}