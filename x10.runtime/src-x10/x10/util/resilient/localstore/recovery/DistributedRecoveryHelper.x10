package x10.util.resilient.localstore.recovery;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.*;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.compiler.Immediate;
import x10.util.concurrent.AtomicInteger;
import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.ResilientCondition;

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
                        
            // remote call
            val dst = Place(i);
            val allocGR = GlobalRef(new AtomicInteger(0n));
            val rCond = ResilientCondition.make(dst);
            val closure = (gr:GlobalRef[Condition]) => {
                at (dst) @Immediate("alloc_spare_async") async {
                    var success:Boolean = false;
                    try {
                        success = plh().allocate(deadVirtualId);
                    } catch (t:Exception) {
                    }
                    val s = success ? 1n : 0n;
                    at (gr) @Immediate("alloc_spare_async_response") async {
                        (allocGR as GlobalRef[AtomicInteger{self!=null}]{self.home == here})().set(s);
                        gr().release();
                    }
                }
            };
            rCond.run(closure);
            val allocated = allocGR().get() == 1n;
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