/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.xrx;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.xrx.txstore.TxLocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.TxStoreConcurrencyLimitException;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreAbortedException;
import x10.xrx.TxStoreFatalException;
import x10.xrx.TxStoreConflictException;
import x10.xrx.txstore.TxConfig;
import x10.xrx.txstore.TxMasterStore;
import x10.xrx.txstore.TxSlaveStore;
import x10.compiler.Uncounted;
import x10.util.concurrent.Condition;
import x10.util.resilient.concurrent.ResilientCondition;
import x10.compiler.Immediate;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.PlaceManager.ChangeDescription;

/**
 * 
 */
public class TxStore {
    public val plh:PlaceLocalHandle[TxLocalStore[Any]];    
    public val callback:(Long, Place, TxStore)=>void;

    private def this(plh:PlaceLocalHandle[TxLocalStore[Any]], callback:(Long, Place, TxStore)=>void) {
        this.plh = plh;
        this.callback = callback;
    }
    
    public static def make(pg:PlaceGroup, immediateRecovery:Boolean) {
        return make (pg, immediateRecovery, null);
    }
    
    public static def make(pg:PlaceGroup, immediateRecovery:Boolean, callback:(Long, Place, TxStore)=>void) {
        Console.OUT.println("Creating a transactional store with "+pg.size()+" active places, immediateRecovery = " + immediateRecovery);
        val plh = PlaceLocalHandle.make[TxLocalStore[Any]](Place.places(), ()=> new TxLocalStore[Any](pg, immediateRecovery) );
        val store = new TxStore(plh, callback);
        Place.places().broadcastFlat(()=> { 
            plh().setPLH(plh); 
            Runtime.addTxStore(store);
        });
        Console.OUT.println("store created successfully ...");
        return store;
    }

    public def prevPlace() {
        return plh().getMaster(here);
    }
    
    public def makeLockingTx():TxLocking {
        val id = plh().getMasterStore().getNextTransactionId();
        return new TxLocking(plh, id);
    }
    
    private def makeLockingTx(members:Rail[Long], keys:Rail[Any], readFlags:Rail[Boolean], o:Long):TxLocking {
        val id = plh().getMasterStore().getNextTransactionId();
        return new TxLocking(plh, id, members, keys, readFlags, o);
    }
    
    public def executeLockingTx(members:Rail[Long], keys:Rail[Any], readFlags:Rail[Boolean], o:Long, closure:(TxLocking)=>void) {
        val tx = makeLockingTx(members, keys, readFlags, o);
        tx.lock();
        finish { 
            closure(tx); 
        }
        tx.unlock();
    }
    
    public def makeTx() {
        val id = plh().getMasterStore().getNextTransactionId();
        return Tx.make(plh, id);
    }
    
    
    public def executeTransaction(closure:(Tx)=>void) {
        return executeTransaction(closure, -1, -1);
    }
    
    public def executeTransaction(closure:(Tx)=>void, maxRetries:Long, maxTimeNS:Long) {
        val beginning = System.nanoTime();
        var retryCount:Long = 0;
        var tx:Tx = null;
        while(true) {
            if (retryCount == maxRetries || (maxTimeNS != -1 && System.nanoTime() - beginning >= maxTimeNS)) {
                throw new TxStoreFatalException("Maximum limit for retrying a transaction reached!! - retryCount["+retryCount+"] maxRetries["+maxRetries+"] maxTimeNS["+maxTimeNS+"]");
            }
            try {
                tx = makeTx();
                finish {
                    Runtime.registerFinishTx(tx, true);
                    closure(tx);
                }
                break;
            } catch(ex:Exception) {
                throwIfFatalSleepIfRequired(tx.id, ex);
            }
            retryCount++;
        }
        return retryCount;
    }
    
    private def throwIfFatalSleepIfRequired(txId:Long, ex:Exception) {
        val immediateRecovery = plh().immediateRecovery;
        var dpe:Boolean = false;
        if (ex instanceof MultipleExceptions) {
            val fatalExList = (ex as MultipleExceptions).getExceptionsOfType[TxStoreFatalException]();
            if (fatalExList != null && fatalExList.size > 0)
                throw fatalExList(0);
            
            val deadExList = (ex as MultipleExceptions).getExceptionsOfType[DeadPlaceException]();
            val confExList = (ex as MultipleExceptions).getExceptionsOfType[TxStoreConflictException]();
            val pauseExList = (ex as MultipleExceptions).getExceptionsOfType[TxStorePausedException]();
            val abortedExList = (ex as MultipleExceptions).getExceptionsOfType[TxStoreAbortedException]();
            val maxConcurExList = (ex as MultipleExceptions).getExceptionsOfType[TxStoreConcurrencyLimitException]();
                    
            if ((ex as MultipleExceptions).exceptions.size > (deadExList.size + confExList.size + pauseExList.size + abortedExList.size)){
                Console.OUT.println(here + " Unexpected MultipleExceptions   size("+(ex as MultipleExceptions).exceptions.size + ")  (" 
                        + deadExList.size + " + " + confExList.size + " + " + pauseExList.size + " + " + abortedExList.size + " + " + maxConcurExList.size + ")");
                ex.printStackTrace();
                throw ex;
            }
            if (deadExList != null && deadExList.size != 0) {
                for (deadEx in deadExList) {
                    plh().replaceDeadPlace(deadEx.place);
                }
                System.threadSleep(TxConfig.DPE_SLEEP_MS);
                dpe = true;
            }
        } else if (ex instanceof DeadPlaceException) {
            if (!immediateRecovery) {
                throw ex;
            } else {
                System.threadSleep(TxConfig.DPE_SLEEP_MS);
                dpe = true;
            }
        } else if (ex instanceof TxStorePausedException) {
            System.threadSleep(TxConfig.DPE_SLEEP_MS);
        } else if (ex instanceof TxStoreConcurrencyLimitException) {
            System.threadSleep(TxConfig.DPE_SLEEP_MS);
        } else if (!(ex instanceof TxStoreConflictException || ex instanceof TxStoreAbortedException  )) {
            throw ex;
        }
        return dpe;
    }
    
    public def fixAndGetActivePlaces() {
        plh().replaceDeadPlaces();
        return plh().getActivePlaces();
    }
    
    public def updateForChangedPlaces(changes:PlaceManager.ChangeDescription){
        val plh = this.plh;
        val recoveryStart = System.nanoTime();
        var i:Long = 0;
        finish for (deadPlace in changes.removedPlaces) {
            val masterOfDeadSlave = changes.oldActivePlaces.prev(deadPlace);
            val spare = changes.addedPlaces.get(i++);
            Console.OUT.println("recovering " + deadPlace + "  through its master " + masterOfDeadSlave);
            at (masterOfDeadSlave) async {
                recoverSlave(plh, deadPlace, spare, recoveryStart);
            }
        }
        val newActivePlaces = changes.newActivePlaces;
        Place.places().broadcastFlat(()=> {
            plh().activePlaces = newActivePlaces;
        });
        val recoveryEnd = System.nanoTime();
        Console.OUT.printf("CentralizedRecoveryHelper.recover completed successfully: recoveryTime %f seconds\n", (recoveryEnd-recoveryStart)/1e9);
    }
    
    public def asyncRecover() {
        val ls = plh();
        if (!ls.immediateRecovery || !ls.slave.isDead()) {
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Runtime.asyncRecover returning  immediate["+ls.immediateRecovery+"] slaveDead["+ls.slave.isDead()+"] ...");
            return;
        }
        
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Runtime.asyncRecover starting  immediate["+ls.immediateRecovery+"] slaveDead["+ls.slave.isDead()+"] masterActive["+ls.masterStore.isActive()+"] ...");
        if ( ls.slave.isDead() && ls.masterStore.isActive() ) {
             ls.masterStore.pausing();
            @Uncounted async recoverSlave();
        }
    }
    
    public def recoverSlave() {
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxStore.recoverSlave: started ...");
        val start = System.nanoTime();
        val deadPlace = plh().slave;
        val oldActivePlaces = plh().getActivePlaces();
        val deadVirtualId = oldActivePlaces.indexOf(deadPlace);
        val spare = allocateSparePlace(plh, deadVirtualId, oldActivePlaces);
        recoverSlave(plh, deadPlace, spare, start);
        if (callback != null) {
            callback(deadVirtualId, spare, this);
        }
    }
    
    public static def recoverSlave(plh:PlaceLocalHandle[TxLocalStore[Any]], deadPlace:Place, spare:Place, timeStartRecoveryNS:Long) {
        assert (timeStartRecoveryNS != -1);
        val startTimeNS = System.nanoTime();
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " TxStore.recoverSlave: started given already allocated spare " + spare);
        val deadMaster = here;
        val oldActivePlaces = plh().getActivePlaces();
        val deadVirtualId = oldActivePlaces.indexOf(deadPlace);
        
        val newActivePlaces = computeNewActivePlaces(oldActivePlaces, deadVirtualId, spare);
        
        if (deadVirtualId == -1) {
            Console.OUT.println(here + " FATAL ERROR, slave index cannot be found in oldActivePlaces");
            System.killHere();
        }
        
        val deadPlaceSlave = oldActivePlaces.next(deadPlace);
        if (deadPlaceSlave.isDead()) {
            Console.OUT.println(here + " FATAL ERROR, two consecutive places died : " + deadPlace + "  and " + deadPlaceSlave);
            System.killHere();
        }
        
        finish {
            at (deadPlaceSlave) async createMasterStoreAtSpare(plh, spare, deadPlace, deadVirtualId, newActivePlaces, deadMaster);
            createSlaveStoreAtSpare(plh, spare, deadPlace, deadVirtualId);
        }
        
        plh().replace(deadPlace, spare);
        plh().slave = spare;
        plh().getMasterStore().reactivate();

        val recoveryTime = System.nanoTime()-timeStartRecoveryNS;
        val spareAllocTime = startTimeNS - timeStartRecoveryNS;
        Console.OUT.printf("Recovering " + here + " TxStore.recoverSlave completed successfully: spareAllocTime %f seconds, totalRecoveryTime %f seconds\n" , (spareAllocTime/1e9), (recoveryTime/1e9));
    }
    
    private static def createMasterStoreAtSpare(plh:PlaceLocalHandle[TxLocalStore[Any]], spare:Place, deadPlace:Place, deadVirtualId:Long, newActivePlaces:PlaceGroup, deadMaster:Place) {
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Slave of the dead master ...");
        
        plh().slaveStore.waitUntilPaused();
        
        val map = plh().slaveStore.getSlaveMasterState();
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Slave prepared a consistent master replica to the spare master spare=["+spare+"] ...");
        
        val deadPlaceSlave = here;
        at (spare) async {
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Spare received the master replica from slave ["+deadPlaceSlave+"] ...");    
            plh().setMasterStore (new TxMasterStore(map, plh().immediateRecovery));
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
    
    private static def createSlaveStoreAtSpare(plh:PlaceLocalHandle[TxLocalStore[Any]], spare:Place, deadPlace:Place, deadVirtualId:Long) {
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Master of the dead slave, prepare a slave replica for the spare place ...");
        plh().getMasterStore().waitUntilPaused();
        val masterState = plh().getMasterStore().getState().getKeyValueMap();
        if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Master prepared a consistent slave replica to the spare slave ...");
        val me = here;
        at (spare) async {
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Spare received the slave replica from master ["+me+"] ...");    
            plh().slaveStore = new TxSlaveStore(masterState);
        }        
    }
    
    private static def waitForSlaveStore(plh:PlaceLocalHandle[TxLocalStore[Any]], sender:Place)  {
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
    
    private static def allocateSparePlace(plh:PlaceLocalHandle[TxLocalStore[Any]], deadVirtualId:Long, oldActivePlaces:PlaceGroup) {
        val nPlaces = Place.numAllPlaces();
        val nActive = oldActivePlaces.size();
        var placeIndx:Long = -1;
        for (var i:Long = nActive; i < nPlaces; i++) {
            if (oldActivePlaces.contains(Place(i)))
                continue;
            
            val masterRes = new GlobalRef[TxStoreAllocateSpareResponse](new TxStoreAllocateSpareResponse());
            if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Try to allocate " + Place(i) );
            val master = Place(i);
            val rCond = ResilientCondition.make(master);
            val closure = (gr:GlobalRef[Condition]) => {
                at (master) @Immediate("alloc_spare_request") async {
                    val result = plh().allocate(deadVirtualId);
                    at (gr) @Immediate("alloc_spare_response") async {
                        val mRes = (masterRes as GlobalRef[TxStoreAllocateSpareResponse]{self.home == here})();
                        mRes.allocated = result;
                        gr().release();
                    }
                }
            };
            rCond.run(closure, true);
            var success:Boolean = true;
            if (rCond.failed() || !masterRes().allocated) {
                success = false;
            }
            rCond.forget();
            masterRes.forget();
            if (!success) {
                if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
            }
            else {
                if (TxConfig.TMREC_DEBUG) Console.OUT.println("Recovering " + here + " Succeeded to allocate " + Place(i) );
                placeIndx = i;
                break;
            }
        }
        if(placeIndx == -1) {
            Console.OUT.println(here + " FATAL No available spare places to allocate ");
            System.killHere();
        }
        return Place(placeIndx);
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
class TxStoreAllocateSpareResponse {
    var allocated:Boolean;
}