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
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.TxStoreConcurrencyLimitException;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreAbortedException;
import x10.xrx.TxStoreFatalException;
import x10.xrx.TxStoreConflictException;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.MasterStore;
import x10.util.resilient.localstore.SlaveStore;
import x10.compiler.Uncounted;

/**
 * 
 */
public class TxStore {
    public val plh:PlaceLocalHandle[LocalStore[Any]];    
    public val app:NonShrinkingApp;

    private def this(plh:PlaceLocalHandle[LocalStore[Any]], app:NonShrinkingApp) {
        this.plh = plh;
        this.app = app;
    }
    
    public static def make(pg:PlaceGroup, immediateRecovery:Boolean) {
        return make (pg, immediateRecovery, null);
    }
    
    public static def make(pg:PlaceGroup, immediateRecovery:Boolean, app:NonShrinkingApp) {
        Console.OUT.println("Creating a transactional store with "+pg.size()+" active places, immediateRecovery = " + immediateRecovery);
        val plh = PlaceLocalHandle.make[LocalStore[Any]](Place.places(), ()=> new LocalStore[Any](pg, immediateRecovery) );
        val store = new TxStore(plh, app);
        Place.places().broadcastFlat(()=> { 
            plh().setPLH(plh); 
            Runtime.addTxStore(store);
        });
        Console.OUT.println("store created successfully ...");
        return store;
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
        executeTransaction(closure, -1, -1);
    }
    
    public def executeTransaction(closure:(Tx)=>void, maxRetries:Long, maxTimeNS:Long) {
        val beginning = System.nanoTime();
        var retryCount:Long = 0;
        var tx:Tx = null;
        while(true) {
            if (retryCount == maxRetries || (maxTimeNS != -1 && System.nanoTime() - beginning >= maxTimeNS)) {
                throw new TxStoreFatalException("Maximum limit for retrying a transaction reached!! - retryCount["+retryCount+"] maxRetries["+maxRetries+"] maxTimeNS["+maxTimeNS+"]");
            }
            retryCount++;
            try {
                tx = makeTx();
                finish {
                    Runtime.registerFinishTx(tx);
                    closure(tx);
                }
                break;
            } catch(ex:Exception) {
                throwIfFatalSleepIfRequired(tx.id, ex);
            }
        }
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
    
    public def resetTxStatistics() {
        if (plh().stat == null)
            return;
        finish for (p in plh().getActivePlaces()) at (p) async {
            plh().stat.clear();
        }
    }
    
    public def fixAndGetActivePlaces() {
        plh().replaceDeadPlaces();
        return plh().getActivePlaces();
    }
    
    public def nextPlaceChange() = plh().nextPlaceChange();
    
    public def asyncRecover() {
       
        val plh = this.plh;
        val ls = plh();
        if (!ls.immediateRecovery || !ls.slave.isDead()) {
            debug("Recovering " + here + " Runtime.asyncRecover returning  immediate["+ls.immediateRecovery+"] slaveDead["+ls.slave.isDead()+"] ...");
            return;
        }
        
        debug("Recovering " + here + " Runtime.asyncRecover starting  immediate["+ls.immediateRecovery+"] slaveDead["+ls.slave.isDead()+"] masterActive["+ls.masterStore.isActive()+"] ...");
        if ( ls.slave.isDead() && ls.masterStore.isActive() ) {
             ls.masterStore.pausing();
            @Uncounted async recoverSlave(plh);
        }
    }
    
    public def recoverSlave(plh:PlaceLocalHandle[LocalStore[Any]]) {
        debug("Recovering " + here + " DistributedRecoveryHelper.recoverSlave: started ...");
        val start = System.nanoTime();
        val deadPlace = plh().slave;
        val oldActivePlaces = plh().getActivePlaces();
        val deadVirtualId = oldActivePlaces.indexOf(deadPlace);
        val spare = allocateSparePlace(plh, deadVirtualId, oldActivePlaces);
        recoverSlave(plh, deadPlace, spare, start);
        if (app != null) {
            app.startPlace(spare, this, true);
        }
    }
    
    public def recoverSlave(plh:PlaceLocalHandle[LocalStore[Any]], deadPlace:Place, spare:Place, timeStartRecoveryNS:Long) {
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
    
    private def createMasterStoreAtSpare(plh:PlaceLocalHandle[LocalStore[Any]], spare:Place, deadPlace:Place, deadVirtualId:Long, newActivePlaces:PlaceGroup, deadMaster:Place) {
        debug("Recovering " + here + " Slave of the dead master ...");
        
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
    
    private def createSlaveStoreAtSpare(plh:PlaceLocalHandle[LocalStore[Any]], spare:Place, deadPlace:Place, deadVirtualId:Long) {
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
    
    private def waitForSlaveStore(plh:PlaceLocalHandle[LocalStore[Any]], sender:Place)  {
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
    
    private def allocateSparePlace(plh:PlaceLocalHandle[LocalStore[Any]], deadVirtualId:Long, oldActivePlaces:PlaceGroup) {
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
    
    private def computeNewActivePlaces(oldActivePlaces:PlaceGroup, virtualId:Long, spare:Place) {
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
    
    private def debug(msg:String) {
        if (TxConfig.get().TMREC_DEBUG) Console.OUT.println( msg );
    }
}