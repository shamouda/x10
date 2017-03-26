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

package x10.util.resilient.localstore;

import x10.util.HashSet;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.tx.TxDesc;
import x10.util.resilient.localstore.tx.TransactionsList;
import x10.util.concurrent.Lock;

/**
 * A store that maintains a master + 1 backup (slave) copy
 * of the data.
 * The mapping between masters and slaves is specififed by
 * the next/prev operations on the activePlaces PlaceGroup.
 */
public class ResilientStore {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val TM_REP = System.getenv("TM_REP") == null ? "lazy" : System.getenv("TM_REP");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public val plh:PlaceLocalHandle[LocalStore];
    
    private val appMaps:HashMap[String,ResilientNativeMap];
    private transient val lock:Lock;
    
    //A resilient map for transactions' descriptors
    var txDescMap:ResilientNativeMap;
    
    private def this(plh:PlaceLocalHandle[LocalStore]) {
        this.plh = plh;
        this.appMaps = new HashMap[String,ResilientNativeMap]();
        this.lock = new Lock();
    }
    
    public static def make(pg:PlaceGroup):ResilientStore {
        val plh = PlaceLocalHandle.make[LocalStore](Place.places(), ()=> new LocalStore(pg) );
        val store = new ResilientStore(plh);
        Place.places().broadcastFlat(()=> {
            plh().setPLH(plh);
        });
        
        if (resilient)
            store.txDescMap = store.makeMap("_TxDesc_");
        return store;
    }
    
    public def makeMap(name:String):ResilientNativeMap {
        try {
            lock.lock();
            val list = PlaceLocalHandle.make[TransactionsList](Place.places(), () => new TransactionsList());
            val map = new ResilientNativeMap(name, this, list);
            appMaps.put(name, map);
            return map;
        } finally {
            lock.unlock();
        }
    }
    
    public def getVirtualPlaceId() {
        try {
            lock.lock();
            return plh().activePlaces.indexOf(here);
        }finally {
            lock.unlock();
        }
    }
    
    public def getActivePlaces() {
    	try {
    		lock.lock();
    		val active = plh().activePlaces;
    		return new SparsePlaceGroup(new Rail[Place](active.size(), (i:Long) => active(i)));
    	}finally {
    		lock.unlock();
    	}
    }

    private def getMaster(p:Place) {
    	try {
    		lock.lock();
    		val active = plh().activePlaces;
    	    return active.prev(p);
    	} finally {
    		lock.unlock();
    	}
    }

    private def getSlave(p:Place) {
    	try {
    		lock.lock();
    		val active = plh().activePlaces;
    	    return active.next(p);
    	}finally {
    		lock.unlock();
    	}
    }
    
    public def getNextPlace() {
    	try {
    		lock.lock();
    		val active = plh().activePlaces;
    		return active.next(here);
    	}finally {
    		lock.unlock();
    	}
    }
    
    public def sameActivePlaces(active:PlaceGroup) {
    	try {
    		lock.lock();
    		return plh().activePlaces.equals(active);
    	}finally {
    		lock.unlock();
    	}
    }

    public def updateForChangedPlaces(changes:ChangeDescription):void {
        checkIfBothMasterAndSlaveDied(changes);
        
        /*complete transactions whose coordinator died*/
        recoverTransactions(changes);
        
        recoverMasters(changes);
        
        recoverSlaves(changes);

        plh().activePlaces = changes.newActivePlaces;
    }

    private def checkIfBothMasterAndSlaveDied(changes:ChangeDescription) {
        for (dp in changes.removedPlaces) {
            val slave = changes.oldActivePlaces.next(dp);
            if (changes.removedPlaces.contains(slave)) {
                val virtualId = changes.oldActivePlaces.indexOf(dp);
                throw new Exception("Fatal: both master and slave were lost for virtual place["+virtualId+"] ");
            }
        }
    }

    private def recoverMasters(changes:ChangeDescription) {
        val plh = this.plh; // don't capture this in at!
        finish {
            for (newMaster in changes.addedPlaces) {
            	val active = changes.newActivePlaces;
                val slave = changes.newActivePlaces.next(newMaster);
                Console.OUT.println("recovering masters: newMaster["+newMaster+"] slave["+slave+"] ");
                at (slave) async {                    
                    val map = plh().slaveStore.getSlaveMasterState();
                    at (newMaster) async {
                        plh().joinAsMaster(active, map);
                    }
                }
            }
        }
    }

    private def recoverSlaves(changes:ChangeDescription) {
        val plh = this.plh; // don't capture this in at!
        finish {
            for (newSlave in changes.addedPlaces) {
                val master = changes.newActivePlaces.prev(newSlave);
                Console.OUT.println("recovering slaves: master["+master+"] newSlave["+newSlave+"] ");
                val masterVirtualId = changes.newActivePlaces.indexOf(master);
                at (master) async {
                    val masterState = plh().masterStore.getState().getKeyValueMap();
                    at (newSlave) {
                        plh().slaveStore.addMasterPlace(masterVirtualId, masterState);
                    }
                    plh().slave = newSlave;
                }
            }
        }
    }
    
    private def recoverTransactions(changes:ChangeDescription) {
        Console.OUT.println("recoverTransactions started");
        val plh = this.plh; // don't capture this in at!
        finish {
            val oldActivePlaces = changes.oldActivePlaces;
            for (deadPlace in changes.removedPlaces) {
                val slave = oldActivePlaces.next(deadPlace);
                Console.OUT.println("recoverTransactions deadPlace["+deadPlace+"] moving to its slave["+slave+"] ");
                at (slave) async {
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
                                    val map = appMaps.getOrThrow(txDesc.mapName);
                                    if (TM_DEBUG) Console.OUT.println(here + " recovering txdesc " + txDesc);
                                    val tx = map.restartGlobalTransaction(txDesc);
                                    if (txDesc.status == TxDesc.COMMITTING) {
                                        if (TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] commit it");
                                        tx.commit(true); //ignore phase one
                                    }
                                    else if (txDesc.status == TxDesc.STARTED) {
                                        if (TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] abort it");
                                        tx.abort();
                                    }
                                }
                            }
                        }
                    }
                    
                    if (TM_REP.equals("lazy"))
                        applySlaveTransactions(plh, changes);
                }
            }
        }
        Console.OUT.println("recoverTransactions completed");
    }
    
    private def applySlaveTransactions(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription) {
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
                    var pl:Place = changes.oldActivePlaces(placeIndex);
                    var master:Boolean = true;
                    if (pl.isDead()){
                        pl = changes.oldActivePlaces.next(pl);
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
    
}