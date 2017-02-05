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
import x10.util.concurrent.SimpleLatch;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.tx.TxDesc;

/**
 * A store that maintains a master + 1 backup (slave) copy
 * of the data.
 * The mapping between masters and slaves is specififed by
 * the next/prev operations on the activePlaces PlaceGroup.
 */
public class ResilientStore {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public val plh:PlaceLocalHandle[LocalStore];
    public var activePlaces:PlaceGroup;
    
    private val appMaps:HashMap[String,ResilientNativeMap];
    private transient val lock:SimpleLatch;
    
    //A resilient map for transactions' descriptors
    var txDescMap:ResilientNativeMap;
    
    private def this(pg:PlaceGroup, plh:PlaceLocalHandle[LocalStore]) {
        this.activePlaces = pg;
        this.plh = plh;
        this.appMaps = new HashMap[String,ResilientNativeMap]();
        this.lock = new SimpleLatch();
    }
    
    public static def make(pg:PlaceGroup):ResilientStore {
        val plh = PlaceLocalHandle.make[LocalStore](pg, () => {
            new LocalStore(pg.indexOf(here), pg.next(here))
        });
        val store = new ResilientStore(pg, plh);
        
        if (resilient)
            store.txDescMap = store.makeMap("_TxDesc_");
        return store;
    }
    
    public def makeMap(name:String):ResilientNativeMap {
    	try {
    		lock.lock();
    		val plh = PlaceLocalHandle.make[ArrayList[Tx]](activePlaces, () => { new ArrayList[Tx]() });
    		val map = new ResilientNativeMap(name, this, plh);
    		appMaps.put(name, map);
    		return map;
    	} finally {
    		lock.unlock();
    	}
    }
    
    public def getVirtualPlaceId() = activePlaces.indexOf(here);
    
    public def getActivePlaces() = activePlaces;

    private def getMaster(p:Place) = activePlaces.prev(p);

    private def getSlave(p:Place) = activePlaces.next(p);

    public def updateForChangedPlaces(changes:ChangeDescription):void {
        // Initialize LocalStore at newly active places.
        for (p in changes.addedPlaces) {
            PlaceLocalHandle.addPlace[LocalStore](plh, p, ()=>new LocalStore());
            val iter = appMaps.keySet().iterator();
            while (iter.hasNext()) {
                val mapName = iter.next();
                val map = appMaps.getOrThrow(mapName);
                PlaceLocalHandle.addPlace[ArrayList[Tx]]( map.plh, p, ()=>new ArrayList[Tx]());
            }
        }

        checkIfBothMasterAndSlaveDied(changes);
        
        /*complete transactions whose coordinator died*/
        recoverTransactions(changes);
        
        recoverMasters(changes);
        
        recoverSlaves(changes);

        activePlaces = changes.newActivePlaces;
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
                val virtualId = changes.newActivePlaces.indexOf(newMaster);
                val slave = changes.newActivePlaces.next(newMaster);
                at (slave) async {
                    val maps = plh().slaveStore.getSlaveMasterState().maps;
                    at (newMaster) async {
                        plh().joinAsMaster(virtualId, slave, maps);
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
                    val masterState = plh().masterStore.getState();
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
                    val masterState = plh().slaveStore.getSlaveMasterState();
                    val txDescMap = masterState.maps.getOrElse("_TxDesc_", null);
                    if (txDescMap != null) {
                        val set = txDescMap.keySet();
                        val iter = set.iterator();
                        while (iter.hasNext()) {
                            val txId = iter.next();
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
                                else {
                                    if (TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] abort it");
                                    tx.abort();
                                }
                            }
                        }
                    }
                }
            }
        }
        Console.OUT.println("recoverTransactions completed");
    }
    
}