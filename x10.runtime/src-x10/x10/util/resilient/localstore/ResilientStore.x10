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
import x10.util.Timer;

/**
 * A store that maintains a master + 1 backup (slave) copy
 * of the data.
 * The mapping between masters and slaves is specififed by
 * the next/prev operations on the activePlaces PlaceGroup.
 */
public class ResilientStore {
    private val plh:PlaceLocalHandle[LocalStore];
    private var activePlaces:PlaceGroup;
    
    private val appMaps:HashMap[String,ResilientNativeMap];
    private transient val lock:SimpleLatch;
    
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
        return new ResilientStore(pg, plh);
    }
    
    public def makeMap(name:String):ResilientNativeMap {
    	try {
    		lock.lock();
    		val map = new ResilientNativeMap(name, plh);
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
        checkIfBothMasterAndSlaveDied(changes);
        
        val start1 = Timer.milliTime();
        // Initialize LocalStore at newly active places.
        for (p in changes.addedPlaces) {
            PlaceLocalHandle.addPlace[LocalStore](plh, p, ()=>new LocalStore());
        }
        val start2 = Timer.milliTime();
        recoverData(changes);
        val end = Timer.milliTime();
        activePlaces = changes.newActivePlaces;
        
        Console.OUT.println("Resilient store recovery completed:addPlacesMS:" + (start2-start1) +":recoverDataMS:"+(end-start2));
    }

    private def checkIfBothMasterAndSlaveDied(changes:ChangeDescription) {
        for (dp in changes.removedPlaces) {
            val slave = changes.oldActivePlaces.next(dp);
            if (changes.removedPlaces.contains(slave)) {
                val virtualId = changes.oldActivePlaces.indexOf(dp);
                throw new Exception("Fatal: both master and slave lost for virtual place["+virtualId+"] ");
            }
        }
    }

    private def recoverData(changes:ChangeDescription) {
        val plh = this.plh; // don't capture this in at!
        finish {
            for (added in changes.addedPlaces) {
                /*from existing slave to new master*/
                val masterVId = changes.newActivePlaces.indexOf(added);
                val slave = changes.newActivePlaces.next(added);
                at (slave) async {
                    val maps = plh().slaveStore.getMasterState(masterVId).maps;
                    at (added) async {
                        plh().joinAsMaster(masterVId, maps);
                    }
                }
                
                /*from existing master to new slave*/
                val master = changes.newActivePlaces.prev(added);
                val virtualId = changes.newActivePlaces.indexOf(master);
                at (master) async {
                    val masterState = plh().masterStore.getState(); 
                    plh().slave = added;
                    at (added) async {
                        plh().joinAsSlave(virtualId, masterState);
                    }
                }
            }
        }
    }
}