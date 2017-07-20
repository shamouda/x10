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
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.util.concurrent.Lock;
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.recovery.*;

/**
 * A store that maintains a master + 1 backup (slave) copy
 * of the data.
 * The mapping between masters and slaves is specififed by
 * the next/prev operations on the activePlaces PlaceGroup.
 */
public class ResilientStore[K] {K haszero} {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public val plh:PlaceLocalHandle[LocalStore[K]];
    
    private transient val lock:Lock;
    
    private static val stores = new ArrayList[Any]();
    
    private def this(plh:PlaceLocalHandle[LocalStore[K]]) {
        this.plh = plh;
        this.lock = new Lock();
    }
    
    public static def make[K](pg:PlaceGroup, immediateRecovery:Boolean) {K haszero} {
        Console.OUT.println("Creating a resilient store with "+pg.size()+" active places, immediateRecovery = " + immediateRecovery);
        val plh = PlaceLocalHandle.make[LocalStore[K]](Place.places(), ()=> new LocalStore[K](pg, immediateRecovery) );
        val store = new ResilientStore[K](plh);
        
        Place.places().broadcastFlat(()=> { 
        	plh().setPLH(plh); 
        });
        stores.add(store);
        Console.OUT.println("store created successfully ...");
        return store;
    }
    
    public def makeMap():ResilientNativeMap[K] {
    	return new ResilientNativeMap[K](plh);
    }
    
    public def getVirtualPlaceId() = plh().getVirtualPlaceId();
    
    public def getActivePlaces() = plh().getActivePlaces();
    
    private def getMaster(p:Place) = plh().getMaster(p);

    private def getSlave(p:Place) = plh().getSlave(p);
    
    public def getNextPlace() = plh().getNextPlace();
    
    public def sameActivePlaces(active:PlaceGroup) = plh().sameActivePlaces(active);

    public def updateForChangedPlaces(changes:ChangeDescription):void {
        CentralizedRecoveryHelper.recover(plh, changes);
        plh().activePlaces = changes.newActivePlaces;
    }
    
}