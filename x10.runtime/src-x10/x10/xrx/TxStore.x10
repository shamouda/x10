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
/**
 * A store that maintains a master + 1 backup (slave) copy
 * of the data.
 * The mapping between masters and slaves is specififed by
 * the next/prev operations on the activePlaces PlaceGroup.
 */
public class TxStore {
    public val plh:PlaceLocalHandle[LocalStore[Any]];    
    private def this(plh:PlaceLocalHandle[LocalStore[Any]]) {
        this.plh = plh;
    }
    
    public static def make(pg:PlaceGroup, immediateRecovery:Boolean) {
        Console.OUT.println("Creating a transactional store with "+pg.size()+" active places, immediateRecovery = " + immediateRecovery);
        val plh = PlaceLocalHandle.make[LocalStore[Any]](Place.places(), ()=> new LocalStore[Any](pg, immediateRecovery) );
        val store = new TxStore(plh);
        Place.places().broadcastFlat(()=> { 
        	plh().setPLH(plh); 
        });
        Console.OUT.println("store created successfully ...");
        return store;
    }
    
    public def makeTx() {
        val id = plh().getMasterStore().getNextTransactionId();
        return new Tx(plh, id);
    }
    
    
    public def resetTxStatistics() {
        if (plh().stat == null)
            return;
        finish for (p in plh().getActivePlaces()) at (p) async {
            plh().stat.clear();
        }
    }
}