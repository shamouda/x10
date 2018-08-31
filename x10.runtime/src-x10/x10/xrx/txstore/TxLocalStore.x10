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

package x10.xrx.txstore;

import x10.util.*;
import x10.util.concurrent.Lock;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.compiler.Ifdef;
import x10.util.resilient.localstore.Cloneable;
import x10.compiler.Uncounted;
import x10.xrx.TxStorePausedException;
import x10.util.concurrent.Condition;
import x10.compiler.Immediate;
import x10.util.resilient.concurrent.ResilientCondition;

public class TxLocalStore[K] {K haszero} {
    public static val RAIL_TYPE = 1n;
    public static val KV_TYPE = 2n;
    
	public val immediateRecovery:Boolean;
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private transient var masterStore:TxMasterStore[K] = null;   
    public transient var slaveStore:TxSlaveStore[K] = null;
    private var plh:PlaceLocalHandle[TxLocalStore[K]];
    private transient var lock:Lock;
    
    public transient var virtualPlaceId:Long = -1; //-1 means a spare place
    public transient var slave:Place;
    public transient var oldSlave:Place;
    public transient var activePlaces:PlaceGroup;
    private val replacementHistory = new HashMap[Long, Long] ();
    
    public static struct PlaceChange {
        public val oldPlace:Place;
        public val newPlace:Place;
        
        def this(oldPlace:Place, newPlace:Place) {
            this.oldPlace = oldPlace;
            this.newPlace = newPlace;
        }
    };
    
    public static struct SlaveChange {
        public val changed:Boolean;
        public val newSlave:Place;
        
        def this(changed:Boolean, newSlave:Place) {
            this.changed = changed;
            this.newSlave = newSlave;
        }
    };
    
    public def this(active:PlaceGroup, immediateRecovery:Boolean, storeType:Int, size:Long, init:(Long)=>K) {
        this.immediateRecovery = immediateRecovery;
        lock = new Lock();
        this.activePlaces = active;
        if (active.contains(here)) {
            virtualPlaceId = active.indexOf(here);
            if (storeType == RAIL_TYPE)
                masterStore = new TxMasterStoreForRail[K](size, init, immediateRecovery);
            else
                masterStore = new TxMasterStore[K](new HashMap[K,Cloneable](), immediateRecovery);
            if (resilient && !TxConfig.DISABLE_SLAVE) {
                slaveStore = new TxSlaveStore[K](storeType, size, init);
                slave = active.next(here);
                oldSlave = this.slave;
            }
        } //else, I am a spare place
    }
    
    public def setPLH(plh:PlaceLocalHandle[TxLocalStore[K]]) {
        this.plh = plh;
    }
    
    /************************Async Recovery Methods****************************************/
    public def allocate(vPlace:Long) {
        try {
            plh().lock();
            Console.OUT.println("Activity["+Runtime.activity()+"] Recovering " + here + " received allocation request to replace virtual place ["+vPlace+"] ");
            if (plh().virtualPlaceId == -1) {
                plh().virtualPlaceId = vPlace;
                Console.OUT.println("Activity["+Runtime.activity()+"] Recovering " + here + " allocation request succeeded");
                return true;
            }
            Console.OUT.println("Activity["+Runtime.activity()+"] Recovering " + here + " allocation request failed, already allocated for virtual place ["+plh().virtualPlaceId+"] ");
            return false;
        } finally {
            plh().unlock();
        }
    }
    
    public def initSpare (newActivePlaces:PlaceGroup, vId:Long, deadPlace:Place, deadPlaceSlave:Place) {
        try {
            plh().lock();
            this.replacementHistory.put(deadPlace.id, here.id);
            this.virtualPlaceId = vId;
            this.activePlaces = newActivePlaces;
            this.slave = deadPlaceSlave;
            this.oldSlave = deadPlaceSlave;
        } finally {
            plh().unlock();
        }
    }
    
    public def slaveStoreExists() {
        try {
            lock();
            return slaveStore != null;
        }finally {
            unlock();
        }
    }
    
    public def getMasterStore() {
        if (masterStore == null) {
            throw new TxStorePausedException(here + " TxMasterStore is not initialized yet");
        }
        return masterStore;
    }
    
    public def setMasterStore(m:TxMasterStore[K]) {
        this.masterStore = m;
    }
    
    /*********** ActivePlaces utility methods ****************/
    public def getMasterVirtualId() {
        try {
            lock();
            return (virtualPlaceId -1 + activePlaces.size());
        }finally {
            unlock();
        }
    }
    
    public def getVirtualPlaceId() {
        try {
            lock();
            return virtualPlaceId;
        }finally {
            unlock();
        }
    }
    
    public def getMapping(members:Set[Int]) {
        try {
            lock();
            val map = new HashMap[Int,Int]();
            for (x in members) {
                map.put(x, activePlaces.next(Place(x as Long)).id as Int);
            }
            return map;
        }finally {
            unlock();
        }
    }
    
    
    public def getActivePlaces() {
        try {
            lock();
            assert (activePlaces != null) : here + " bug, activePlaces is null";
            return activePlaces;
        }finally {
            unlock();
        }
    }
    
    public def activePlacesAsString() {
        try {
            lock();
            var str:String = " activePlaces{";
            if (activePlaces != null) {
                for (p in activePlaces)
                    str += p + " : ";
            }
            str += " }";
            return str;
        }finally {
            unlock();
        }
    }
    
    public def getMaster(p:Place) {
        try {
            lock();
            var master:Place = activePlaces.prev(p);
            if (master.id == -1) {
                Console.OUT.println(here + " Master of ("+p+") computed as -1,  replacementHist = " + replacementHistory.getOrElse(p.id, -1));
                val newP = Place(replacementHistory.getOrThrow(p.id));
                master = activePlaces.prev(newP);
                Console.OUT.println(here + " Master of (" + p + ") corrected to " + master );
            }
            return master;
        } finally {
            unlock();
        }
    }

    public def getSlave(p:Place) {
        try {
            lock();
            var slave:Place = activePlaces.next(p);
            if (slave.id == -1) {
                Console.OUT.println(here + " Slave of ("+p+") computed as -1,  replacementHist = " + replacementHistory.getOrElse(p.id, -1));
                val newP = Place(replacementHistory.getOrThrow(p.id));
                slave = activePlaces.next(newP);
                Console.OUT.println(here + " Slave of (" + p + ") corrected to " + slave );
            }
            return slave;
        }finally {
            unlock();
        }
    }
    
    
    public def getSlave(virtualId:Long) {
        try {
            lock();
            val size = activePlaces.size();
            return activePlaces((virtualId+1)%size);
        }finally {
            unlock();
        }
    }
    
    public def getPlace(virtualId:Long) {
        try {
            lock();
            return activePlaces(virtualId);
        }finally {
            unlock();
        }
    }
    
    public def getPlaceIndex(p:Place) {
        val idx = activePlaces.indexOf(p);
        return idx;
    }
        
    /*******************************************/
    public def lock() {
        if (!TxConfig.LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.LOCK_FREE)
            lock.unlock();
    }
    
    public def replace(oldP:Place, newP:Place) {
        try {
            lock();
            val size = activePlaces.size();
            val rail = new Rail[Place](size);
            replacementHistory.put(oldP.id, newP.id);
            for (var i:Long = 0; i< size; i++) {
                if (activePlaces(i).id == oldP.id)
                    rail(i) = newP;
                else
                    rail(i) = activePlaces(i);
            }
            activePlaces = new SparsePlaceGroup(rail);
            
            if (TxConfig.TMREC_DEBUG) {
                var str:String = "";
                for (p in activePlaces)
                    str += p + ",  " ;
                Console.OUT.println("Recovering " + here + " - updated activePlaces to be: " + str);
                Console.OUT.println("Recovering " + here + " - Handshake with new place ["+newP+"] ..." );
            }
            return activePlaces;
        }finally {
            unlock();
        }
    }
    
    static class Replacement {
        public var oldPlace:Place = Place(-1);
        public var newPlace:Place = Place(-1);
    }
    
    public def replaceDeadPlace(dead:Place) {
        if (dead.id == slave.id) //the slave is recovered asynchronously
            return;
        
        val plh = this.plh;
        val me = here;
        val gr = GlobalRef(new Replacement());
        if (dead.isDead()) {
            val knownRep = replacementHistory.getOrElse(dead.id, -1); 
            if ( knownRep == -1) {
                //@Uncounted async
                if (here.id % 2 == 0) {
                    val master = activePlaces.prev(dead);
                    if (master.id != -1) finish at (master) async {
                        val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                        if (rep != -1) {
                            at (gr) async {
                                gr().newPlace = Place(rep);
                            }
                        }
                    }
                    
                    if (gr().newPlace.id == -1) {
                        System.threadSleep(TxConfig.DPE_SLEEP_MS);
                        val slave = activePlaces.next(dead);
                        if (slave.id != -1) finish at (slave) async {
                            val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                            if (rep != -1) {
                                at (gr) async  {
                                    gr().newPlace = Place(rep);
                                }
                            }
                        }
                    }
                } else {
                    
                    val slave = activePlaces.next(dead);
                    if (slave.id != -1) finish at (slave) async {
                        val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                        if (rep != -1) {
                            at (gr) async  {
                                gr().newPlace = Place(rep);
                            }
                        }
                    }
                    if (gr().newPlace.id == -1) {
                        System.threadSleep(TxConfig.DPE_SLEEP_MS);
                        val master = activePlaces.prev(dead);
                        if (master.id != -1) finish at (master) async {
                            val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                            if (rep != -1) {
                                at (gr) async {
                                    gr().newPlace = Place(rep);
                                }
                            }
                        }
                    }
                }
                
                if (gr().newPlace.id != -1) {
                    replace(dead, gr().newPlace);
                }
            }
            else {
                replace(dead, Place(knownRep));
            }
        }
    }
    
    public def replaceDeadPlaces() {
        for (p in activePlaces) {
            if (p.isDead()) {
                replaceDeadPlace(p);
            }
        }
    }
    
    static class ExceptionContainer {
        public var excp:CheckedThrowable;
    }
    
    private def debug (pl:Place, msg:String) {
        Console.OUT.println(pl + " - " + msg);
    }
}