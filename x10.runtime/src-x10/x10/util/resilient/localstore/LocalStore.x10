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

import x10.util.*;
import x10.util.concurrent.Lock;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.compiler.Ifdef;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.tx.TxDesc;
import x10.util.resilient.localstore.tx.TransactionsList;

public class LocalStore {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public transient var masterStore:MasterStore = null;
    public transient var virtualPlaceId:Long = -1; //-1 means a spare place
    
    /*resilient mode variables*/    
    public transient var slave:Place;
    public transient var slaveStore:SlaveStore = null;
    public transient var activePlaces:PlaceGroup;
    private var plh:PlaceLocalHandle[LocalStore];
    private transient var heartBeatOn:Boolean;
    private transient var lock:Lock;
    
    /*store transactions for statistical purposes*/
    public transient val txList:TransactionsList = new TransactionsList();
    private transient var txDescMap:ResilientNativeMap; //A resilient map for transactions' descriptors
    
    public def this(active:PlaceGroup) {
        if (active.contains(here)) {
        	this.activePlaces = active;
            this.virtualPlaceId = active.indexOf(here);
            masterStore = new MasterStore(new HashMap[String,Cloneable]());
            if (resilient) {
                slaveStore = new SlaveStore(active.prev(here));
                this.slave = active.next(here);
            }
            lock = new Lock();
        }
    }
    
    public def getTxLoggingMap() {
    	if (txDescMap == null)
    		txDescMap = new ResilientNativeMap("_TxDesc_", plh);
    	return txDescMap;
    }
    
    /*used when a spare place joins*/
    public def joinAsMaster (active:PlaceGroup, data:HashMap[String,Cloneable]) {
        assert(resilient);
        this.activePlaces = active;
        this.virtualPlaceId = active.indexOf(here);
        masterStore = new MasterStore(data);
        if (resilient) {
            slaveStore = new SlaveStore(active.prev(here));
            this.slave = active.next(here);
        }
        lock = new Lock();
    }
    
    public def setPLH(plh:PlaceLocalHandle[LocalStore]) {
        this.plh = plh;
    }
    
    public def PLH() = plh;
    
    /*****  Heartbeating from slave to master ******/
    public def startHeartBeat(hbIntervalMS:Long) {
        assert(resilient);
        try {
            lock();
            heartBeatOn = true;
            while (heartBeatOn)
                heartBeatLocked(hbIntervalMS);
        }
        finally {
            unlock();
        }
    }
    
    public def stopHeartBeat() {
        try {
            lock();
            heartBeatOn = false;
        }
        finally {
            unlock();
        }
    }
    
    private def heartBeatLocked(hbIntervalMS:Long) {
    	var i:Long = 0;
        while (heartBeatOn && !slaveStore.master.isDead()) {
            unlock();
            System.threadSleep(hbIntervalMS);                   
            lock();
            
            if (heartBeatOn) {
	            try {
	                finish at (slaveStore.master) async {}
	            }
	            catch(ex:Exception) { break; }            
	            Console.OUT.println(here + " " + (i++) + ") HB>>>>> " + slaveStore.master + " ... OK ");
            }
        }
        
        if (heartBeatOn && slaveStore.master.isDead()) {
        	Console.OUT.println(here + " HB>>>>> " + slaveStore.master + " ... ERROR ");
        	val spare = allocateSparePlace();
        	
        	val newActivePlaces = generateNewActivePlaces(slaveStore.master, spare);
        	val masterOfDeadSlave = activePlaces.prev(slaveStore.master);
        	DistributedRecoveryHelper.recover(plh, spare, masterOfDeadSlave, activePlaces, newActivePlaces);
        	
            activePlaces = newActivePlaces;
            
        	slaveStore.master = spare;
        }
    }
    
    private def allocateSparePlace() {
        val plh = this.plh;
        try {
            plh().lock();
            val nPlaces = Place.numAllPlaces();
            val nActive = plh().activePlaces.size();
            var placeIndx:Long = -1;
            for (var i:Long = nActive; i < nPlaces; i++) {
                if (activePlaces.contains(Place(i)))
                    continue;
                
                var allocated:Boolean = false;
                try {
                    allocated = at (Place(i)) plh().allocate(virtualPlaceId -1);
                }catch(ex:Exception) {
                }
                
                if (!allocated)
                    Console.OUT.println(here + " - Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
                else {
                    Console.OUT.println(here + " - Succeeded to allocate " + Place(i) );
                    placeIndx = i;
                    break;
                }
            }
            assert(placeIndx != -1) : here + " no available spare places to allocate ";
            return Place(placeIndx);
        }
        finally {
            plh().unlock();
        }   
    }
    
    
    private def allocate(vPlace:Long) {
        val plh = this.plh;
        try {
            lock();
            Console.OUT.println(here + " received allocation request to replace virtual place ["+vPlace+"] ");
            if (virtualPlaceId == -1) {
                virtualPlaceId = vPlace;
                Console.OUT.println(here + " allocation request succeeded");
                return true;
            }
            Console.OUT.println(here + " allocation request failed, already allocated for virtual place ["+virtualPlaceId+"] ");
            return false;
        }
        finally {
            unlock();
        }
    }
    
    private def generateNewActivePlaces(deadPlace:Place, newPlace:Place) {
        val rail = new Rail[Place]();
        for (var i:Long = 0; i < activePlaces.size(); i++) {
            if (activePlaces(i).id == deadPlace.id)
                rail(i) = newPlace;
        }
        return new SparsePlaceGroup(rail);
    }
    /**************     ActivePlaces utility methods     ****************/
    public def getVirtualPlaceId() {
        try {
        	lock();
            return virtualPlaceId;
        }finally {
        	unlock();
        }
    }
    
    public def getActivePlaces() {
    	try {
    		lock();
    		return activePlaces;
    	}finally {
    		unlock();
    	}
    }

    public def setActivePlaces(active:PlaceGroup) {
    	try {
    		lock();
    		this.activePlaces = active;
    	}finally {
    		unlock();
    	}
    }
    
    public def getMaster(p:Place) {
    	try {
    		lock();
    	    return activePlaces.prev(p);
    	} finally {
    		unlock();
    	}
    }

    public def getSlave(p:Place) {
    	try {
    		lock();
    	    return activePlaces.next(p);
    	}finally {
    		unlock();
    	}
    }
    
    public def getNextPlace() {
    	try {
    		lock();
    		return activePlaces.next(here);
    	}finally {
    		unlock();
    	}
    }
    
    public def sameActivePlaces(active:PlaceGroup) {
    	try {
    		lock();
    		return activePlaces.equals(active);
    	}finally {
    		unlock();
    	}
    }
    
    
    /*******************************************/
    private def lock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.lock();
    }
    
    private def unlock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.unlock();
    }
}