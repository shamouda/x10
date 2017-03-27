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

public class LocalStore {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public var masterStore:MasterStore = null;
    public var virtualPlaceId:Long = -1; //-1 means a spare place
    
    /*resilient mode variables*/    
    public var slave:Place;
    public var slaveStore:SlaveStore = null;
    
    public var activePlaces:PlaceGroup;
    
    private var plh:PlaceLocalHandle[LocalStore];
    
    private var heartBeatOn:Boolean;

    private transient var lock:Lock;
    
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
        	val spare = allocateSparePlace();
            //recover master:
        	//  allocate NEW_MASTER
        	//  give NEW_MASTER its data
        	//  recover NEW_MASTER's incomplete transactions
        	//  slaveStore.master = {NEW_MASTER}
        	Console.OUT.println(here + " HB>>>>> " + slaveStore.master + " ... ERROR ");
        }
    }
    
    /*****  Recover master ******/
    private def allocateSparePlace() {
    	try {
            lock();
            val nPlaces = Place.numAllPlaces();
            val nActive = activePlaces.size();
            val masterIndex = 
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
                    Console.OUT.println(here + "- Failed to allocate " + Place(i) + ", is it dead? " + Place(i).isDead());
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
            unlock();
        }	
    }
    
    public def allocate(vPlace:Long) {
        try {
            lock.lock();
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
            lock.unlock();
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
    
    /*******************************************/
    private def lock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.lock();
    }
    
    private def unlock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.unlock();
    }
    /********************Resilient Store Helper Methods *************************/
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
    		return new SparsePlaceGroup(new Rail[Place](activePlaces.size(), (i:Long) => activePlaces(i)));
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
}