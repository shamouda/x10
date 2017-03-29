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
import x10.compiler.Uncounted;

public class LocalStore(immediateRecovery:Boolean) {
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public transient var masterStore:MasterStore = null;
    public transient var virtualPlaceId:Long = -1; //-1 means a spare place
    public transient var slave:Place;
    public transient var slaveStore:SlaveStore = null;
    public transient var activePlaces:PlaceGroup;
    private var plh:PlaceLocalHandle[LocalStore];
    private transient var heartBeatOn:Boolean;
    private transient var lock:Lock;
    public transient val txList:TransactionsList = new TransactionsList();
    private transient var txDescMap:ResilientNativeMap; //A resilient map for transactions' descriptors
    
    public def this(active:PlaceGroup, immediateRecovery:Boolean) {
    	property(immediateRecovery);
    	lock = new Lock();
    	
        if (active.contains(here)) {
        	this.activePlaces = active;
            this.virtualPlaceId = active.indexOf(here);
            masterStore = new MasterStore(new HashMap[String,Cloneable]());
            if (resilient) {
                slaveStore = new SlaveStore(active.prev(here));
                this.slave = active.next(here);
            }
        } //else, I am a spare place, initialize me using joinAsMaster(...)
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
    }
    
    public def getTxLoggingMap() {
    	if (txDescMap == null)
    		txDescMap = new ResilientNativeMap("_TxDesc_", plh);
    	return txDescMap;
    }
    
    public def setPLH(plh:PlaceLocalHandle[LocalStore]) {
        this.plh = plh;
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
    
    public def getActivePlacesCopy() {
    	try {
    		lock();
    		val rail = new Rail[Place](activePlaces.size());
    		for (var i:Long = 0 ; i < activePlaces.size(); i++) {
    			rail(i) = activePlaces(i);
    		}
    		return new SparsePlaceGroup(rail);
    	}finally {
    		unlock();
    	}
    }
    

    public def replace(dead:Place, spare:Place) {
    	try {
    		lock();
    		val size = activePlaces.size();
    		val rail = new Rail[Place](size);
    		for (var i:Long = 0; i< size; i++) {
    			if (activePlaces(i).id == dead.id)
    				rail(i) = spare;
    			else
    				rail(i) = activePlaces(i);
    		}
    		activePlaces = new SparsePlaceGroup(rail);
    		
    		if (TxConfig.get().TM_DEBUG) {
    			var str:String = "";
    		    for (p in activePlaces)
    		    	str += p + ",  " ;
    			Console.OUT.println(here + " - updated activePlaces to be: " + str);
    		}
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
    		val result = activePlaces == active;
    		if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " - sameActivePlaces returned " + result);
    		return result;
    	}finally {
    		unlock();
    	}
    }
    
    
    public def getMembersIndices(members:PlaceGroup):Rail[Long] {
    	try {
    		lock();
    		val rail = new Rail[Long](members.size());
    		for (var i:Long = 0; i <  members.size(); i++)
    			rail(i) = activePlaces.indexOf(members(i));
    		return rail;
    	} finally {
    		unlock();
    	}
    }
    
    public def getMembers(members:Rail[Long]):PlaceGroup {
    	try {
    		lock();
	        val list = new ArrayList[Place]();
	        for (var i:Long = 0; i < members.size; i++) {
	            if (!activePlaces(members(i)).isDead())
	                list.add(activePlaces(members(i)));
	        }
	        return new SparsePlaceGroup(list.toRail());
    	} finally {
    		unlock();
    	}
    }
    
    /*******************************************/
    public def lock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.get().LOCK_FREE)
            lock.unlock();
    }
    
    /********************Transparent Recovery methods********************************/
    public def asyncSlaveRecovery() {
    	if (!immediateRecovery)
    		return;
    	
    	assert (slave.isDead()) : "bug in LocalStore, calling asyncSlaveRecovery although the slave is alive";
    	assert (virtualPlaceId != -1) : "bug in LocalStore, virtualPlaceId ("+virtualPlaceId+") = -1";
    	
    	if ( masterStore.isActive() ) {
    		 masterStore.pause();
    		@Uncounted async {
    			DistributedRecoveryHelper.recoverSlave(plh);
    		}
    	}
    }
    
}