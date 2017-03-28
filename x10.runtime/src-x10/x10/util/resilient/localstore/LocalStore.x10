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
    private transient val immediateRecovery:Boolean;
    public def this(active:PlaceGroup, immediateRecovery:Boolean) {
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
        this.immediateRecovery = immediateRecovery;
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
    
   	//DistributedRecoveryHelper.recover(plh, spare, masterOfDeadSlave, activePlaces, newActivePlaces);
   	
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
    public def lock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.lock();
    }
    
    public def unlock() {
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.unlock();
    }
}