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
    
    /*store transactions for statistical purposes*/
    public val txList = new TransactionsList();
    private var txDescMap:ResilientNativeMap; //A resilient map for transactions' descriptors
    
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
            //recover master:
        	//  allocate NEW_MASTER
        	//  give NEW_MASTER its data
        	//  recover NEW_MASTER's incomplete transactions
        	//  slaveStore.master = {NEW_MASTER}
        	
        	val newActivePlaces = generateNewActivePlaces(slaveStore.master, spare);
        	 /*complete transactions whose coordinator died*/
            recoverTransactions(spare);
            
            recoverMasters(spare, newActivePlaces);
            
            recoverSlaves(spare, activePlaces.prev(slaveStore.master)); //todo: pause before copying
            
            activePlaces = newActivePlaces;
            
        	slaveStore.master = spare;
        }
        
        val active = activePlaces;
        activePlaces.broadcastFlat(()=> { 
        	setActivePlaces(active);
        });
    }
    
    /*****  Recover master ******/
    private def allocateSparePlace() {
    	val plh = this.plh;
    	try {
            lock();
            val nPlaces = Place.numAllPlaces();
            val nActive = activePlaces.size();
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
    
    public def generateNewActivePlaces(deadPlace:Place, newPlace:Place) {
    	val rail = new Rail[Place]();
    	for (var i:Long = 0; i < activePlaces.size(); i++) {
    		if (activePlaces(i).id == deadPlace.id)
    			rail(i) = newPlace;
    	}
    	return new SparsePlaceGroup(rail);
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

    private def recoverTransactions(spare:Place) {
        Console.OUT.println(here + " - recoverTransactions started");
        val plh = this.plh; // don't capture this in at!
        finish {
            Console.OUT.println(here + " - recoverTransactions deadPlace["+slaveStore.master+"] moving to its slave["+here+"] ");
            
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
                            val map = new ResilientNativeMap(txDesc.mapName, plh);
                            if (TM_DEBUG) Console.OUT.println(here + " - recovering txdesc " + txDesc);
                            val tx = map.restartGlobalTransaction(txDesc);
                            if (txDesc.status == TxDesc.COMMITTING) {
                                if (TM_DEBUG) Console.OUT.println(here + " - recovering Tx["+tx.id+"] commit it");
                                tx.commit(true); //ignore phase one
                            }
                            else if (txDesc.status == TxDesc.STARTED) {
                                if (TM_DEBUG) Console.OUT.println(here + " - recovering Tx["+tx.id+"] abort it");
                                tx.abort();
                            }
                        }
                    }
                }
            }
            
            if (TxConfig.getInstance().TM_REP.equals("lazy"))
                applySlaveTransactions();
        }
        Console.OUT.println("recoverTransactions completed");
    }
    
    private def recoverMasters(spare:Place, newActivePlaces:PlaceGroup) {
        val plh = this.plh; // don't capture this in at!
        finish {
            val map = plh().slaveStore.getSlaveMasterState();
            at (spare) async {
                plh().joinAsMaster(newActivePlaces, map);
            }
        }
    }

    private def recoverSlaves(spare:Place, masterOfDeadSlave:Place) {
        val plh = this.plh; // don't capture this in at!
        val vPlaceId = activePlaces.indexOf(masterOfDeadSlave);
        finish {
            at (masterOfDeadSlave) async {
                val masterState = plh().masterStore.getState().getKeyValueMap();
                at (spare) {
                    plh().slaveStore.addMasterPlace(vPlaceId, masterState);
                }
                plh().slave = spare;
            }
        }
    }

    private def applySlaveTransactions() {
    	val plh = this.plh;
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
                    var pl:Place = activePlaces(placeIndex);
                    var master:Boolean = true;
                    if (pl.isDead()){
                        pl = activePlaces.next(pl);
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