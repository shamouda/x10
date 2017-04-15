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
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.util.resilient.localstore.tx.TransactionsList;
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.recovery.DistributedRecoveryHelper;
import x10.util.resilient.localstore.tx.logging.TxDescManager;

public class LocalStore(immediateRecovery:Boolean) {
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public transient var masterStore:MasterStore = null;
    public transient var virtualPlaceId:Long = -1; //-1 means a spare place
    public transient var slave:Place;
    public transient var oldSlave:Place;
    public transient var slaveStore:SlaveStore = null;
    public transient var activePlaces:PlaceGroup;
    private var plh:PlaceLocalHandle[LocalStore];
    private transient var heartBeatOn:Boolean;
    private transient var lock:Lock;
    public transient val txList:TransactionsList = new TransactionsList();
    
    public transient var txDescManager:TxDescManager; //A resilient map for transactions' descriptors
    
    public def this(active:PlaceGroup, immediateRecovery:Boolean) {
        property(immediateRecovery);
        lock = new Lock();
        
        if (active.contains(here)) {
            this.activePlaces = active;
            this.virtualPlaceId = active.indexOf(here);
            masterStore = new MasterStore(new HashMap[String,Cloneable](), immediateRecovery);
            if (resilient && !TxConfig.get().DISABLE_SLAVE) {
                slaveStore = new SlaveStore();
                this.slave = active.next(here);
                this.oldSlave = this.slave;
            }
        } //else, I am a spare place, initialize me using joinAsMaster(...)
    }
    
    /*CentralizedRecovery: used when a spare place joins*/
    public def joinAsMaster (active:PlaceGroup, data:HashMap[String,Cloneable]) {
        plh().lock();
        assert(resilient && virtualPlaceId == -1) : "virtualPlaceId  is not -1 (="+virtualPlaceId+") ";
        this.activePlaces = active;
        this.virtualPlaceId = active.indexOf(here);
        masterStore = new MasterStore(data, immediateRecovery);
        slaveStore = new SlaveStore();
        this.slave = active.next(here);
        this.oldSlave = this.slave;
        plh().unlock();
    }
    
    /************************Distributed Recovery Methods****************************************/
    
    public def allocate(vPlace:Long) {
        try {
            plh().lock();
            Console.OUT.println("Recovering " + here + " received allocation request to replace virtual place ["+vPlace+"] ");
            if (plh().virtualPlaceId == -1) {
                plh().virtualPlaceId = vPlace;
                Console.OUT.println("Recovering " + here + " allocation request succeeded");
                return true;
            }
            Console.OUT.println("Recovering " + here + " allocation request failed, already allocated for virtual place ["+plh().virtualPlaceId+"] ");
            return false;
        } finally {
            plh().unlock();
        }
    }
    
    public static struct PlaceChange {
        public val virtualPlaceId:Long;
        public val newPlace:Place;
        
        def this(virtualPlaceId:Long, newPlace:Place) {
            this.virtualPlaceId = virtualPlaceId;
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
    
    
    public def handshake (oldActivePlaces:PlaceGroup, vId:Long) {
        try {
            plh().lock();
            this.virtualPlaceId = vId;
            //update other places according to the version of active places that my master is aware of
            val newAddedPlaces = new GlobalRef[ArrayList[PlaceChange]](new ArrayList[PlaceChange]());
            val me = here;
            try {
                 finish for (p in oldActivePlaces) {
                     val expectedSlave = oldActivePlaces.next(p);
                     val activeSize = oldActivePlaces.size();
                     if (p.isDead() || expectedSlave.id == here.id /*p is my master*/ || p.id == here.id /* p is me*/)
                         continue;
                     at (p) async {
                         //at handshare receiver
                         plh().replace(vId, me);
                         if (plh().slave.id != expectedSlave.id){
                             val slaveVId = (plh().virtualPlaceId + 1)%activeSize;
                             val newSlave = plh().slave;
                             at (newAddedPlaces) {
                                 atomic newAddedPlaces().add(new PlaceChange(slaveVId, newSlave));
                             }
                         }
                     }
                 }
            }catch(e:Exception) {/*ignore dead places at this point*/}
        
            this.activePlaces = oldActivePlaces;
            if (newAddedPlaces().size() > 0) {
                Console.OUT.println("Recovering " + here + " My master gave me outdated information");
                //my master was not aware of some place changes
                for (change in newAddedPlaces()) {
                    plh().replace(change.virtualPlaceId, change.newPlace);
                }
            }
        } finally {
            plh().unlock();
        }
    }
    
    public def setPLH(plh:PlaceLocalHandle[LocalStore]) {
        this.plh = plh;
        this.txDescManager = new TxDescManager(new ResilientNativeMap("_TxDesc_", plh));
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
    
    public def getPreviousVirtualPlaceId() {
        try {
            lock();
            return (virtualPlaceId -1 + activePlaces.size());
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
    

    public def replace(virtualId:Long, spare:Place) {
        //Console.OUT.println(here + " replacing : vId["+virtualId+"] spare["+spare+"] ..." );
        try {
            lock();
            val size = activePlaces.size();
            val rail = new Rail[Place](size);
            for (var i:Long = 0; i< size; i++) {
                if (virtualId == i)
                    rail(i) = spare;
                else
                    rail(i) = activePlaces(i);
            }
            activePlaces = new SparsePlaceGroup(rail);
            
            if (TxConfig.get().TM_DEBUG) {
                var str:String = "";
                for (p in activePlaces)
                    str += p + ",  " ;
                Console.OUT.println("Recovering " + here + " - updated activePlaces to be: " + str);
            }
            return activePlaces;
        }finally {
            unlock();
            Console.OUT.println("Recovering " + here + " - Handshake with new place ["+spare+"]  at virtualId ["+virtualId+"] ..." );
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
    
    
    public def getSlave(virtualId:Long) {
        try {
            lock();
            val size = activePlaces.size();
            return activePlaces((virtualId+1)%size);
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
    
    public def nextPlaceChange() {
        try {
            lock();
            if (oldSlave.id != slave.id) {
                oldSlave = slave;
                return new SlaveChange(true, slave);
            }
            else
                return new SlaveChange(false, slave);
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
    
    public def getPlace(virtualId:Long) {
        try {
            lock();
            return activePlaces(virtualId);
        }finally {
            unlock();
        }
    }
    
    public def physicalToVirtual(members:PlaceGroup):Rail[Long] {
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
    
    public def getTxMembers(virtualMembers:Rail[Long], includeDead:Boolean):TxMembers {
        try {
            lock();
            var str:String = "";
            var str1:String = "";
            val members = new TxMembers(virtualMembers.size);
            for (var i:Long = 0; i < virtualMembers.size; i++) {
                str1 += virtualMembers(i) + " ";
                val pl = activePlaces(virtualMembers(i));
                if (includeDead || !pl.isDead()){
                    members.addPlace(virtualMembers(i), pl);
                    str += pl + " ";
                }
            }
            if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " - getTxMembers virtual["+str1+"] physical["+str+"]");
            return members;
        } finally {
            unlock();
        }
    }
    
    public def physicalToVirtual(p1:Place, p2:Place) {
        try {
            lock();
            val indx1 = activePlaces.indexOf(p1);
            val indx2 = activePlaces.indexOf(p2);
            val rail = new Rail[Long](2);
            rail(0) = indx1;
            rail(1) = indx2;
            return rail;
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
             masterStore.pausing();
            @Uncounted async {
                DistributedRecoveryHelper.recoverSlave(plh);
            }
        }
    }

    //synchronized version of asyncSlaveRecovery
    public def recoverSlave(spare:Place) {
        Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+")");
        if (immediateRecovery)
            return;
        
        assert (slave.isDead()) : "bug in LocalStore, calling asyncSlaveRecovery although the slave is alive";
        assert (virtualPlaceId != -1) : "bug in LocalStore, virtualPlaceId ("+virtualPlaceId+") = -1";
        
        if ( masterStore.isActive() ) {
             Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+") master is active");
             masterStore.pausing();
             DistributedRecoveryHelper.recoverSlave(plh, spare, -1);
        }
        else
            Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+") master is not active");
    }
    
}