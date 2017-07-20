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
import x10.compiler.Uncounted;
import x10.util.resilient.localstore.recovery.DistributedRecoveryHelper;
import x10.util.resilient.localstore.tx.logging.TxDescManager;
import x10.util.resilient.localstore.tx.StorePausedException;
import x10.util.concurrent.Condition;
import x10.compiler.Immediate;

public class LocalStore[K] {K haszero} {
	public val immediateRecovery:Boolean;
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private transient var masterStore:MasterStore[K] = null;
    public transient var virtualPlaceId:Long = -1; //-1 means a spare place
    public transient var slave:Place;
    public transient var oldSlave:Place;
    public transient var slaveStore:SlaveStore[K] = null;
    public transient var activePlaces:PlaceGroup;
    private var plh:PlaceLocalHandle[LocalStore[K]];
    private transient var lock:Lock;
    public transient val stat:TxPlaceStatistics;
    public transient var txDescManager:TxDescManager[K];
    
    private val replacementHistory = new HashMap[Long, Long] ();
    
    public def this(active:PlaceGroup, immediateRecovery:Boolean) {
        this.immediateRecovery = immediateRecovery;
        lock = new Lock();
        if (TxConfig.ENABLE_STAT)
            stat = new TxPlaceStatistics();
        else
            stat = null;
        
        if (active.contains(here)) {
            this.activePlaces = active;
            this.virtualPlaceId = active.indexOf(here);
            masterStore = new MasterStore[K](new HashMap[K,Cloneable](), immediateRecovery);
            if (resilient && !TxConfig.DISABLE_SLAVE) {
                slaveStore = new SlaveStore[K]();
                this.slave = active.next(here);
                this.oldSlave = this.slave;
                ConditionsList.get().setSlave(this.slave.id);
            }
        } //else, I am a spare place, initialize me using joinAsMaster(...)
    }
    
    public def setPLH(plh:PlaceLocalHandle[LocalStore[K]]) {
        this.plh = plh;
        this.txDescManager = new TxDescManager[K]( plh );
    }
    
    /*CentralizedRecovery: used when a spare place joins*/
    public def joinAsMaster (active:PlaceGroup, data:HashMap[K,Cloneable]) {
        plh().lock();
        assert(resilient && virtualPlaceId == -1) : "virtualPlaceId  is not -1 (="+virtualPlaceId+") ";
        this.activePlaces = active;
        this.virtualPlaceId = active.indexOf(here);
        masterStore = new MasterStore(data, immediateRecovery);
        slaveStore = new SlaveStore[K]();
        this.slave = active.next(here);
        ConditionsList.get().setSlave(this.slave.id);
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
    
    
    public def handshake (oldActivePlaces:PlaceGroup, vId:Long, deadPlace:Place) {
        try {
            plh().lock();
            this.replacementHistory.put(deadPlace.id, here.id);
            
            this.virtualPlaceId = vId;
            //update other places according to the version of active places that my master is aware of
            val newAddedPlaces = new GlobalRef[ArrayList[PlaceChange]](new ArrayList[PlaceChange]());
            val me = here;
            val activeSize = oldActivePlaces.size();
            try {
                 finish for (p in oldActivePlaces) {
                     val expectedSlave = oldActivePlaces.next(p);
                     if (p.isDead() || expectedSlave.id == me.id /*p is my master*/ || p.id == me.id /* p is me*/)
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
        try {
            lock();
            val size = activePlaces.size();
            val rail = new Rail[Place](size);
            val oldPlace = activePlaces(virtualId);
            replacementHistory.put(oldPlace.id, spare.id);
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
                Console.OUT.println("Recovering " + here + " - Handshake with new place ["+spare+"]  at virtualId ["+virtualId+"] ..." );
            }
            return activePlaces;
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
                ConditionsList.get().setSlave(slave.id);
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
    
    public def getTxMembersIncludingDead(virtualMembers:Rail[Long]):TxMembers {
        try {
            lock();
            val size = virtualMembers.size;
            val virtual = new Rail[Long](size);
            val places = new Rail[Long](size);
            for (var i:Long = 0; i < size; i++) {
            	virtual(i) = virtualMembers(i);
            	places(i) = activePlaces(virtualMembers(i)).id;
            }
            return new TxMembers(virtual, places);
        } finally {
            unlock();
        }
    }
    
    public def getTxMembersIncludingDead(virtualMembers:GrowableRail[Long]):TxMembers {
        try {
            lock();
            val size = virtualMembers.size();
            val virtual = new Rail[Long](size);
            val places = new Rail[Long](size);
            for (var i:Long = 0; i < size; i++) {
            	virtual(i) = virtualMembers(i);
            	places(i) = activePlaces(virtualMembers(i)).id;
            }
            return new TxMembers(virtual, places);
        } finally {
            unlock();
        }
    }
    
    
    public def getPlaceIndex(p:Place) {
        return activePlaces.indexOf(p);
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
        if (!immediateRecovery || !slave.isDead())
            return;
        
        assert (virtualPlaceId != -1) : "bug in LocalStore, virtualPlaceId ("+virtualPlaceId+") = -1";
        
        if ( slave.isDead() && masterStore.isActive() ) {
             masterStore.pausing();
            @Uncounted async {
                DistributedRecoveryHelper.recoverSlave(plh);
            }
        }
    }

    //synchronized version of asyncSlaveRecovery
    public def recoverSlave(spare:Place) {
        Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+")");
        if (immediateRecovery || !slave.isDead())
            return;
        
        assert (virtualPlaceId != -1) : "bug in LocalStore, virtualPlaceId ("+virtualPlaceId+") = -1";
        
        if ( slave.isDead() && masterStore.isActive() ) {
             Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+") master is active");
             masterStore.pausing();
             DistributedRecoveryHelper.recoverSlave(plh, spare, -1);
        }
        else
            Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+") master is not active");
    }
    
    
    public def getMasterStore() {
        if (masterStore == null) {
            throw new StorePausedException(here + " MasterStore is not initialized yet");
        }
        return masterStore;
    }
    
    public def setMasterStore(m:MasterStore[K]) {
        this.masterStore = m;
    }
    /*
    public def releaseAllConditions() {
        try {
            lock();
            Console.OUT.println(here + " releaseAllConditions CONDSIZE= " + slaveCondList.size());
            if (virtualPlaceId != -1 && slave.isDead()) {
                for (cond in slaveCondList){
                    cond.release();
                }
                slaveCondList.clear();
            }
        } finally {
            unlock();
        }
    }*/
    
    public def runImmediateAtSlave(cl:()=>void):void {
        if (slave.isDead())
            throw new DeadPlaceException(slave);
        
        val h = here;
        val cond = new Condition();
        val condGR = GlobalRef[Condition](cond); 
        val exc = GlobalRef(new Cell[CheckedThrowable](null));
        at (slave) @Immediate("finish_resilient_low_level_at_out") async {
            try {
                cl();
                at (condGR) @Immediate("finish_resilient_low_level_at_back") async {
                    condGR().release();
                }
            } catch (t:Exception) {
                at (condGR) @Immediate("finish_resilient_low_level_at_back_exc") async {
                    exc()(t);
                    condGR().release();
                };
            }
        };
        ConditionsList.get().add(cond);
        try {
            if (Runtime.NUM_IMMEDIATE_THREADS == 0n) Runtime.increaseParallelism();
            cond.await();
        }finally {
            if (Runtime.NUM_IMMEDIATE_THREADS == 0n) Runtime.decreaseParallelism(1n);
        }
        // Unglobalize objects
        condGR.forget();
        exc.forget();
        ConditionsList.get().remove(cond);
        val t = exc()();
        if (t != null) {
            Runtime.throwCheckedWithoutThrows(t);
        }
        if (slave.isDead())
            throw new DeadPlaceException(slave);
    }
    
    private def debug (pl:Place, msg:String) {
        Console.OUT.println(pl + " - " + msg);
    }
    
}