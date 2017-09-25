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
    public transient var slaveStore:SlaveStore[K] = null;
    private var plh:PlaceLocalHandle[LocalStore[K]];
    private transient var lock:Lock;
    public transient val stat:TxPlaceStatistics;
    public transient var txDescManager:TxDescManager[K];

    
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
    
    public def this(active:PlaceGroup, immediateRecovery:Boolean) {
        this.immediateRecovery = immediateRecovery;
        lock = new Lock();
        if (TxConfig.ENABLE_STAT)
            stat = new TxPlaceStatistics();
        else
            stat = null;
        this.activePlaces = active;
        if (active.contains(here)) {
            virtualPlaceId = active.indexOf(here);
            masterStore = new MasterStore[K](new HashMap[K,Cloneable](), immediateRecovery);
            if (resilient && !TxConfig.DISABLE_SLAVE) {
                slaveStore = new SlaveStore[K]();
                slave = active.next(here);
                oldSlave = this.slave;
                ConditionsList.get().setSlave(this.slave.id);
            }
        } //else, I am a spare place
    }
    
    public def setPLH(plh:PlaceLocalHandle[LocalStore[K]]) {
        this.plh = plh;
        this.txDescManager = new TxDescManager[K]( plh );
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
    
    public def initSpare (newActivePlaces:PlaceGroup, vId:Long, deadPlace:Place) {
        try {
            plh().lock();
            this.replacementHistory.put(deadPlace.id, here.id);
            this.virtualPlaceId = vId;
            this.activePlaces = newActivePlaces;
        } finally {
            plh().unlock();
        }
    }
    
    /**************     ActivePlaces utility methods     ****************/
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
        val idx = activePlaces.indexOf(p);
        if (idx == -1) {
            val gr = GlobalRef(new Replacement());
            finish async at (p) {
                val iter = plh().replacementHistory.keySet().iterator();
                while (iter.hasNext()) {
                    val key = iter.next();
                    val value = plh().replacementHistory.getOrThrow(key);
                    if (value == here.id) {
                        async at (gr) {
                            gr().oldPlace = Place(key);
                            gr().newPlace = here;
                        }
                    }
                }
            }
            replace(gr().oldPlace, gr().newPlace);
        }
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
    public def recoverSlave(deadPlace:Place, spare:Place) {
        Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+")");
        if (immediateRecovery || !slave.isDead())
            return;
        
        assert (virtualPlaceId != -1) : "bug in LocalStore, virtualPlaceId ("+virtualPlaceId+") = -1";
        
        if ( slave.isDead() && masterStore.isActive() ) {
             Console.OUT.println(here + " LocalStore.recoverSlave(spare="+spare+") master is active");
             masterStore.pausing();
             DistributedRecoveryHelper.recoverSlave(plh, deadPlace, spare, -1);
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
            
            if (TxConfig.get().TM_DEBUG) {
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
        val gr = GlobalRef(new Replacement());
        if (dead.isDead()) {
            val knownRep = replacementHistory.getOrElse(dead.id, -1); 
            if ( knownRep == -1) {
                //@Uncounted async 
                {
                    val master = activePlaces.prev(dead);
                    finish async at (master) {
                        val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                        if (rep != -1) {
                            async at (gr) {
                                gr().newPlace = Place(rep);
                            }
                        }
                    }
                    
                    if (gr().newPlace.id == -1) {
                        val slave = activePlaces.next(dead);
                        finish async at (slave) {
                            val rep = plh().replacementHistory.getOrElse(dead.id, -1);
                            if (rep != -1) {
                                async at (gr) {
                                    gr().newPlace = Place(rep);
                                }
                            }
                        }
                    }
                }
            }
            else {
                replace(dead, Place(knownRep));
            }
        }
    }
    
    static class ExceptionContainer {
        public var excp:CheckedThrowable;
    }

    public def runImmediateAtSlave(cl:()=>void):void {
        if (slave.isDead())
            throw new DeadPlaceException(slave);
        
        val h = here;
        val cond = new Condition();
        val condGR = GlobalRef[Condition](cond); 
        val exc = GlobalRef[ExceptionContainer](new ExceptionContainer());
        at (slave) @Immediate("finish_resilient_low_level_at_out") async {
            try {
                cl();
                at (condGR) @Immediate("finish_resilient_low_level_at_back") async {
                    condGR().release();
                }
            } catch (t:Exception) {
                at (condGR) @Immediate("finish_resilient_low_level_at_back_exc") async {
                    exc().excp = t;
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
        val t = exc().excp;
        if (t != null) {
            Runtime.throwCheckedWithoutThrows(t);
        }
        if (slave.isDead())
            throw new DeadPlaceException(slave);
    }
    
    private def debug (pl:Place, msg:String) {
        Console.OUT.println(pl + " - " + msg);
    }
    
    
    /***********************don't change************************************/
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
                         plh().replace(deadPlace, me);
                         if (plh().slave.id != expectedSlave.id){
                             val oldSlave = expectedSlave;
                             val newSlave = plh().slave;
                             at (newAddedPlaces) {
                                 atomic newAddedPlaces().add(new PlaceChange(oldSlave, newSlave));
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
                    plh().replace(change.oldPlace, change.newPlace);
                }
            }
        } finally {
            plh().unlock();
        }
    }
}