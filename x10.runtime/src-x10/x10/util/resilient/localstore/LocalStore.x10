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
    private static val HB_INTERVAL_MS = System.getenv("HB_INTERVAL_MS") == null ? 1000 : Long.parseLong(System.getenv("HB_INTERVAL_MS"));
    
       
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
    
    public def allocate(vPlace:Long) {
        try {
            lock.lock();
            if (virtualPlaceId == -1) {
                virtualPlaceId = vPlace;
                return true;
            }
            return false;
        }
        finally {
            lock.unlock();
        }
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
    
    private def heartBeatLocked(hbIntervalMS:Long) {
        while (heartBeatOn && !slaveStore.master.isDead()) {
            unlock();
            System.threadSleep(HB_INTERVAL_MS);                   
            lock();
            
            try {
                finish at (slaveStore.master) async {}
            }
            catch(ex:Exception) { heartBeatOn = false; }
        }
        
        if (slaveStore.master.isDead()) {
            //recover masters
        }
        
        heartBeatOn = true;
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

    /*******************************************/
    
    private def lock(){
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.lock();
    }
    
    private def unlock(){
        if (!TxConfig.getInstance().LOCK_FREE)
            lock.unlock();
    }
}