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

import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicLong;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;

public class MasterStore {
    /*Each map has an object of TxManager (same object even after failures)*/
    private val maps:HashMap[String,TxManager];
    private val lock:Lock;
    private val sequence:AtomicLong;
    private val futureSequence:AtomicLong;

    public static val TX_FACTOR=1000000;
    
    //used for original active places joined before any failured
    public def this() {
        this.maps = new HashMap[String,TxManager]();
        this.lock = new Lock(); 
        this.sequence = new AtomicLong();
        this.futureSequence = new AtomicLong();
    }
    
    //used when a spare place is replacing a dead one
    public def this(masterMaps:HashMap[String,HashMap[String,Cloneable]]) {
        this.maps = new HashMap[String,TxManager]();
        this.lock = new Lock(); 
        this.sequence = new AtomicLong();
        this.futureSequence = new AtomicLong();
        
        if (masterMaps != null) {
        	val iter = masterMaps.keySet().iterator();
        	while (iter.hasNext()) {
        		val mapName = iter.next();
        		val mapData = new MapData(mapName, masterMaps.getOrThrow(mapName));
        		this.maps.put(mapName, TxManager.make(mapData));
        	}
        }
    }    
    
    public def addFuture(mapName:String, id:Long, future:Future[Any]) {
        getTxManager(mapName).addFuture(id, future);
    }
    
    private def getTxManager(mapName:String) {
        try {
            lock.lock();
            var mgr:TxManager = maps.getOrElse(mapName, null);
            if (mgr == null) {
                mgr = TxManager.make(mapName);
                maps.put(mapName, mgr);
            }
            return mgr;
        }
        finally {
            lock.unlock();
        }
    }
    
    public def getTxCommitLog(mapName:String, id:Long) {
        return getTxManager(mapName).getTxCommitLog(id);
    }
    
    public def get(mapName:String, id:Long, key:String):Cloneable {
        return getTxManager(mapName).get(id, key);
    }
    
    public def put(mapName:String, id:Long, key:String, value:Cloneable):Cloneable {
        return getTxManager(mapName).put(id, key, value);
    }
    
    public def delete(mapName:String, id:Long, key:String):Cloneable {
        return getTxManager(mapName).delete(id, key);
    }
    
    public def validate(mapName:String, id:Long) {
        getTxManager(mapName).validate(id);
    }
    
    public def commit(mapName:String, id:Long) {
        getTxManager(mapName).commit(id);
    }
    
    public def commit(log:TxLog) {
        getTxManager(log.mapName).commit(log);
    }
    
    public def abort(mapName:String, id:Long) {
        getTxManager(mapName).abort(id);
    }
    
    public def keySet(mapName:String, id:Long) {
        return getTxManager(mapName).keySet(id);
    }
    
    public def resetState(mapName:String) {
        getTxManager(mapName).resetState();
    }
    
    public def getState(mapName:String) {
        return getTxManager(mapName).getState();
    }
    
    public def getState():SlaveMasterState {
        var state:SlaveMasterState = null;
        try {
            lock.lock();
            val tmp = new HashMap[String,HashMap[String,Cloneable]]();            
            val iter = maps.keySet().iterator();
            while (iter.hasNext()) {
            	val mapName = iter.next();
            	val mapData = maps.getOrThrow(mapName).data;
            	tmp.put(mapName, mapData.getKeyValueMap());
            }            
            state = new SlaveMasterState(tmp);
        }
        finally {
            lock.unlock();
        }
        return state;
    }
    
    public def getNextTransactionId() {
        return ( (here.id + 1) * TX_FACTOR) + sequence.incrementAndGet();
    }
    
    public def getNextFutureId() {
        return futureSequence.incrementAndGet();
    }
    
    /*Lock based method*/
    public def lock(mapName:String, id:Long, key:String) {
         (getTxManager(mapName) as TxManager_LockBased).lock(id, key);
    }
    
    public def unlock(mapName:String, id:Long, key:String) {
        (getTxManager(mapName) as TxManager_LockBased).unlock(id, key);
    }
    
    public def getLocked(mapName:String, id:Long, key:String):Cloneable {
        return getTxManager(mapName).get(id, key);
    }
    
    public def deleteLocked(mapName:String, id:Long, key:String):Cloneable {
        return getTxManager(mapName).delete(id, key);
    }
    
    public def putLocked(mapName:String, id:Long, key:String, value:Cloneable):Cloneable {
        return getTxManager(mapName).put(id, key, value);
    }
}
