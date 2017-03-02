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

package x10.util.resilient.localstore.tx;

public class MapBucket(id:Long, lockMode:Int) implements x10.io.Unserializable {
	public val map = new HashMap[String,MemoryUnit]();
	public val locks = new HashMap[String,TxLock]();
	
	public def this(id:Long) {
		property(id);
		map = new HashMap[String,MemoryUnit]();
		locks = new HashMap[String,TxLock]();
	}
	
	public def this(id:Long, serMap:SerializableMapBucket) {
		property(id);
		map = serMap.map; 
		locks = new HashMap[String,TxLock]();
	}
	
	public def getSerializableBucket() {
		return new SerializableMapBucket(id, map);
	}
	
	public def get(k:String) {
		
		if (locks.getOrElse)
	}
	
	
	private def getOrAddLock(k:String) {
		var lock:TxLock = locks.getOrElse(k, null);
	    if (lock == null){
	        if (lockMode == TxManager.LOCK_BLOCKING) 
	        	lock = new TxLockCREWBlocking();
	        else if (lockMode == TxManager.LOCK_NON_BLOCKING)
	        	lock = new TxLockCREW();
	        else
	        	lock = null;
	        
	        if (lock != null)
	        	locks.put(k, lock);
	    }
	    return lock;
	}
	
	public def tryLockRead(k:String) {
		
	}
	
	public def tryLockWrite(k:String) {
		
	}
	
	
}