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

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.Set;
import x10.util.HashSet;
import x10.util.resilient.localstore.TxConfig;
import x10.util.Map.Entry;
import x10.compiler.NonEscaping;

public class SafeBucketHashMap[K,V] {V haszero} implements x10.io.Unserializable  {
	
	private val buckets:Rail[Bucket[K,V]];
	private val bucketsCnt:Long;
	
    static class Bucket[Key,Value] {Value haszero} {
        val bucketMap = new HashMap[Key,Value]();
        val lock:Lock;
        public def this(initLock:Boolean) {
        	if (initLock)
        		lock = new Lock();
        	else
        		lock = null;
        }
        public def lock() {
        	lock.lock();
        }
        public def unlock() {
        	lock.unlock();
        }
    }
    
    public def this(bucketsCnt:Long) {
        val initLock = !TxConfig.getInstance().LOCK_FREE;
        buckets = new Rail[Bucket[K,V]](bucketsCnt, (i:Long)=> new Bucket[K,V](initLock));
        this.bucketsCnt = bucketsCnt;
    }
    
    /***************************Safe methods**********************************/
    public def containsKeySafe(k:K):Boolean {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.containsKey(k);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }
    
    public def getSafe(k:K):V {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.get(k);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }

    public def getOrElseSafe(k:K, orelse:V):V {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.getOrElse(k, orelse);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }
    
    public def getOrThrowSafe(k:K):V {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.getOrThrow(k);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }

    public def putSafe(k:K, v:V):V {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.put(k, v);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }

    public def deleteSafe(k:K):boolean {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.delete(k);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }
    
    public def removeSafe(k:K):V {
        var bucket:Bucket[K,V] = null;
        try {
        	bucket = lockInternal(k);
    		return bucket.bucketMap.remove(k);
    	}
    	finally {
    		if (bucket != null)
    			unlockInternal(bucket);
    	}
    }

    public def clearSafe():void {
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		buckets(i).lock();
    		buckets(i).bucketMap.clear();
    		buckets(i).unlock();
    	}
    }
    
    public def keySetSafe():Set[K] {
    	val set = new HashSet[K]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		buckets(i).lock();
    		set.addAll(buckets(i).bucketMap.keySet());
    		buckets(i).unlock();
    	}
    	return set;
    }

    public def entriesSafe():Set[Entry[K,V]] {
    	val set = new HashSet[Entry[K,V]]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		buckets(i).lock();
    		set.addAll(buckets(i).bucketMap.entries());
    		buckets(i).unlock();
    	}
    	return set;
    }
    
    public def sizeSafe():Long {
    	var s:Long = 0;
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		buckets(i).lock();
    		s += buckets(i).bucketMap.size();
    		buckets(i).unlock();
    	}
    	return s;
    }
    
    /***************************Unsafe methods**********************************/
    public def containsKeyUnsafe(k:K):Boolean {
    	val indx = hashInternal(k) % bucketsCnt;
    	return buckets(indx).bucketMap.containsKey(k);
    }

    public def getUnsafe(k:K):V {
    	val indx = hashInternal(k) % bucketsCnt;
   		return buckets(indx).bucketMap.get(k);
    }

    public def getOrElseUnsafe(k:K, orelse:V):V {
    	val indx = hashInternal(k) % bucketsCnt;
   		return buckets(indx).bucketMap.getOrElse(k, orelse);
    }

    public def getOrThrowUnsafe(k:K):V {
    	val indx = hashInternal(k) % bucketsCnt;
    	return buckets(indx).bucketMap.getOrThrow(k);
    }

    public def putUnsafe(k:K, v:V):V {
    	val indx = hashInternal(k) % bucketsCnt;
    	return buckets(indx).bucketMap.put(k, v);
    }

    public def deleteUnsafe(k:K):boolean {
    	val indx = hashInternal(k) % bucketsCnt;
   		return buckets(indx).bucketMap.delete(k);
    }

    public def removeUnsafe(k:K):V {
    	val indx = hashInternal(k) % bucketsCnt;
    	return buckets(indx).bucketMap.remove(k);
    }

    public def clearUnsafe():void {
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		buckets(i).bucketMap.clear();
    	}
    }
    
    public def keySetUnsafe():Set[K] {
    	val set = new HashSet[K]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		set.addAll(buckets(i).bucketMap.keySet());
    	}
    	return set;
    }

    public def entriesUnsafe():Set[Entry[K,V]] {
    	val set = new HashSet[Entry[K,V]]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		set.addAll(buckets(i).bucketMap.entries());
    	}
    	return set;
    }
    
    public def sizeUnsafe():Long {
    	var s:Long = 0;
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		s += buckets(i).bucketMap.size();
    	}
    	return s;
    }
    
    /**************** Locking Methods ****************/
    public def lockAll() {
    	if (!TxConfig.getInstance().LOCK_FREE) {
	    	for (var i:Long = 0; i < bucketsCnt; i++) {
	    		buckets(i).lock();
	    	}
    	}
    }
    
    public def unlockAll() {
    	if (!TxConfig.getInstance().LOCK_FREE) {
	    	for (var i:Long = 0; i < bucketsCnt; i++) {
	    		buckets(i).unlock();
	    	}
    	}
    }

    public def lock(k:K):void {
    	val indx = hashInternal(k) % bucketsCnt;
    	if (!TxConfig.getInstance().LOCK_FREE)
    		buckets(indx).lock();
    }
    
    public def unlock(k:K):void {
    	if (!TxConfig.getInstance().LOCK_FREE) {
    		val indx = hashInternal(k) % bucketsCnt;
    		buckets(indx).unlock();
    	}
    }
    
    private def lockInternal(k:K):Bucket[K,V] {
    	val indx = hashInternal(k) % bucketsCnt;
    	val bucket = buckets(indx); 
    	if (!TxConfig.getInstance().LOCK_FREE)
    		bucket.lock();
    	return bucket;
    }
    
    private def unlockInternal(bucket:Bucket[K,V]):void {
    	if (!TxConfig.getInstance().LOCK_FREE)
        	bucket.unlock();
    }
    
    
    @NonEscaping protected final def hashInternal(k:K):Int {
        return Math.abs(k.hashCode() * 17n);
    }
}