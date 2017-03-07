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
	private val bucketsLocks:Rail[Lock];
	private val bucketsCnt:Long;
	
    static class Bucket[Key,Value] {Value haszero} {
        val bucketMap = new HashMap[Key,Value]();
    }
    
    public def this(bucketsCnt:Long) {
        buckets = new Rail[Bucket[K,V]](bucketsCnt, (i:Long)=> new Bucket[K,V]());
        if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
        	bucketsLocks = new Rail[Lock](bucketsCnt, (i:Long)=> new Lock());
        else
        	bucketsLocks = null;
        this.bucketsCnt = bucketsCnt;
    }
    
    /***************************Safe methods**********************************/
    public def containsKeySafe(k:K):Boolean {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.containsKey(k);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def getSafe(k:K):V {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.get(k);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def getOrElseSafe(k:K, orelse:V):V {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.getOrElse(k, orelse);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def getOrThrowSafe(k:K):V {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.getOrThrow(k);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def putSafe(k:K, v:V):V {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.put(k, v);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def deleteSafe(k:K):boolean {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.delete(k);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def removeSafe(k:K):V {
    	try {
    		val indx = lock(k);
    		return buckets(indx).bucketMap.remove(k);
    	}
    	finally {
    		unlock(k);
    	}
    }

    public def clearSafe():void {
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).lock();
    		buckets(i).bucketMap.clear();
    		bucketsLocks(i).unlock();
    	}
    }
    
    public def keySetSafe():Set[K] {
    	val set = new HashSet[K]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).lock();
    		set.addAll(buckets(i).bucketMap.keySet());
    		bucketsLocks(i).unlock();
    	}
    	return set;
    }

    public def entriesSafe():Set[Entry[K,V]] {
    	val set = new HashSet[Entry[K,V]]();
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).lock();
    		set.addAll(buckets(i).bucketMap.entries());
    		bucketsLocks(i).unlock();
    	}
    	return set;
    }
    
    public def sizeSafe():Long {
    	var s:Long = 0;
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).lock();
    		s += buckets(i).bucketMap.size();
    		bucketsLocks(i).unlock();
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
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).lock();
    	}
    }
    
    public def unlockAll() {
    	for (var i:Long = 0; i < bucketsCnt; i++) {
    		bucketsLocks(i).unlock();
    	}
    }

    public def lock(k:K) {
    	val indx = hashInternal(k) % bucketsCnt;
    	if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE)
    		bucketsLocks(indx).lock();
    	return indx;
    }
    
    public def unlock(k:K) {
    	if (TxConfig.getInstance().LOCKING_MODE != TxConfig.LOCKING_MODE_FREE) {
    		val indx = hashInternal(k) % bucketsCnt;
    		bucketsLocks(indx).unlock();
    	}
    }
    
    @NonEscaping protected final def hashInternal(k:K):Int {
        return Math.abs(k.hashCode() * 17n);
    }
}