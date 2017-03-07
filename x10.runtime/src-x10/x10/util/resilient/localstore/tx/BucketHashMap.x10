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

public class BucketHashMap[K,V] {V haszero} /* extends HashMap[K,V] */{
	/*
	private val buckets:Rail[Bucket[K,V]];
	private val bucketsLocks:Rail[Lock];
	private val bucketsSize:Long;
	
    static class Bucket[Key,Value] {Value haszero} {
        val bucketMap = new HashMap[Key,Value]();
    }
    
    public def this(bucketsSize:Long) {
        super();
        buckets = new Rail[Bucket[K,V]](BUCKETS_COUNT, (i:Long)=> new Bucket[K,V]());
        bucketsLocks = new Rail[Lock](BUCKETS_COUNT, (i:Long)=> new Lock());
        this.bucketsSize = bucketsSize;
    }
    
    public def containsKey(k:K):Boolean;

    public def get(k:K):V;

    public operator this(k:K):V;

    public def getOrElse(k:K, orelse:V):V;

    public def getOrThrow(k:K):V;

    public def put(k:K, v:V):V;

    public operator this(k:K)=(v:V):V;

    public def delete(k:K):boolean;

    public def remove(k:K):V;

    public def clear():void;
    
    public def keySet():Set[K];

    public def entries():Set[Entry[K,V]];
    
    public def size():Long;
*/    
}