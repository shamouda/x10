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

package x10.xrx.txstore;

import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;
import x10.util.HashSet;
import x10.xrx.TxStorePausedException;
import x10.xrx.TxStoreFatalException;

public class TxRail[K](size:Long) {K haszero} {
    val values:Rail[K];
    val locks:Rail[TxLock];
    val versions:Rail[Int];

    val lc:Lock;

    public static struct TxItem[K] {
        public val version:Int;
        public val value:K;
        private transient val lock:TxLock;
        def this(v:K, ver:Int, lc:TxLock) {
            value = v;
            version = ver;
            lock = lc;
        }
    };
    
    public def this(size:Long) {
        property(size);
        values = new Rail[K](size);
        locks = new Rail[TxLock](size, (i:Long) => TxLock.make());
        versions = new Rail[Int](size, 0n);
        lc = new Lock();
    }
    
    public def this(packedValues:HashMap[Long,TxItem[K]]) {
        property(packedValues.size());
        values = new Rail[K](size);
        locks = new Rail[TxLock](size, (i:Long) => TxLock.make());
        versions = new Rail[Int](size);
        lc = new Lock();
        
        for (var i:Long = 0; i < packedValues.size(); i++) {
            val item = packedValues.getOrThrow(i);
            values(i) = item.value;
            versions(i) = item.version;
        }
    }
    
    public def packItemsForReplication() {
        try {
            lock(-1);
            val result = new HashMap[Long,TxItem[K]]();
            for (var i:Long = 0; i < size; i++) {
                result.put(i, new TxItem[K](values(i), versions(i), null));
            }
            return result;
            
        }finally {
            unlock(-1);
        }
    }
    
    public def getItem(i:Long):TxItem[K] {
        try {
            lock(-1);
            return new TxItem[K](values(i), versions(i), locks(i));
        }finally {
            unlock(-1);
        }
    }
    
    public def lock(txId:Long){
        if (!TxConfig.LOCK_FREE) {
            lc.lock();
        }
    }
    
    public def unlock(txId:Long) {
        if (!TxConfig.LOCK_FREE) {
            lc.unlock();
        }
    }
}