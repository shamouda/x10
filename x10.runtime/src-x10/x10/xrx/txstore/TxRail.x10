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
import x10.xrx.TxStoreConflictException;

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
    
    public def this(size:Long, init:(Long)=>K) {
        property(size);
        values = new Rail[K](size, init);
        locks = new Rail[TxLock](size, (i:Long) => TxLock.make());
        versions = new Rail[Int](size, 0n);
        lc = new Lock();
    }
    
    
    public def this(backupValues:HashMap[Long,K]) {
        property(backupValues.size());
        values = new Rail[K](size);
        locks = new Rail[TxLock](size, (i:Long) => TxLock.make());
        versions = new Rail[Int](size, 0n);
        lc = new Lock();
        
        for (var i:Long = 0; i < backupValues.size(); i++) {
            values(i) = backupValues.getOrThrow(i);
        }
    }
    
    public def getRailForRecovery() {
        try {
            lock(-1);
            return new Rail[K](values);
        } finally {
            unlock(-1);
        }
    }
    
    public def logValueAndVersion(index:Long, location:Long, log:TxLogForRail[K]) {
        try {
            lock(-1);
            log.initialize(location, versions(index), values(index));
        }finally {
            unlock(-1);
        }
    }
    
    
    public def lockReadAndValidateVersion(id:Long, index:Long, initVersion:Int) {
        locks(index).lockRead(id);
        try {
            lock(-1);
            if (initVersion != versions(index))
                throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
        }finally {
            unlock(-1);
        }
    }
    
    public def lockWriteAndValidateVersion(id:Long, index:Long, initVersion:Int) {
        locks(index).lockWrite(id);
        try {
            lock(-1);
            if (initVersion != versions(index))
                throw new TxStoreConflictException("ConflictException["+here+"] Tx["+id+"] ", here);
        } finally {
            unlock(-1);
        }
    }
    
    public def lockWriteFast(id:Long, index:Long) {
        locks(index).lockWrite(id);
    }
    
    public def lockReadFast(id:Long, index:Long) {
        locks(index).lockRead(id);
    }
    
    public def unlockWriteFast(id:Long, index:Long) {
        locks(index).unlockWrite(id);
    }
    
    public def unlockReadFast(id:Long, index:Long) {
        locks(index).unlockRead(id);
    }
    
    
    public def updateAndunlockWrite(id:Long, index:Long, currValue:K) {
        try {
            lock(-1);
            values(index) = currValue;
            versions(index)++;
        } finally {
            unlock(-1);
        }
        locks(index).unlockWrite(id);
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