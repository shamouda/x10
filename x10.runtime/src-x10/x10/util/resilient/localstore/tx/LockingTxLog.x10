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

import x10.util.HashMap;
import x10.util.GrowableRail;
import x10.util.resilient.localstore.TxConfig;

public class LockingTxLog[K] {K haszero} {
    public var id:Long = -1;

    public def this() {
        
    }
    
    public def reset() {
        id = -1;
        rail.clear();
    }
    
    public static struct KeyMemory[K] {K haszero} {
        public val key:K;
        public val memU:MemoryUnit[K];
    
        public def this(key:K, memU:MemoryUnit[K]) {
            this.key = key;
            this.memU = memU;
        }
        
        public def toString() {
            return " key[" + key + "] mem[" + memU + "] ";
        }
    }
    
    private val rail = new GrowableRail[KeyMemory[K]](TxConfig.get().PREALLOC_TXKEYS);
    
    public def searchMemoryUnit(key:K):MemoryUnit[K] {
        for (var i:Long = 0; i < rail.size(); i++) {
            if (rail(i).key.equals(key))
                return rail(i).memU;
        }
        return null;
    }
    
    public def addMemoryUnit(key:K, memU:MemoryUnit[K]) {
        rail.add(new KeyMemory(key, memU));
    }
    
    public def toString() {
        var s:String = "LockingLog {";
        for (var i:Long = 0; i < rail.size(); i++) {
            s += rail(i) + "  ";
        }
        s += " } ";
        return s;
    }
}