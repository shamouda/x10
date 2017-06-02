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

import x10.util.resilient.localstore.Cloneable;

/**
 * This class is used for both Undo Logging (UL), and Write Buffering (WB):
 * In UL: the value represents the initial value, inplace changes occur on the Map, we use the logged initial value for rollback.
 * In WB: the value is the current value local to the transaction, changes occur on the log value (not directly on the Map), we use this value for commit.
 **/
public class TxKeyChange[K] {K haszero} {
    private var key:K;

    /*A copy of the value, used to isolate the transaction updates for the actual value*/
    private var value:Cloneable;
    
    /*Initial version at Tx start*/
    private var initVersion:Int = -1n;
    
    /*Initial version at Tx start*/
    private var initTxId:Long = -1;
    
    /*A flag to indicate if the value was used for read only operations*/
    private var readOnly:Boolean = true;
    private var lockedRead:Boolean = false;
    private var lockedWrite:Boolean = false;

    private var added:Boolean = false;
    private var deleted:Boolean = false;
    private var memU:MemoryUnit[K];
    
    private var location:Long;
   
    public def this(){
        
    }
    
    public def init(key:K, initTxId:Long, lockedRead:Boolean, memU:MemoryUnit[K], added:Boolean,
            initValue:Cloneable, initVersion:Int) {
        this.key = key;
        this.initTxId = initTxId;
        this.lockedRead = lockedRead;
        this.memU = memU;
        this.added = added;
        this.value = initValue;
        this.initVersion = initVersion;
    }
    
    public def indx() = location;
    
    public def setIndx(l:Long) {
        location = l;
    }
    
    public def key() = key;
    
    public def update(n:Cloneable) {
        //Undo Logging should only log the initial value and perform updates inplace
        val oldValue = value;
        value = n;
        readOnly = false;
        return oldValue;
    }
    
    public def delete() {
        val oldValue = value;
        value = null;
        readOnly = false;
        deleted = true;
        return oldValue;
    }
    
    public def setReadOnly(ro:Boolean) {
        readOnly = ro;
    }
    
    public def setLockedRead(lr:Boolean) {
        lockedRead = lr;
    }
    
    public def setLockedWrite(lw:Boolean) {
        lockedWrite = lw;
    }
    
    public def setDeleted(d:Boolean) {
        deleted = d;
    }
    public def getMemoryUnit() = memU;
    public def getValue() = value;
    public def getReadOnly() = readOnly;
    public def getDeleted() = deleted;
    public def getInitVersion() = initVersion;
    public def getInitTxId() = initTxId;
    public def getLockedRead() = lockedRead;
    public def getLockedWrite() = lockedWrite;
    public def getAdded() = added;
}