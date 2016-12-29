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
import x10.util.resilient.localstore.Cloneable;

/**
 * This class is usef for both Undo Logging (UL), and Write Buffering (WB):
 * In UL: the value represents the initial value, inplace changes occur on the Map, we use the logged initial value for rollback.
 * In WB: the value is the current value local to the transaction, changes occur on the log value (not directly on the Map), we use this value for commit.
 **/
public class TxKeyChange {
    /*A copy of the value, used to isolate the transaction updates for the actual value*/
    private var value:Cloneable;
    
    /*Initial version at Tx start*/
    private val initVersion:Int;
    
    /*Initial version at Tx start*/
    private val initTxId:Long;
    
    /*A flag to indicate if the value was used for read only operations*/
    private var readOnly:Boolean = true;

    /*A flag to differentiate between setting a NULL and deleting an object*/
    private var deleted:Boolean = false;

    private var locked:Boolean = false;

    private var unlocked:Boolean = false;
    
    public def this(initValue:Cloneable, initVersion:Int, initTxId:Long) {
        this.value = initValue;
        this.initVersion = initVersion;
        this.initTxId = initTxId;
    }
    
    public def this (value:Cloneable, initVersion:Int, initTxId:Long, readOnly:Boolean, deleted:Boolean, locked:Boolean, unlocked:Boolean) {
        this.value = value;
        this.initVersion = initVersion;
        this.initTxId = initTxId;
        this.readOnly = readOnly;
        this.deleted = deleted;
        this.locked = locked;
        this.unlocked = unlocked;
    }
    
    public def update(n:Cloneable) {
        //Undo Logging should only log the initial value and perform updates inplace
        value = n;
        readOnly = false;
        if (deleted)
            deleted = false;
    }
    
    public def delete() {
        //Undo Logging should only log the initial value and perform updates inplace
        readOnly = false;
        deleted = true;
        value = null;
    }
    
    
    public def markAsModified() {
        readOnly = false;
    }
    
    public def markAsDeleted() {
        readOnly = false;
        deleted = true;
    }
    
    public def markAsLocked() {
        locked = true;
    }
    
    public def markAsUnLocked() {
        unlocked = true;
    }
    
    public def clone() {
        return new TxKeyChange(value, initVersion, initTxId, readOnly, deleted, locked, unlocked);
    }
    
    public def getValue() = value;
    public def readOnly() = readOnly;
    public def isDeleted() = deleted;
    public def getInitVersion() = initVersion;
    public def getInitTxId() = initTxId;
    public def isLocked() = locked;
    public def isUnlocked() = unlocked;
    
}