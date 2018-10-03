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

import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;

public class TxSlaveLog[K] {K haszero} {
    public var transLog:HashMap[K,Cloneable];
    public var transLogV:HashMap[K,Int];
    public var transLogRail:HashMap[Long,K];
    public val id:Long;
    public val ownerPlaceIndex:Long;

    public def this(id:Long, ownerPlaceIndex:Long) {
    	this.id = id;
    	this.ownerPlaceIndex = ownerPlaceIndex;
    }
    
    public def this(id:Long, ownerPlaceIndex:Long, transLog:HashMap[K,Cloneable], transLogV:HashMap[K,Int], 
            transLogRail:HashMap[Long,K]) {
    	this.id = id;
    	this.ownerPlaceIndex = ownerPlaceIndex;
    	this.transLog = transLog;
    	this.transLogV = transLogV;
    	this.transLogRail = transLogRail;
    }
    
    public def toString() {
        return TxManager.txIdToString(id) + " ownerIndx:" + ownerPlaceIndex;
    }
}