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

public class TxSlaveLog(id:Long, ownerPlaceIndex:Long) {
    public var transLog:HashMap[String,Cloneable];
    
    public def this(id:Long, ownerPlaceIndex:Long) {
    	property(id, ownerPlaceIndex);
    }
    
    public def this(id:Long, ownerPlaceIndex:Long, transLog:HashMap[String,Cloneable]) {
    	property(id, ownerPlaceIndex);
    	this.transLog = transLog;
    }
    
    public def toString() {
        return TxManager.txIdToString(id) + " ownerIndx:" + ownerPlaceIndex;
    }
}