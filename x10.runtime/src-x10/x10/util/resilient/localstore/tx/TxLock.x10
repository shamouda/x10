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

public abstract class TxLock {
	
	public abstract def tryLockRead(txId:Long, key:String):Boolean;
	
	public abstract def tryLockWrite(txId:Long, key:String):Boolean;
	
    public abstract def lockRead(txId:Long, key:String):void;
    
    public abstract def lockWrite(txId:Long, key:String):void;
  
    public abstract def unlockRead(txId:Long, key:String):void;
    
    public abstract def unlockWrite(txId:Long, key:String):void;
    
    public def lock(txId:Long, key:String) {
		lockWrite(txId, key);
	}
	public def unlock(txId:Long, key:String) {
		unlockWrite(txId, key);
	}
}