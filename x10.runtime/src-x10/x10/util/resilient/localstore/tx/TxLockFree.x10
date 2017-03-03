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
import x10.util.concurrent.ReadWriteSemaphore;
import x10.util.HashSet;

/*
 * Not a real lock, used for performance evaluation only
 * */
public class TxLockFree extends TxLock {
    public def lockRead(txId:Long, key:String) { }
    public def unlockRead(txId:Long, key:String) { }
    public def lockWrite(txId:Long, key:String) { }
    public def unlockWrite(txId:Long, key:String) { }
    public def tryLockRead(txId:Long, key:String) = true;
    public def tryLockWrite(txId:Long, key:String) = true;
}