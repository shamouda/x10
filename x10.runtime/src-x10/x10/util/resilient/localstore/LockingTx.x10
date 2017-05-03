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

package x10.util.resilient.localstore;

import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Future;

/*should be used in non-resilient mode only*/
public class LockingTx extends AbstractTx {
    public transient val startTime:Long = Timer.milliTime(); ////
    public transient var lockingElapsedTime:Long = 0;  //////
    public transient var processingElapsedTime:Long = 0; //// (including waitTime)
    public transient var unlockingElapsedTime:Long = 0; ///////
    public transient var totalElapsedTime:Long = 0; //////
   
    public val requests:ArrayList[LockingRequest];

    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, requests:ArrayList[LockingRequest]) {
        super(plh, id, mapName);
        this.requests = requests;
    }
    
    /****************lock and unlock all keys**********************/

    public def lock() {
        val startLock = Timer.milliTime();
        
        if (requests.size() == 1 && requests.get(0).dest == here.id) {//local locking
            val req = requests.get(0);
            
            if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                Runtime.increaseParallelism();
            
            for (var i:Long = 0; i < req.keys.size ; i++) {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") locking " + req.keys(i).key + "  read: " + req.keys(i).read);
                if (req.keys(i).read)
                    plh().getMasterStore().lockRead(mapName, id, req.keys(i).key);
                else
                    plh().getMasterStore().lockWrite(mapName, id, req.keys(i).key);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") locking " + req.keys(i).key + "  read: " + req.keys(i).read + " -done");
            }
            
            if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                Runtime.decreaseParallelism(1n);
        }
        else {
            finish for (req in requests) {
                at (Place(req.dest)) { //locking must be done sequentially
                    if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                        Runtime.increaseParallelism();
                    
                    for (var i:Long = 0; i < req.keys.size ; i++) {
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") locking " + req.keys(i).key + "  read: " + req.keys(i).read);
                        if (req.keys(i).read)
                            plh().getMasterStore().lockRead(mapName, id, req.keys(i).key);
                        else
                            plh().getMasterStore().lockWrite(mapName, id, req.keys(i).key);
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") locking " + req.keys(i).key + "  read: " + req.keys(i).read + " -done");
                    }
                    
                    if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                        Runtime.decreaseParallelism(1n);
                }
            }
        }
        lockingElapsedTime = Timer.milliTime() - startLock;
    }

    public def unlock() {
        val startUnlock = Timer.milliTime();
        if (requests.size() == 1 && requests.get(0).dest == here.id) {//local locking
            val req = requests.get(0);
            for (var i:Long = 0; i < req.keys.size ; i++) {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") unlocking " + req.keys(i).key + "  read: " + req.keys(i).read);
                if (req.keys(i).read)
                    plh().getMasterStore().unlockRead(mapName, id, req.keys(i).key);
                else
                    plh().getMasterStore().unlockWrite(mapName, id, req.keys(i).key);
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") unlocking " + req.keys(i).key + "  read: " + req.keys(i).read + " -done");
            }
        }
        else {
            finish for (req in requests) {
                at (Place(req.dest)) async {
                    for (var i:Long = 0; i < req.keys.size ; i++) {
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") unlocking " + req.keys(i).key + "  read: " + req.keys(i).read);
                        if (req.keys(i).read)
                            plh().getMasterStore().unlockRead(mapName, id, req.keys(i).key);
                        else
                            plh().getMasterStore().unlockWrite(mapName, id, req.keys(i).key);
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " +here+ " ("+i+"/"+req.keys.size+") unlocking " + req.keys(i).key + "  read: " + req.keys(i).read + " -done");
                    }
                }
            }
        }
        unlockingElapsedTime = Timer.milliTime() - startUnlock;
        totalElapsedTime = Timer.milliTime() - startTime;
    }
    
    
}
