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
public class LockingTx[K] {K haszero} extends AbstractTx[K] {
    public transient val startTime:Long=Timer.milliTime(); ////
    public transient var startProcessing:Long=0; ////
    public transient var lockingElapsedTime:Long=0; //////
    public transient var processingElapsedTime:Long=0; //// (including waitTime)
    public transient var unlockingElapsedTime:Long=0; ///////
    public transient var totalElapsedTime:Long=0; //////

    public val members:Rail[Long];
    public val keys:Rail[K];
    public val readFlags:Rail[Boolean];
    public val opPerPlace:Long;

    public def this(plh:PlaceLocalHandle[LocalStore[K]],id:Long,mapName:String,members:Rail[Long],keys:Rail[K],readFlags:Rail[Boolean],opPerPlace:Long)
    {
        super(plh, id, mapName);
        this.members = members;
        this.keys = keys;
        this.readFlags = readFlags;
        this.opPerPlace = opPerPlace;
    }

    /****************lock and unlock all keys**********************/

    public def lock() {
        //don't copy this in remote operations
        val members = this.members;
        val keys = this.keys;
        val readFlags = this.readFlags;
        val plh = this.plh;
        val opPerPlace = this.opPerPlace;
        
        val startLock = Timer.milliTime();
        
        if (members.size == 1 && members(0) == here.id) {//local locking
            if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                Runtime.increaseParallelism();
            
            for (var x:Long = 0; x < opPerPlace ; x++) {
                if (readFlags(x))
                    plh().getMasterStore().lockRead(mapName, id, keys(x));
                else
                    plh().getMasterStore().lockWrite(mapName, id, keys(x));
            }
            
            if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                Runtime.decreaseParallelism(1n);
        }
        else {
            finish for (var i:Long = 0; i < members.size; i++) {
                val dest = members(i);
                val start = opPerPlace*i;
                at (Place(dest)) { //locking must be done sequentially
                    if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                        Runtime.increaseParallelism();
                    
                    for (var x:Long = 0; x < opPerPlace ; x++) {
                        if (readFlags(start+x))
                            plh().getMasterStore().lockRead(mapName, id, keys(start+x));
                        else
                            plh().getMasterStore().lockWrite(mapName, id, keys(start+x));
                    }
                    
                    if (!TxConfig.get().DISABLE_INCR_PARALLELISM && !TxConfig.get().LOCK_FREE)
                        Runtime.decreaseParallelism(1n);
                }
            }
        }
        lockingElapsedTime = Timer.milliTime() - startLock;
        startProcessing = Timer.milliTime();
    }

    public def unlock() {
        processingElapsedTime = Timer.milliTime() - startProcessing;
        //don't copy this in remote operations
        val members = this.members;
        val keys = this.keys;
        val readFlags = this.readFlags;
        val plh = this.plh;
        val opPerPlace = this.opPerPlace;
        
        val startUnlock = Timer.milliTime();
        if (members.size == 1 && members(0) == here.id) {//local locking
            for (var x:Long = 0; x < opPerPlace ; x++) {
                if (readFlags(x))
                    plh().getMasterStore().unlockRead(mapName, id, keys(x));
                else
                    plh().getMasterStore().unlockWrite(mapName, id, keys(x));
            }
        }
        else {
            finish for (var i:Long = 0; i < members.size; i++) {
                val dest = members(i);
                val start = opPerPlace*i;
                at (Place(dest)) async {
                    for (var x:Long = 0; x < opPerPlace ; x++) {
                        if (readFlags(start+x))
                            plh().getMasterStore().unlockRead(mapName, id, keys(start+x));
                        else
                            plh().getMasterStore().unlockWrite(mapName, id, keys(start+x));
                    }
                }
            }
        }
        unlockingElapsedTime = Timer.milliTime() - startUnlock;
        totalElapsedTime = Timer.milliTime() - startTime;
        
        plh().stat.addLockingTxStats(totalElapsedTime, lockingElapsedTime, processingElapsedTime, unlockingElapsedTime);
    }

}