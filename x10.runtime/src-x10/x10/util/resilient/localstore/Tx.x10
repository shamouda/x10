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
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.tx.commit.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;

public class Tx extends AbstractTx {
    private val commitHandler:CommitHandler;
    
    public transient val startTime:Long = Timer.milliTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;
    
    // consumed time
    public transient var processingElapsedTime:Long = 0; ////including waitTime
    public transient var txLoggingElapsedTime:Long = 0;
    
    /* resilient mode variables */
    private transient var aborted:Boolean = false;
    
    private val members:TxMembers;
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
        super(plh, id, mapName);
        this.members = members;
        
        if (TxConfig.get().TM_DEBUG) {
            var memStr:String = "";
            if (members != null)
                memStr = members.toString();
            Console.OUT.println("TX["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] started members["+memStr+"]");
        }
        	
        if (resilient) {
            if (TxConfig.get().DISABLE_SLAVE) {
                commitHandler = new NonResilientCommitHandler(plh, id, mapName, members);
            }
            else {
                if (TxConfig.get().TM_REP.equals("lazy"))
                    commitHandler = new LazyReplicationCommitHandler(plh, id, mapName, members);
                else   
                    commitHandler = new EagerReplicationCommitHandler(plh, id, mapName, members);
            }
        }
        else
        	commitHandler = new NonResilientCommitHandler(plh, id, mapName, members);
    }
    
    /********** Setting the pre-commit time for statistical analysis **********/   
    public def getPhase1ElapsedTime() = commitHandler.phase1ElapsedTime;
    public def getPhase2ElapsedTime() = commitHandler.phase2ElapsedTime;
    public def getTxLoggingElapsedTime() = commitHandler.txLoggingElapsedTime;

    /*********************** Abort ************************/  
    public def abortRecovery() {
        plh().slaveStore.abort(id);
        abort(true);
    }
    
    public def abort() {
        abort(false);
    }
    
    private def abort(recovery:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here="+ here + " abort started alreadyAborted = " + aborted);
        if (!aborted)
            aborted = true;
        else 
            return;
        
        commitHandler.abort(recovery);
        
        abortTime = Timer.milliTime();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here="+ here + " aborted, allTxTime ["+(abortTime-startTime)+"] ms");
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    public def commitRecovery() {
        plh().slaveStore.commit(id);
        return commit(true);
    }
    
    public def commit():Int {
        if (TxConfig.get().DISABLE_COMMIT)
            return AbstractTx.SUCCESS;
        return commit(false);
    }
    
    public def commit(recovery:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here=" + here + " commit started");
        if (TxConfig.get().DISABLE_COMMIT)
            return AbstractTx.SUCCESS;
                
        val success = commitHandler.commit(recovery);

        commitTime = Timer.milliTime();
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here=" + here + " committed, allTxTime [" + (commitTime-startTime) + "] ms");
        return success;
    }
    
    
    public def asyncAt(virtualPlace:Long, closure:()=>void) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here=" + here + " asyncAt(dest="+virtualPlace+") ...");
        if (members == null){
            val vMembers = new Rail[Long](1);
            vMembers(0) = virtualPlace;
            plh().txDescManager.addVirtualMembers(id, vMembers, false) ;
        }
        
        val pl = plh().getPlace(virtualPlace);
        assert (pl.id >= 0 && pl.id < Place.numPlaces()) : "fatal bug, wrong place id " + pl.id;
        at (pl) async closure();
    }
        
}