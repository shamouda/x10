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

public class OptTx[K] {K haszero} extends AbstractTx[K] {
    private val commitHandler:CommitHandler[K];
    public transient val startTime:Long = Timer.milliTime();
    
    // consumed time
    public transient var processingElapsedTime:Long = 0; ////including waitTime
    public transient var txLoggingElapsedTime:Long = 0;
    private var txDescCreated:Boolean = false;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        super(plh, id);
        if (resilient && !TxConfig.DISABLE_SLAVE) {
            commitHandler = new ResilientTreeCommitHandler[K](plh, id);
        } else {
            commitHandler = new NonResilientTreeCommitHandler[K](plh, id);
        }
    }

    /*********************** Abort ************************/  
    public def abortRecovery() {
        abort(true);
    }
    
    public def abort() {
        abort(false);
    }
    
    private def abort(recovery:Boolean) {
        if (processingElapsedTime == 0)
            processingElapsedTime = Timer.milliTime() - startTime;
        
        try {
            commitHandler.abort(recovery);
        } catch (ex:Exception) {
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("OptTx["+id+"] " + TxManager.txIdToString(id) + " here="
                                                              + here + " ignoring exception during abort ");    
        }
        if (plh().stat != null)
            plh().stat.addAbortedTxStats(Timer.milliTime() - startTime, 
                processingElapsedTime);
    }
    
    /***********************   Two Phase Commit Protocol ************************/
    public def commitRecovery() {
        return commit(true);
    }
    
    public def commit():Int {
        return commit(false);
    }
    
    public def commit(recovery:Boolean) {
        if (plh().stat != null && processingElapsedTime == 0)
            processingElapsedTime = Timer.milliTime() - startTime;
        
        val success:Int;
        try {
            success = commitHandler.commit(recovery);
        }catch (ex:Exception) {
            if (plh().stat != null)
                plh().stat.addAbortedTxStats(Timer.milliTime() - startTime, processingElapsedTime);
            throw ex;
        }

        if (plh().stat != null)
            plh().stat.addCommittedTxStats(Timer.milliTime() - startTime, 
                processingElapsedTime,
                commitHandler.phase1ElapsedTime,
                commitHandler.phase2ElapsedTime,
                commitHandler.txLoggingElapsedTime);
        
        return success;
    }
    
    public def clean() {
        commitHandler.clean();
    }

}