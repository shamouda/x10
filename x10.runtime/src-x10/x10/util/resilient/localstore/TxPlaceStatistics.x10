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
import x10.util.concurrent.Lock;

public class TxPlaceStatistics {
    
    public val p:Place;
    private val lock:Lock;
    public val g_commitList:ArrayList[Double];
    public val g_commitProcList:ArrayList[Double];  
    public val g_commitPH1List:ArrayList[Double];
    public val g_commitPH2List:ArrayList[Double];
    public val g_txLoggingList:ArrayList[Double]; 
    public val g_abortList:ArrayList[Double];
    public val g_abortProcList:ArrayList[Double]; 
    public val l_commitList:ArrayList[Double];
    public val l_commitProcList:ArrayList[Double];
    public val l_abortList:ArrayList[Double];
    public val l_abortProcList:ArrayList[Double];
    public val lk_totalList:ArrayList[Double];
    public val lk_lockList:ArrayList[Double];
    public val lk_procList:ArrayList[Double];
    public val lk_unlockList:ArrayList[Double];

    public def this() {
        p = here;
        lock = new Lock();
        g_commitList = new ArrayList[Double]();
        g_commitProcList = new ArrayList[Double]();  
        g_commitPH1List = new ArrayList[Double]();
        g_commitPH2List = new ArrayList[Double]();
        g_txLoggingList = new ArrayList[Double](); 
        g_abortList = new ArrayList[Double]();
        g_abortProcList = new ArrayList[Double](); 
        l_commitList = new ArrayList[Double]();
        l_commitProcList = new ArrayList[Double]();
        l_abortList = new ArrayList[Double]();
        l_abortProcList = new ArrayList[Double]();
        lk_totalList = new ArrayList[Double]();
        lk_lockList = new ArrayList[Double]();
        lk_procList = new ArrayList[Double]();
        lk_unlockList = new ArrayList[Double]();
    }
    
    public def addCommittedTxStats(allTime:Double, processingTime:Double, ph1Time:Double, ph2Time:Double, loggingTime:Double) {
        try {
            lock();
            g_commitList.add(allTime);
            g_commitProcList.add(processingTime);
            g_commitPH1List.add(ph1Time);
            g_commitPH2List.add(ph2Time);
            g_txLoggingList.add(loggingTime);
        } finally {
            unlock();
        }
    }
    
    public def addAbortedTxStats(allTime:Double, processingTime:Double) {
        try {
            lock();
            g_abortList.add(allTime);
            g_abortProcList.add(processingTime);
        } finally {
            unlock();
        }
    }
    
    public def addAbortedLocalTxStats(allTime:Double, processingTime:Double) {
        try {
            lock();
            l_abortList.add(allTime);
            l_abortProcList.add(processingTime);
        } finally {
            unlock();
        }
    }
    
    public def addCommittedLocalTxStats(allTime:Double, processingTime:Double) {
        try {
            lock();
            l_commitList.add(allTime);
            l_commitProcList.add(processingTime);
        } finally {
            unlock();
        }
    }
    
    public def addLockingTxStats(totalElapsedTime:Double, lockingElapsedTime:Double, processingElapsedTime:Double, unlockingElapsedTime:Double) {
        try {
            lock();
            lk_totalList.add(totalElapsedTime);
            lk_lockList.add(lockingElapsedTime);
            lk_procList.add(processingElapsedTime);
            lk_unlockList.add(unlockingElapsedTime);
        } finally {
            unlock();
        }
    }
    
    public def toString() {
        
        val g_commitMean      = TxStatistics.mean(g_commitList);
        val g_commitSTDEV     = TxStatistics.stdev(g_commitList, g_commitMean);
        val g_commitBox       = TxStatistics.boxPlot(g_commitList);        
        val g_commitProcMean  = TxStatistics.mean(g_commitProcList);
        val g_commitProcSTDEV = TxStatistics.stdev(g_commitProcList, g_commitProcMean);
        val g_commitProcBox   = TxStatistics.boxPlot(g_commitProcList);
        val g_ph1Mean         = TxStatistics.mean(g_commitPH1List);
        val g_ph1STDEV        = TxStatistics.stdev(g_commitPH1List, g_ph1Mean);
        val g_ph1Box          = TxStatistics.boxPlot(g_commitPH1List);
        val g_ph2Mean         = TxStatistics.mean(g_commitPH2List);
        val g_ph2STDEV        = TxStatistics.stdev(g_commitPH2List, g_ph2Mean);
        val g_ph2Box          = TxStatistics.boxPlot(g_commitPH2List);
        val g_logMean         = TxStatistics.mean(g_txLoggingList);
        val g_logSTDEV        = TxStatistics.stdev(g_txLoggingList, g_logMean);
        val g_logBox          = TxStatistics.boxPlot(g_txLoggingList);
        val g_abortMean       = TxStatistics.mean(g_abortList);
        val g_abortSTDEV      = TxStatistics.stdev(g_abortList, g_abortMean);
        val g_abortBox        = TxStatistics.boxPlot(g_abortList);
        val g_abortProcMean   = TxStatistics.mean(g_abortProcList);
        val g_abortProcSTDEV  = TxStatistics.stdev(g_abortProcList, g_abortProcMean);
        val g_abortProcBox    = TxStatistics.boxPlot(g_abortProcList);
        
        val l_commitMean      = TxStatistics.mean(l_commitList);
        val l_commitSTDEV     = TxStatistics.stdev(l_commitList, l_commitMean);
        val l_commitBox       = TxStatistics.boxPlot(l_commitList);        
        val l_commitProcMean  = TxStatistics.mean(l_commitProcList);
        val l_commitProcSTDEV = TxStatistics.stdev(l_commitProcList, l_commitProcMean);
        val l_commitProcBox   = TxStatistics.boxPlot(l_commitProcList);
        val l_abortMean       = TxStatistics.mean(l_abortList);
        val l_abortSTDEV      = TxStatistics.stdev(l_abortList, l_abortMean);
        val l_abortBox        = TxStatistics.boxPlot(l_abortList);
        val l_abortProcMean   = TxStatistics.mean(l_abortProcList);
        val l_abortProcSTDEV  = TxStatistics.stdev(l_abortProcList, l_abortProcMean);
        val l_abortProcBox    = TxStatistics.boxPlot(l_abortProcList);
        
        val lk_totalMean      = TxStatistics.mean(lk_totalList);
        val lk_totalSTDEV     = TxStatistics.stdev(lk_totalList, lk_totalMean);
        val lk_totalBox       = TxStatistics.boxPlot(lk_totalList);
        val lk_lockMean      = TxStatistics.mean(lk_lockList);
        val lk_lockSTDEV     = TxStatistics.stdev(lk_lockList, lk_lockMean);
        val lk_lockBox       = TxStatistics.boxPlot(lk_lockList);
        val lk_procMean      = TxStatistics.mean(lk_procList);
        val lk_procSTDEV     = TxStatistics.stdev(lk_procList, lk_procMean);
        val lk_procBox       = TxStatistics.boxPlot(lk_procList);
        val lk_unlockMean      = TxStatistics.mean(lk_unlockList);
        val lk_unlockSTDEV     = TxStatistics.stdev(lk_unlockList, lk_unlockMean);
        val lk_unlockBox       = TxStatistics.boxPlot(lk_unlockList);
        
        var str:String = "";
        if (g_commitList.size() > 0) {
            str += p + ":GLOBAL_TX:commitCount:"+ g_commitList.size() + ":commitMeanMS:"     + g_commitMean    + ":commitSTDEV:"    + g_commitSTDEV    + ":commitBox:(:" + g_commitBox + ":)" 
                                                                      + ":commitProcMeanMS:" + g_commitProcMean + ":commitProcSTDEV:" + g_commitProcSTDEV + ":commitProcBox:(:" + g_commitProcBox + ":)"
                                                                      + ":ph1MeanMS:"        + g_ph1Mean       + ":ph1STDEV:"   + g_ph1STDEV    + ":ph1Box:(:" + g_ph1Box + ":)"
                                                                      + ":ph2MeanMS:"        + g_ph2Mean       + ":ph2STDEV:"   + g_ph2STDEV    + ":ph2Box:(:" + g_ph2Box + ":)"
                                                                      + ":logMeanMS:"        + g_logMean       + ":logSTDEV:"   + g_logSTDEV    + ":logBox:(:" + g_logBox + ":)\n";
            str += p + ":GLOBAL_TX:abortCount:" + g_abortList.size()  + ":abortMeanMS:"      + g_abortMean     + ":abortSTDEV:" + g_abortSTDEV  + ":abortBox:(:"  + g_abortBox  + ":)" 
                                                                      + ":abortProcMeanMS:"  + g_abortProcMean + ":abortProcSTDEV:"  + g_abortProcSTDEV  + ":abortProcBox:(:"  + g_abortProcBox + ":)\n";
        }
        
        if (l_commitList.size() > 0) {
            str += p + ":LOCAL_TX:commitCount:"+ l_commitList.size() + ":commitMeanMS:"    + l_commitMean    + ":commitSTDEV:"    + l_commitSTDEV    + ":commitBox:(:" + l_commitBox + ":)" 
                                                                     + ":commitProcMeanMS:" + l_commitProcMean + ":commitProcSTDEV:" + l_commitProcSTDEV + ":commitProcBox:(:" + l_commitProcBox + ":)\n" ; 
              str += p + ":LOCAL_TX:abortCount:" + l_abortList.size()  + ":abortMeanMS:"     + l_abortMean     + ":abortSTDEV:"     + l_abortSTDEV     + ":abortBox:(:" + l_abortBox + ":)"   
                                                                     + ":abortProcMeanMS:"  + l_abortProcMean  + ":abortProcSTDEV:"  + l_abortProcSTDEV  + ":abortProcBox:(:"  + l_abortProcBox + ":)\n" ;            
        }
        
        if (lk_totalList.size() > 0) {
            str += p + ":LOCKING_TX:count:"+ lk_totalList.size() 
                + ":totalMeanMS:"    + lk_totalMean    + ":totalSTDEV:"    + lk_totalSTDEV    + ":totalBox:(:" + lk_totalBox + ":)"
                + ":lockMeanMS:"    + lk_lockMean    + ":lockSTDEV:"    + lk_lockSTDEV    + ":lockBox:(:" + lk_lockBox + ":)" 
                + ":procMeanMS:"    + lk_procMean    + ":procSTDEV:"    + lk_procSTDEV    + ":procBox:(:" + lk_procBox + ":)" 
                + ":unlockMeanMS:" + lk_unlockMean + ":unlockSTDEV:" + lk_unlockSTDEV + ":unlockBox:(:" + lk_unlockBox + ":)\n" ; 
        }

        
        return str;
    }    
    
    private def lock(){
        if (!TxConfig.get().LOCK_FREE) {
            lock.lock();
        }
    }
    
    private def unlock() {
        if (!TxConfig.get().LOCK_FREE) {
            lock.unlock();
        }
    }
}