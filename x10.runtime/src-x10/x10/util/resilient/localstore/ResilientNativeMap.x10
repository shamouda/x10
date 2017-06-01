package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.Cloneable;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.logging.TxDesc;

public class ResilientNativeMap (name:String, plh:PlaceLocalHandle[LocalStore]) {
    private static val TM_STAT_ALL = System.getenv("TM_STAT_ALL") != null && System.getenv("TM_STAT_ALL").equals("1");

    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    private var baselineTxId:Long = 0;
    
    /** 
     * Get the value of key k in the resilient map.
     */
    public def get(k:String) {
        return executeLocalTransaction((tx:LocalTx) => tx.get(k) ).output as Cloneable;
    }

    /**
     * Associate value v with key k in the resilient map.
     */
    public def set(k:String, v:Cloneable) {
        return executeLocalTransaction((tx:LocalTx) => tx.put(k,v) ).output as Cloneable;
    }

    /**
     * Remove any value associated with key k from the resilient map.
     */
    public def delete(k:String) {
        return executeLocalTransaction((tx:LocalTx) => tx.delete(k) ).output as Cloneable;
    }

    public def keySet():Set[String] {
        val trans = startLocalTransaction();
        val set = trans.keySet();
        trans.commit();
        return set;
    }
    
    public def setAll(data:HashMap[String,Cloneable]) {
        if (data == null)
            return;        
        val trans = startLocalTransaction();
        val iter = data.keySet().iterator();
        while (iter.hasNext()) {
            val k = iter.next();    
            trans.put(k, data.getOrThrow(k));
        }
        trans.commit();
    }
    
    public def set2(key:String, value:Cloneable, place:Place, key2:String, value2:Cloneable) {
        throw new Exception("method deprecated");
    }
    
    public def set2(key:String, value:Cloneable, key2:String, value2:Cloneable) {        
        return executeLocalTransaction((tx:LocalTx) => { tx.put(key, value); tx.put(key2, value2) });
    }
    
    /***********************  Places functions ****************************/
    public def getVirtualPlaceId() = plh().getVirtualPlaceId();
    
    public def getActivePlaces() = plh().getActivePlaces();
    
    public def getPlace(virtualId:Long) = plh().getPlace(virtualId);

    public def nextPlaceChange() = plh().nextPlaceChange();
    
    /***********************   Local Transactions ****************************/
    
    public def startLocalTransaction():LocalTx {
        assert(plh().virtualPlaceId != -1) : here + " LocalTx assertion error  virtual place id = -1";
        val id = plh().getMasterStore().getNextTransactionId();
        return new LocalTx(plh, id, name);
    }
    
    public def executeLocalTransaction(closure:(LocalTx)=>Any) {
        var out:Any;
        var commitStatus:Int = -1n;

        while(true) {
            val tx = startLocalTransaction();
            var commitCalled:Boolean = false;
            val start = Timer.milliTime();
            try {
                out = closure(tx);
                commitCalled = true;
                commitStatus = tx.commit();
                break;
            } catch(ex:Exception) {
                if (!commitCalled) {
                    tx.processingElapsedTime = Timer.milliTime() - start;
                    //no need to call abort, abort occurs automatically in local tx all the time
                }
                throwIfFatalSleepIfRequired(ex, tx.plh().immediateRecovery);
            }
        }

        return new TxResult(commitStatus, out);
    }
    
    public def executeLocalTransaction(target:Place, closure:(LocalTx)=>Any) {
        val txResult = at (target) {
            var out:Any;
            var commitStatus:Int = -1n;

            while(true) {
                val tx = startLocalTransaction();
                var commitCalled:Boolean = false;
                val start = Timer.milliTime();
                try {
                    out = closure(tx);

                    commitCalled = true;
                    
                    commitStatus = tx.commit();
                    break;
                } catch(ex:Exception) {
                    if (!commitCalled) {
                        tx.processingElapsedTime = Timer.milliTime() - start;
                        //no need to call abort, abort occurs automatically in local tx all the time
                    }
                    throwIfFatalSleepIfRequired(ex, tx.plh().immediateRecovery);
                }
            }
            new TxResult(commitStatus, out)
        };
        return txResult;
    }
    
    /***********************   Global Transactions ****************************/
    private def startGlobalTransaction():Tx {
        return startGlobalTransaction(null);
    }
    
    private def startGlobalTransaction(members:TxMembers):Tx {
        val id = plh().getMasterStore().getNextTransactionId();
        val tx = new Tx(plh, id, name, members);
        try {
            var predefinedMembers:Rail[Long] = null;
            if (members != null)
                predefinedMembers = members.virtual;

            if ((members == null || resilient) && TxConfig.get().COMMIT)
                plh().txDescManager.add(id, predefinedMembers, false);
            
        }catch(ex:NullPointerException) {
            ex.printStackTrace();
        }
        return tx;
    }
    
    public def restartGlobalTransaction(txDesc:TxDesc):Tx {
        assert(plh().virtualPlaceId != -1);
        val includeDead = true; // The commitHandler will take the correct actions regarding the dead master
        val members = txDesc.staticMembers? plh().getTxMembers(txDesc.virtualMembers, includeDead):null;
        return new Tx(plh, txDesc.id, name,  members);
    }
    
    public def executeTransaction(closure:(Tx)=>Any, maxRetries:Long):TxResult {
        return executeTransaction(null, closure, maxRetries, -1);
    }
    
    public def executeTransaction(virtualMembers:Rail[Long], closure:(Tx)=>Any, maxRetries:Long, maxTimeNS:Long):TxResult {
        val beginning = System.nanoTime();
        var members:TxMembers = null;
        if (virtualMembers != null) 
            members = plh().getTxMembers(virtualMembers, true);
        
        var retryCount:Long = 0;
        while(true) {
            if (retryCount > 0 && retryCount % 1000 == 0)
                Console.OUT.println(here + " executeTransaction retryCount reached " + retryCount);
            if (retryCount == maxRetries || (maxTimeNS != -1 && System.nanoTime() - beginning >= maxTimeNS))
                throw new FatalTransactionException("Reached maximum limit for retrying a transaction");
            retryCount++;
            
            var tx:Tx = null; 
            var commitCalled:Boolean = false;
            try {
                tx = startGlobalTransaction(members);
                val out:Any;
                finish {
                    out = closure(tx);
                }
                
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} succeeded");
                commitCalled = true;
                return new TxResult(tx.commit(), out);
            } catch(ex:Exception) {
                if (tx != null && !commitCalled) {
                    tx.abort(); // tx.commit() aborts automatically if needed
                }
                
                if (TxConfig.get().TM_DEBUG) {
                    val txId = tx == null? -1 : tx.id;
                    Console.OUT.println("Tx[" + txId + "] executeTransaction  {finish closure();} failed with Error ["+ex.getMessage()+"] commitCalled["+commitCalled+"] ");
                    ex.printStackTrace();
                }
                val dpe = throwIfFatalSleepIfRequired(ex, plh().immediateRecovery);
                if (dpe && virtualMembers != null)
                    members = plh().getTxMembers(virtualMembers, true);
            }
        }
    }
    
    /***********************   Lock-based Transactions ****************************/
    
    private def startLockingTransaction(members:Rail[Long], keys:Rail[String], readFlags:Rail[Boolean], o:Long):LockingTx {
        assert(plh().virtualPlaceId != -1);
        val id = plh().getMasterStore().getNextTransactionId();
        return new LockingTx(plh, id, name, members, keys, readFlags, o);
    }
    
    public def executeLockingTransaction(members:Rail[Long], keys:Rail[String], readFlags:Rail[Boolean], o:Long, closure:(LockingTx)=>Any) {
        val tx = startLockingTransaction(members, keys, readFlags, o);
        tx.lock();
        val out = closure(tx);
        tx.unlock();
        return new TxResult(Tx.SUCCESS, out);
    }
    
    
    /**************Baseline Operations*****************/
    private def startBaselineTransaction():AbstractTx {
        assert(plh().virtualPlaceId != -1);
        val id = baselineTxId ++;
        val tx = new AbstractTx(plh, id, name);
        return tx;
    }
    
    public def executeBaselineTransaction(closure:(AbstractTx)=>Any) {
        val tx = startBaselineTransaction();
        val out = closure(tx);
        return out;
    }
    /**************End of Baseline Operations*****************/
    
    private static def throwIfFatalSleepIfRequired(ex:Exception, immediateRecovery:Boolean) {
        var dpe:Boolean = false;
        if (ex instanceof MultipleExceptions) {
            val deadExList = (ex as MultipleExceptions).getExceptionsOfType[DeadPlaceException]();
            val confExList = (ex as MultipleExceptions).getExceptionsOfType[ConflictException]();
            val pauseExList = (ex as MultipleExceptions).getExceptionsOfType[StorePausedException]();
            val abortedExList = (ex as MultipleExceptions).getExceptionsOfType[AbortedTransactionException]();
            val maxConcurExList = (ex as MultipleExceptions).getExceptionsOfType[ConcurrentTransactionsLimitExceeded]();
                    
            if ((ex as MultipleExceptions).exceptions.size > (deadExList.size + confExList.size + pauseExList.size + abortedExList.size)){
                Console.OUT.println(here + " Unexpected MultipleExceptions   size("+(ex as MultipleExceptions).exceptions.size + ")  (" 
                        + deadExList.size + " + " + confExList.size + " + " + pauseExList.size + " + " + abortedExList.size + " + " + maxConcurExList.size + ")");
                ex.printStackTrace();
                throw ex;
            }
            
            if (deadExList != null && deadExList.size != 0) {
                if (!immediateRecovery) {
                    throw ex;
                } else {
                    System.threadSleep(TxConfig.get().DPE_SLEEP_MS);
                    dpe = true;
                }
            }
        } else if (ex instanceof DeadPlaceException) {
            if (!immediateRecovery) {
                throw ex;
            } else {
                System.threadSleep(TxConfig.get().DPE_SLEEP_MS);
                dpe = true;
            }
        } else if (ex instanceof StorePausedException) {
            System.threadSleep(TxConfig.get().DPE_SLEEP_MS);
        } else if (ex instanceof ConcurrentTransactionsLimitExceeded) {
            System.threadSleep(TxConfig.get().DPE_SLEEP_MS);
        } else if (!(ex instanceof ConflictException || ex instanceof AbortedTransactionException  )) {
            throw ex;
        }
        return dpe;
    }
    
    public def printTxStatistics() {
        Console.OUT.println("Calculating execution statistics ...");
        val pl_stat = new ArrayList[TxPlaceStatistics]();
        for (p in plh().getActivePlaces()) {
            val pstat = at (p) plh().stat;
            pl_stat.add(pstat);
        }
        
        val g_allCommitList = new ArrayList[Double]();
        val g_allCommitProcList = new ArrayList[Double]();
        val g_allPH1List = new ArrayList[Double]();
        val g_allPH2List = new ArrayList[Double]();
        val g_allTxLoggingList = new ArrayList[Double]();
        val g_allAbortList = new ArrayList[Double]();
        val g_allAbortProcList = new ArrayList[Double]();
        var g_cPlaces:Long = 0;
        var g_aPlaces:Long = 0;
        
        val l_allCommitList = new ArrayList[Double]();
        val l_allCommitProcList = new ArrayList[Double]();
        val l_allAbortList = new ArrayList[Double]();
        val l_allAbortProcList = new ArrayList[Double]();
        var l_cPlaces:Long = 0;
        var l_aPlaces:Long = 0;
        
        val lk_allTotalList = new ArrayList[Double]();
        val lk_allLockList = new ArrayList[Double]();
        val lk_allProcList = new ArrayList[Double]();
        val lk_allUnlockList = new ArrayList[Double]();
        var lk_Places:Long = 0;
        
        for (pstat in pl_stat) {
            val str = pstat.toString();
            if (TM_STAT_ALL && !str.equals(""))
                Console.OUT.println(str);
            g_allCommitList.addAll(pstat.g_commitList);
            g_allCommitProcList.addAll(pstat.g_commitProcList);
            g_allPH1List.addAll(pstat.g_commitPH1List);
            g_allPH2List.addAll(pstat.g_commitPH2List);
            g_allTxLoggingList.addAll(pstat.g_txLoggingList);
            
            if (pstat.g_commitList.size() > 0)
                g_cPlaces ++;
            
            g_allAbortList.addAll(pstat.g_abortList);
            g_allAbortProcList.addAll(pstat.g_abortProcList);
            if (pstat.g_abortList.size() > 0)
                g_aPlaces ++;
            
            l_allCommitList.addAll(pstat.l_commitList);
            l_allCommitProcList.addAll(pstat.l_commitProcList);
            if (pstat.l_commitList.size() > 0)
                l_cPlaces ++;
            
            l_allAbortList.addAll(pstat.l_abortList);
            l_allAbortProcList.addAll(pstat.l_abortProcList);
            if (pstat.l_abortList.size() > 0)
                l_aPlaces ++;
            
            
            lk_allTotalList.addAll(pstat.lk_totalList);
            lk_allLockList.addAll(pstat.lk_lockList);
            lk_allProcList.addAll(pstat.lk_procList);
            lk_allUnlockList.addAll(pstat.lk_unlockList);
            
            if (pstat.lk_totalList.size() > 0)
                lk_Places ++;
        }
        
        val g_cCnt       = g_allCommitList.size();
        val g_cMean      = TxStatistics.mean(g_allCommitList);
        val g_cSTDEV     = TxStatistics.stdev(g_allCommitList, g_cMean);
        val g_cBox       = TxStatistics.boxPlot(g_allCommitList);
        val g_cProcMean  = TxStatistics.mean(g_allCommitProcList);
        val g_cProcSTDEV = TxStatistics.stdev(g_allCommitProcList, g_cProcMean);
        val g_cProcBox   = TxStatistics.boxPlot(g_allCommitProcList);
        val g_cPH1Mean   = TxStatistics.mean(g_allPH1List);
        val g_cPH1STDEV  = TxStatistics.stdev(g_allPH1List, g_cPH1Mean);
        val g_cPH1Box    = TxStatistics.boxPlot(g_allPH1List);
        val g_cPH2Mean   = TxStatistics.mean(g_allPH2List);
        val g_cPH2STDEV  = TxStatistics.stdev(g_allPH2List, g_cPH2Mean);
        val g_cPH2Box    = TxStatistics.boxPlot(g_allPH2List);
        val g_cLogMean   = TxStatistics.mean(g_allTxLoggingList);
        val g_cLogSTDEV  = TxStatistics.stdev(g_allTxLoggingList, g_cLogMean);
        val g_cLogBox    = TxStatistics.boxPlot(g_allTxLoggingList);
        val g_aCnt       = g_allAbortList.size();
        val g_aMean      = TxStatistics.mean(g_allAbortList);
        val g_aSTDEV     = TxStatistics.stdev(g_allAbortList, g_aMean);
        val g_aBox       = TxStatistics.boxPlot(g_allAbortList);
        val g_aProcMean  = TxStatistics.mean(g_allAbortProcList);
        val g_aProcSTDEV = TxStatistics.stdev(g_allAbortProcList, g_aProcMean);
        val g_aProcBox   = TxStatistics.boxPlot(g_allAbortProcList);
        
        val l_cCnt       = l_allCommitList.size();
        val l_cMean      = TxStatistics.mean(l_allCommitList);
        val l_cSTDEV     = TxStatistics.stdev(l_allCommitList, l_cMean);
        val l_cBox       = TxStatistics.boxPlot(l_allCommitList);
        val l_cProcMean  = TxStatistics.mean(l_allCommitProcList);
        val l_cProcSTDEV = TxStatistics.stdev(l_allCommitProcList, l_cProcMean);
        val l_cProcBox   = TxStatistics.boxPlot(l_allCommitProcList);
        val l_aCnt       = l_allAbortList.size();
        val l_aMean      = TxStatistics.mean(l_allAbortList);
        val l_aSTDEV     = TxStatistics.stdev(l_allAbortList, l_aMean);
        val l_aBox       = TxStatistics.boxPlot(l_allAbortList);
        val l_aProcMean  = TxStatistics.mean(l_allAbortProcList);
        val l_aProcSTDEV = TxStatistics.stdev(l_allAbortProcList, l_aProcMean);
        val l_aProcBox   = TxStatistics.boxPlot(l_allAbortProcList);
                
        
        val lk_Cnt        = lk_allTotalList.size();
        val lk_totalMean  = TxStatistics.mean(lk_allTotalList);
        val lk_totalSTDEV = TxStatistics.stdev(lk_allTotalList, lk_totalMean);
        val lk_totalBox   = TxStatistics.boxPlot(lk_allTotalList);
        val lk_lockMean  = TxStatistics.mean(lk_allLockList);
        val lk_lockSTDEV = TxStatistics.stdev(lk_allLockList, lk_lockMean);
        val lk_lockBox   = TxStatistics.boxPlot(lk_allLockList);
        val lk_procMean  = TxStatistics.mean(lk_allProcList);
        val lk_procSTDEV = TxStatistics.stdev(lk_allProcList, lk_procMean);
        val lk_procBox   = TxStatistics.boxPlot(lk_allProcList);
        val lk_unlockMean  = TxStatistics.mean(lk_allUnlockList);
        val lk_unlockSTDEV = TxStatistics.stdev(lk_allUnlockList, lk_unlockMean);
        val lk_unlockBox   = TxStatistics.boxPlot(lk_allUnlockList);
        
        if (g_cCnt > 0) {            
            Console.OUT.println("Summary:GLOBAL_TX:committedTxs:"     + g_cCnt       + ":committedPlaces:" + g_cPlaces ); 
            Console.OUT.println("Summary:GLOBAL_TX:commitMeanMS:"     + g_cMean      + ":commitSTDEV:"     + g_cSTDEV     + ":commitBox:(:"     + g_cBox + ":)" ); 
            Console.OUT.println("Summary:GLOBAL_TX:commitProcMeanMS:" + g_cProcMean  + ":commitProcSTDEV:" + g_cProcSTDEV + ":commitProcBox:(:" + g_cProcBox + ":)" );
            Console.OUT.println("Summary:GLOBAL_TX:ph1MeanMS:"        + g_cPH1Mean   + ":ph1STDEV:"        + g_cPH1STDEV  + ":ph1Box:(:"        + g_cPH1Box + ":)" );
            Console.OUT.println("Summary:GLOBAL_TX:ph2MeanMS:"        + g_cPH2Mean   + ":ph2STDEV:"        + g_cPH2STDEV  + ":ph2Box:(:"        + g_cPH2Box + ":)" );
            Console.OUT.println("Summary:GLOBAL_TX:logMeanMS:"        + g_cLogMean   + ":logSTDEV:"        + g_cLogSTDEV  + ":logBox:(:"        + g_cLogBox + ":)" );
            Console.OUT.println("Summary:GLOBAL_TX:abortedTxs:"       + g_aCnt       + ":abortedPlaces:"   + g_aPlaces );
            Console.OUT.println("Summary:GLOBAL_TX:abortMeanMS:"      + g_aMean      + ":abortSTDEV:"      + g_aSTDEV     + ":abortBox:(:"      + g_aBox + ":)" );
            Console.OUT.println("Summary:GLOBAL_TX:abortProcMeanMS:"  + g_aProcMean  + ":abortProcSTDEV:"  + g_aProcSTDEV + ":abortProcBox:(:"  + g_aProcBox + ":)" );
        }
        
        if (l_cCnt > 0) {
            Console.OUT.println("Summary:LOCAL_TX:committedTxs:"      + l_cCnt       + ":committedPlaces:" + l_cPlaces );
            Console.OUT.println("Summary:LOCAL_TX:commitMeanMS:"      + l_cMean      + ":commitSTDEV:"     + l_cSTDEV     + ":commitBox:(:"     + l_cBox + ":)" );
            Console.OUT.println("Summary:LOCAL_TX:commitProcMeanMS:"  + l_cProcMean  + ":commitProcSTDEV:" + l_cProcSTDEV + ":commitProcBox:(:" + l_cProcBox + ":)" );
            Console.OUT.println("Summary:LOCAL_TX:abortedTxs:"        + l_aCnt       + ":abortedPlaces:"   + l_aPlaces );
            Console.OUT.println("Summary:LOCAL_TX:abortMeanMS:"       + l_aMean      + ":abortSTDEV:"      + l_aSTDEV     + ":abortBox:(:"      + l_aBox + ":)" );
            Console.OUT.println("Summary:LOCAL_TX:abortProcMeanMS:"   + l_aProcMean  + ":abortProcSTDEV:"  + l_aProcSTDEV + ":abortProcBox:(:"  + l_aProcBox + ":)" );
        }
        
        if (lk_Cnt > 0) {
            Console.OUT.println("Summary:LOCKING_TX:count:"       + lk_Cnt            + ":Places:"     + lk_Places );
            Console.OUT.println("Summary:LOCKING_TX:totalMeanMS:"  + lk_totalMean  + ":totalSTDEV:"  + lk_totalSTDEV    + ":totalBox:(:"    + lk_totalBox + ":)" );
            Console.OUT.println("Summary:LOCKING_TX:lockMeanMS:"   + lk_lockMean   + ":lockSTDEV:"   + lk_lockSTDEV     + ":lockBox:(:"     + lk_lockBox + ":)" );
            Console.OUT.println("Summary:LOCKING_TX:procMeanMS:"   + lk_procMean   + ":procSTDEV:"   + lk_procSTDEV     + ":procBox:(:"     + lk_procBox + ":)" );
            Console.OUT.println("Summary:LOCKING_TX:unlockMeanMS:" + lk_unlockMean + ":unlockSTDEV:" + lk_unlockSTDEV   + ":unlockBox:(:"   + lk_unlockBox + ":)" );
        }
    }
    
    public def resetTxStatistics() {
        finish for (p in plh().getActivePlaces()) at (p) async {
            plh().stat.clear();
        }
    }
    
    private static def railToString(sorted:Rail[Double], name:String) {
        var str:String = name + ":";
        for (v in sorted) {
            str += v + ":";
        }
        return str;
    }
    
}