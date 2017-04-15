package x10.util.resilient.localstore;

import x10.util.Set;
import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.RailUtils;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LockingRequest;
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
        val id = plh().masterStore.getNextTransactionId();
        val tx = new LocalTx(plh, id, name);
        plh().txList.addLocalTx(tx);
        return tx;
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
                tx.processingElapsedTime = Timer.milliTime() - start ;
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
                    tx.processingElapsedTime = Timer.milliTime() - start;
                    
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
        assert(plh().masterStore != null && plh().virtualPlaceId != -1) : "here["+here+"] masterStore = ["+plh().masterStore+"] , virtualPlace ["+plh().virtualPlaceId+"]";
        val id = plh().masterStore.getNextTransactionId();
        val tx = new Tx(plh, id, name, members);
        try {
            var predefinedMembers:Rail[Long] = null;
            if (members != null)
                predefinedMembers = members.virtual;
            plh().txDescManager.add(id, predefinedMembers, false);
            
            plh().txList.addGlobalTx(tx);
        }catch(ex:NullPointerException) {
            ex.printStackTrace();
        }
        return tx;
    }
    
    public def restartGlobalTransaction(txDesc:TxDesc):Tx {
        assert(plh().virtualPlaceId != -1);
        val includeDead = true; // The commitHandler will take the correct actions regarding the dead master
        val members = txDesc.staticMembers? plh().getTxMembers(txDesc.virtualMembers, includeDead):null;
        val tx = new Tx(plh, txDesc.id, name,  members);
        plh().txList.addGlobalTx(tx);
        return tx;
    }
    
    public def executeTransaction(closure:(Tx)=>Any, maxRetries:Long):TxResult {
        return executeTransaction(null, closure, maxRetries);
    }
    
    public def executeTransaction(virtualMembers:Rail[Long], closure:(Tx)=>Any, maxRetries:Long):TxResult {
        var members:TxMembers = null;
        if (virtualMembers != null) 
            members = plh().getTxMembers(virtualMembers, true);
        
            var retryCount:Long = 0;
            while(true) {
                if (retryCount % 1000 == 0)
                    Console.OUT.println(here + " executeTransaction retryCount reached " + retryCount);
                if (retryCount == maxRetries)
                    throw new FatalTransactionException("Reached maximum limit for retrying a transaction");
                retryCount++;
                
                var tx:Tx = null; 
                var commitCalled:Boolean = false;
                val start = Timer.milliTime();
                try {
                    tx = startGlobalTransaction(members);
                    val out:Any;
                    finish {
                        out = closure(tx);
                    }
                    tx.processingElapsedTime = Timer.milliTime() - start ;
                    
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+tx.id+"] executeTransaction  {finish closure();} succeeded  preCommitTime["+tx.processingElapsedTime+"] ms");
                    commitCalled = true;
                    return new TxResult(tx.commit(), out);
                } catch(ex:Exception) {
                    if (tx != null && !commitCalled) {
                        tx.processingElapsedTime = Timer.milliTime() - start;
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
    
    private def startLockingTransaction(requests:ArrayList[LockingRequest]):LockingTx {
        assert(plh().virtualPlaceId != -1);
        val id = plh().masterStore.getNextTransactionId();
        val tx = new LockingTx(plh, id, name, requests);
        plh().txList.addLockingTx(tx);
        return tx;
    }
    
    public def executeLockingTransaction(lockRequests:ArrayList[LockingRequest], closure:(LockingTx)=>Any) {
        val tx = startLockingTransaction(lockRequests);
        
        tx.lock();
        
        val start = Timer.milliTime();
        val out = closure(tx);
        tx.processingElapsedTime = Timer.milliTime() - start;
        
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
            
            if ((ex as MultipleExceptions).exceptions.size > (deadExList.size + confExList.size + pauseExList.size + abortedExList.size)){
                Console.OUT.println(here + " Unexpected MultipleExceptions   size("+(ex as MultipleExceptions).exceptions.size + ")  (" 
                        + deadExList.size + " + " + confExList.size + " + " + pauseExList.size + " + " + abortedExList.size + ")");
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
        } else if (!(ex instanceof ConflictException || ex instanceof AbortedTransactionException  )) {
            throw ex;
        }
        return dpe;
    }
    
    public def printTxStatistics() {
        Console.OUT.println("Calculating execution statistics ...");
        val pl_stat = new ArrayList[TxPlaceStatistics]();
        for (p in plh().getActivePlaces()) {
            val pstat = at (p) {
                //####         Global TX         ####//
                val g_commitList = new ArrayList[Double]();
                val g_commitProcList = new ArrayList[Double]();
                val g_commitPH1List = new ArrayList[Double]();
                val g_commitPH2List = new ArrayList[Double]();
                val g_txLoggingList = new ArrayList[Double]();
                
                val g_abortList = new ArrayList[Double]();
                val g_abortProcList = new ArrayList[Double]();
                
                for (tx in plh().txList.globalTx) {
                    if (tx.commitTime != -1) {
                        g_commitList.add(tx.commitTime - tx.startTime);
                        g_commitProcList.add(tx.processingElapsedTime);
                        g_commitPH1List.add(tx.getPhase1ElapsedTime());
                        g_commitPH2List.add(tx.getPhase2ElapsedTime());
                        g_txLoggingList.add(tx.getTxLoggingElapsedTime());
                    }
                    else if (tx.abortTime != -1) {
                        g_abortList.add(tx.abortTime - tx.startTime);
                        g_abortProcList.add(tx.processingElapsedTime);
                    }
                }
                //####         Local TX         ####//
                val l_commitList = new ArrayList[Double]();
                val l_commitProcList = new ArrayList[Double]();
                
                val l_abortList = new ArrayList[Double]();
                val l_abortProcList = new ArrayList[Double]();
                
                for (tx in plh().txList.localTx) {
                    if (tx.commitTime != -1) {
                        l_commitList.add(tx.commitTime - tx.startTime);
                        l_commitProcList.add(tx.processingElapsedTime);
                    }
                    else if (tx.abortTime != -1) {
                        l_abortList.add(tx.abortTime - tx.startTime);
                        l_abortProcList.add(tx.processingElapsedTime);
                    }
                }
                
              //####         Locking TX         ####//
                val lk_totalList = new ArrayList[Double]();
                val lk_lockList = new ArrayList[Double]();
                val lk_procList = new ArrayList[Double]();
                val lk_unlockList = new ArrayList[Double]();
                
                for (tx in plh().txList.lockingTx) {
                    lk_totalList.add(tx.totalElapsedTime);
                    lk_lockList.add(tx.lockingElapsedTime);
                    lk_procList.add(tx.processingElapsedTime);
                    lk_unlockList.add(tx.unlockingElapsedTime);
                }
                 
                new TxPlaceStatistics(here, g_commitList, g_commitProcList, g_commitPH1List, g_commitPH2List, g_txLoggingList, g_abortList, g_abortProcList, 
                                            l_commitList, l_commitProcList, l_abortList, l_abortProcList,
                                            lk_totalList, lk_lockList, lk_procList, lk_unlockList)
            };
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
            g_allPH1List.addAll(pstat.g_ph1List);
            g_allPH2List.addAll(pstat.g_ph2List);
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
            plh().txList.globalTx.clear();
            plh().txList.localTx.clear();
            plh().txList.lockingTx.clear();
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

class TxPlaceStatistics(p:Place, g_commitList:ArrayList[Double], g_commitProcList:ArrayList[Double],  
                                 g_ph1List:ArrayList[Double], g_ph2List:ArrayList[Double], g_txLoggingList:ArrayList[Double], 
                                 g_abortList:ArrayList[Double], g_abortProcList:ArrayList[Double], 
                                 l_commitList:ArrayList[Double], l_commitProcList:ArrayList[Double], l_abortList:ArrayList[Double], l_abortProcList:ArrayList[Double],
                                 lk_totalList:ArrayList[Double], lk_lockList:ArrayList[Double], lk_procList:ArrayList[Double], lk_unlockList:ArrayList[Double]) {
    public def toString() {
        
        val g_commitMean      = TxStatistics.mean(g_commitList);
        val g_commitSTDEV     = TxStatistics.stdev(g_commitList, g_commitMean);
        val g_commitBox       = TxStatistics.boxPlot(g_commitList);        
        val g_commitProcMean  = TxStatistics.mean(g_commitProcList);
        val g_commitProcSTDEV = TxStatistics.stdev(g_commitProcList, g_commitProcMean);
        val g_commitProcBox   = TxStatistics.boxPlot(g_commitProcList);
        val g_ph1Mean         = TxStatistics.mean(g_ph1List);
        val g_ph1STDEV        = TxStatistics.stdev(g_ph1List, g_ph1Mean);
        val g_ph1Box          = TxStatistics.boxPlot(g_ph1List);
        val g_ph2Mean         = TxStatistics.mean(g_ph2List);
        val g_ph2STDEV        = TxStatistics.stdev(g_ph2List, g_ph2Mean);
        val g_ph2Box          = TxStatistics.boxPlot(g_ph2List);
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
    
}