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

package x10.xrx.txstore;

import x10.xrx.TxStoreFatalException;

public class TxConfig {
    public static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    public static val TMREC_DEBUG = System.getenv("TMREC_DEBUG") != null && System.getenv("TMREC_DEBUG").equals("1");
    public static val DPE_SLEEP_MS = System.getenv("DPE_SLEEP_MS") == null ? 10 : Long.parseLong(System.getenv("DPE_SLEEP_MS"));
    public static val CONFLICT_SLEEP_MS = System.getenv("CONFLICT_SLEEP_MS") == null ? 0 : Long.parseLong(System.getenv("CONFLICT_SLEEP_MS"));
    public static val MIN_CONCURRENT_TXS = System.getenv("MIN_CONCURRENT_TXS") == null ? 10 : Long.parseLong(System.getenv("MIN_CONCURRENT_TXS"));
    public static val MAX_CONCURRENT_TXS = System.getenv("MAX_CONCURRENT_TXS") == null ? 1000 : Long.parseLong(System.getenv("MAX_CONCURRENT_TXS"));
    public static val PREALLOC_READERS = System.getenv("PREALLOC_READERS") == null ? 5 : Long.parseLong(System.getenv("PREALLOC_READERS"));
    public static val PREALLOC_TXKEYS = System.getenv("PREALLOC_TXKEYS") == null ? 5 : Long.parseLong(System.getenv("PREALLOC_TXKEYS"));
    public static val PREALLOC_MEMBERS = System.getenv("PREALLOC_MEMBERS") == null ? 5 : Long.parseLong(System.getenv("PREALLOC_MEMBERS"));
    //used for performance testing only
	public static val DISABLE_SLAVE = System.getenv("DISABLE_SLAVE") != null && System.getenv("DISABLE_SLAVE").equals("1");
    public static val ENABLE_STAT = System.getenv("ENABLE_STAT") != null && System.getenv("ENABLE_STAT").equals("1");
    
    public static val BUSY_LOCK = System.getenv("BUSY_LOCK") != null && System.getenv("BUSY_LOCK").equals("1");
    
    public static val MAX_LOCK_WAIT = System.getenv("MAX_LOCK_WAIT") == null ? -1 : Long.parseLong(System.getenv("MAX_LOCK_WAIT"));
    
    public static val STM = System.getenv("TM") == null || !System.getenv("TM").equals("locking");
    public static val LOCKING = System.getenv("TM") != null && System.getenv("TM").equals("locking");
    public static val LOCKING_COMPLEX = System.getenv("LOCKING_COMPLEX") != null && System.getenv("LOCKING_COMPLEX").equals("1");
    public static val WRITE_BUFFERING = System.getenv("TM") != null && !System.getenv("TM").equals("locking") && System.getenv("TM").contains("WB");

    public static val LOCK_FREE = (System.getenv("LOCK_FREE") == null || System.getenv("LOCK_FREE").equals("")) ? false : Long.parseLong(System.getenv("LOCK_FREE")) == 1;
   
    //TM=locking|RL_EA_UL|RL_EA_WB|RL_LA_WB|RV_EA_UL|RV_EA_WB|RV_LA_WB
    public static val TM = ( System.getenv("TM") == null || System.getenv("TM").equals("")) ? "RL_EA_UL" : System.getenv("TM");
    
    public static val VALIDATION_REQUIRED = System.getenv("TM") != null && (System.getenv("TM").equals("RL_LA_WB") || 
                                                                            System.getenv("TM").equals("RV_EA_UL") || 
                                                                            System.getenv("TM").equals("RV_EA_WB") || 
                                                                            System.getenv("TM").equals("RV_LA_WB"));
    
    public static val MUST_PROGRESS = System.getenv("MUST_PROGRESS") == null || System.getenv("MUST_PROGRESS").equals("1");
    
// 1 : start 
// 2 : start + submit
// 3 : start + submit + valid  
// 4 : start + submit + valid + commit  (default)
    private static val WAIT_MS = System.getenv("WAIT_MS") == null ? 10 : Long.parseLong(System.getenv("WAIT_MS"));
    public static val MASTER_WAIT_MS = System.getenv("MASTER_WAIT_MS") == null ? 50 : Long.parseLong(System.getenv("MASTER_WAIT_MS"));
    
    public static def getTxPlaceId(txId:Long):Int {
    	return (txId >> 32) as Int;
    }
    
    public static def getTxSequence(txId:Long):Int {
    	return (txId as Int);
    }
    
    public static def waitSleep() {
        System.threadSleep(WAIT_MS);
    }
    
    public static def txIdToString (txId:Long) {
        return TxManager.txIdToString(txId);
    }
}