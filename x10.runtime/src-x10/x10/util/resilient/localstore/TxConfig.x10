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

public class TxConfig {
    public val TM:String; //baseline|locking|RL_EA_UL|RL_EA_WB|RL_LA_WB|RV_EA_UL|RV_EA_WB|RV_LA_WB
    public val TM_REP:String; //lazy|eager
    public val VALIDATION_REQUIRED:Boolean;
    public val BUCKETS_COUNT:Long;
    public val DISABLE_INCR_PARALLELISM:Boolean;
    
    public val BASELINE:Boolean;
    public val STM:Boolean;
    public val LOCKING:Boolean;
    public val LOCK_FREE:Boolean;
    
    private static val instance = new TxConfig();
    
    private def this(){
        TM = System.getenv("TM");
        assert (TM != null && !TM.equals("")) : "you must specify the TM environment variable, allowed values = locking|RL_EA_UL|RL_EA_WB|...";

        if (TM.equals("baseline") || TM.startsWith("locking") || TM.equals("RL_EA_UL") || TM.equals("RL_EA_WB")) {
            VALIDATION_REQUIRED = false;
        }
        else if (TM.equals("RL_LA_WB") || TM.equals("RV_EA_UL") || TM.equals("RV_EA_WB") || TM.equals("RV_LA_WB")) {
            VALIDATION_REQUIRED = true;
        }
        else {
            VALIDATION_REQUIRED = false;
            assert(false) : "Invalid TM value, possible values are: baseline|locking|RL_EA_UL|RL_EA_WB|RL_LA_WB|RV_EA_UL|RV_EA_WB|RV_LA_WB";
        }
        
        BUCKETS_COUNT = (System.getenv("BUCKETS_COUNT") == null || System.getenv("BUCKETS_COUNT").equals("")) ? 256 : Long.parseLong(System.getenv("BUCKETS_COUNT"));
        DISABLE_INCR_PARALLELISM = (System.getenv("DISABLE_INCR_PARALLELISM") == null || System.getenv("DISABLE_INCR_PARALLELISM").equals("")) ? false : Long.parseLong(System.getenv("DISABLE_INCR_PARALLELISM")) == 1;

        if (TM.equals("baseline"))
            BASELINE = true;
        else
            BASELINE = false;
        
        if (BASELINE || TM.startsWith("locking")) 
            STM = false;
        else
            STM = true;
        
        LOCKING = ! ( STM || BASELINE);
        
        LOCK_FREE = BASELINE || (System.getenv("LOCK_FREE") == null || System.getenv("LOCK_FREE").equals("")) ? false : Long.parseLong(System.getenv("LOCK_FREE")) == 1;
        
        TM_REP = System.getenv("TM_REP") == null ? "lazy" : System.getenv("TM_REP");
    }
    
    public static def getInstance() = instance;
    
}