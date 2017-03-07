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
	public val TM:String; //locking|RL_EA_UL|RL_EA_WB|RL_LA_WB|RV_EA_UL|RV_EA_WB|RV_LA_WB
	
    public val LOCKING_MODE:Int;
    public val VALIDATION_REQUIRED:Boolean;
    public val TM_READ:Int;
    public val TM_ACQUIRE:Int;
    public val TM_RECOVER:Int;
    
    /*locking modes*/
    public static val LOCKING_MODE_FREE = 0n;
    public static val LOCKING_MODE_BLOCKING = 1n;
    public static val LOCKING_MODE_STM = 2n;
    
    /*STM TM dimensions*/
    public static val INVALID = -1n;
    public static val EARLY_ACQUIRE= 1n;
    public static val LATE_ACQUIRE = 2n;
    public static val UNDO_LOGGING= 1n;
    public static val WRITE_BUFFERING = 2n;
    public static val READ_LOCKING= 1n;
    public static val READ_VERSIONING = 2n;
    
    private static val instance = new TxConfig();
    
    private def this(){
        TM = System.getenv("TM");
        val lockfree = (System.getenv("LOCK_FREE") == null || System.getenv("LOCK_FREE").equals("")) ? false : Long.parseLong(System.getenv("LOCK_FREE")) == 1;
        assert (TM != null && !TM.equals("")) : "you must specify the TM environment variable, allowed values = locking|RL_EA_UL|RL_EA_WB|...";
        
        	
        if (TM.contains("locking")) {
        	if (lockfree)
        		LOCKING_MODE = LOCKING_MODE_FREE;
        	else
        		LOCKING_MODE = LOCKING_MODE_BLOCKING;
            VALIDATION_REQUIRED = false;
            TM_READ = INVALID;
            TM_ACQUIRE = INVALID;
            TM_RECOVER = INVALID;
        }
        else {
        	if (lockfree)
        		LOCKING_MODE = LOCKING_MODE_FREE;
        	else
        		LOCKING_MODE = LOCKING_MODE_STM;
            if (TM.contains("RL"))
                TM_READ = READ_LOCKING;
            else
                TM_READ = READ_VERSIONING;
            
            if (TM.contains("EA"))
                TM_ACQUIRE = EARLY_ACQUIRE;
            else
                TM_ACQUIRE = LATE_ACQUIRE;
            
            if (TM.contains("UL"))
                TM_RECOVER = UNDO_LOGGING;
            else 
                TM_RECOVER = WRITE_BUFFERING;
            
            if (TM_READ == READ_LOCKING && TM_ACQUIRE == EARLY_ACQUIRE )
                VALIDATION_REQUIRED = false;
            else
                VALIDATION_REQUIRED = true;
        }
    }
    
    public static def getInstance() = instance;
    
}