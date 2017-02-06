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

package x10.util.resilient.localstore.tx;

import x10.util.concurrent.SimpleLatch;
import x10.xrx.Runtime;

public class TxFuture(txId:Long, fid:Long, targetPlace:Place) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    private var value:Any;
    private var exception:Exception = null;
    private val latch = new SimpleLatch();
    
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public def test() = latch();
    
    public def fill(v:Any, ex:Exception) {
        value = v;
        exception = ex;
        latch.release();
    }
    
    /* Changed the name from wait() to waitV() because Java has a Object.wait() 
     * function that gets confused with this function*/
    public def waitV():Any {
    	if (!latch()) {
    		if (resilient) {
	    		Runtime.increaseParallelism();
	    		while (!latch() && !targetPlace.isDead())
	    			System.threadSleep(0);
	    		Runtime.decreaseParallelism(1n);
	        	
	            if (targetPlace.isDead())
	                throw new DeadPlaceException(targetPlace);
	        }
    		else {
    			latch.await();
    		}
    	}
        
        if (exception != null)
            throw exception;
        
        return value;
    }
    
    public def toString() {
        return "Tx["+txId+"] here["+here+"] future ["+fid+"] complete["+latch()+"]";
    }
}