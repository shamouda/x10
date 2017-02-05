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

import x10.util.concurrent.AtomicBoolean;
import x10.xrx.Runtime;

public class TxFuture(txId:Long, fid:Long, targetPlace:Place) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private static val TM_FUTURE_WAIT = System.getenv("TM_FUTURE_WAIT") == null ? "loop" : System.getenv("TM_FUTURE_WAIT");
    
    private var value:Any;
    private var complete:Boolean = false;
    private var exception:Exception = null;

    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;

    public def test() = complete;
    
    public def fill(v:Any, ex:Exception) {
        atomic {
            value = v;
            complete = true;
            exception = ex;
        }
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] here["+here+"] future ["+fid+"] completed ...");
    }
    
    /* Changed the name from wait() to waitV() because Java has a Object.wait() 
     * function that gets confused with this function*/
    public def waitV():Any {
        if (TM_FUTURE_WAIT.equals("when")) {
           val startWhen = System.nanoTime();
           when (complete);
           val endWhen = System.nanoTime();
           if(TM_DEBUG) Console.OUT.println("Tx["+txId+"] when waiting time: [" + ((endWhen-startWhen)/1e6) + "] ms");
           if (exception != null)
               throw exception;
        }
        else {
            var c:Long = 0;
            Runtime.increaseParallelism();
            while (!complete && !targetPlace.isDead()) {
                System.threadSleep(0);
                c++;
                if ( c%100 == 0 && TM_DEBUG )
                    Console.OUT.println("Tx["+txId+"] future waiting for place["+targetPlace+"]  isDead["+targetPlace.isDead()+"] ");
            }
            Runtime.decreaseParallelism(1n);
            if (targetPlace.isDead()) {
                if(TM_DEBUG) Console.OUT.println("Tx["+txId+"] future ["+fid+"] here["+here+"] throwing DPE targetPlace["+targetPlace+"]");
                throw new DeadPlaceException(targetPlace);
            }
            if (exception != null) {
                if(TM_DEBUG) Console.OUT.println("Tx["+txId+"] future ["+fid+"] here["+here+"] throwing exception["+exception.getMessage()+"] targetPlace["+targetPlace+"]");
                throw exception;
            }
        }
        return value;
    }
    
    public def toString() {
        return "Tx["+txId+"] here["+here+"] future ["+fid+"] complete["+complete+"]";
    }
}