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
    private static val TM_FORCE_WHEN = System.getenv("TM_FORCE_WHEN") != null && System.getenv("TM_FORCE_WHEN").equals("1");
    
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
    
    public def notifyPlaceDeath() {
        Console.OUT.println("Tx["+txId+"] future["+fid+"] futureNotifyPlaceDeath ...");
        if (targetPlace.isDead()) {
            atomic complete = true;
            Console.OUT.println("Tx["+txId+"] future["+fid+"] futureNotifyPlaceDeath  set flag to true...");
        }
        if(TM_DEBUG) Console.OUT.println("Tx["+txId+"] future["+fid+"] targetPlace["+targetPlace+"] notifyPlaceDeath ...");
    }
    
    /* Changed the name from wait() to waitV() because Java has a Object.wait() 
     * function that gets confused with this function*/
    public def waitV():Any {
       val startWhen = System.nanoTime();
       Console.OUT.println(here + " Tx["+txId+"] future["+fid+"] startwait   target["+targetPlace+"] ");
       if (!targetPlace.isDead())
           when (complete);
       Console.OUT.println(here + " Tx["+txId+"] future["+fid+"] endwait");
       val endWhen = System.nanoTime();
       
       if(TM_DEBUG) Console.OUT.println("Tx["+txId+"] when waiting time: [" + ((endWhen-startWhen)/1e6) + "] ms, targetPlaceDied["+targetPlace.isDead()+"] ");
       
       if (targetPlace.isDead())
           throw new DeadPlaceException(targetPlace);
       
       if (exception != null)
           throw exception;

        return value;
    }
    
    public def toString() {
        return "Tx["+txId+"] here["+here+"] future ["+fid+"] complete["+complete+"]";
    }
}