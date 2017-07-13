/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

import x10.xrx.Runtime;
import x10.compiler.Native;
import x10.compiler.Pragma;

public class SPMDFinishBench {

    static OUTER_ITERS = 100;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)

    static RESILIENT = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    @Native("c++", "false")
    @Native("java", "true")
    static native def needsWarmup():Boolean;
    
    public static def main(args:Rail[String]){here==Place.FIRST_PLACE}{
        if (Place.numPlaces() < 2) {
            Console.ERR.println("Fair evaluation of place-zero based finish requires more than one place, and preferably more than one host.");
            System.setExitCode(1n);
            return;
        }

        val think:Long = args.size == 0 ? 0 : Long.parse(args(0));

        if (Runtime.RESILIENT_MODE == 0n) {
            Console.OUT.println("Configuration: DEFAULT (NON-RESILIENT)");
        } else {
            Console.OUT.println("Configuration: RESILIENT MODE "+Runtime.RESILIENT_MODE);
        }

        Console.OUT.println("Running with "+Place.numPlaces()+" places.");
        Console.OUT.println("Min elapsed time for each test: "+MIN_NANOS/1e9+" seconds.");
        Console.OUT.println("Think time for each activity: "+think+" nanoseconds.");

        if (needsWarmup()) {
            Console.OUT.println("Doing warmup for ManagedX10 -- expect 1 minute delay");
            doTest("warmup", think, false, MIN_NANOS/2);
            Console.OUT.println("Warmup complete");
        }
        
        Console.OUT.println("Test based from place 0");
        doTest("place 0 -- ", think, true, MIN_NANOS);
        Console.OUT.println();

        Console.OUT.println("Test based from place 1");
        at (Place(1)) doTest("place 1 -- ", think, true, MIN_NANOS);
        Console.OUT.println();
    }

    public static def doTest(prefix:String, t:Long, print:Boolean, minTime:Long) {
        var time0:Long, time1:Long;
        var iterCount:Long;
        val home = here;
        
        iterCount = 0;
        time0 = System.nanoTime();
        do {
            for (i in 1..OUTER_ITERS) {
                finish {
                    for (p in Place.places()) {
                        at (p) async { think(t); }
                    }
                }
            }
            time1 = System.nanoTime();
            iterCount++;
        } while (time1-time0 < minTime);
        if (print) Console.OUT.println(prefix+"flat fan out: "+(time1-time0)/1E9/OUTER_ITERS/iterCount+" seconds");

        if (RESILIENT) {
	        iterCount = 0;
	        time0 = System.nanoTime();
	        do {
	            for (i in 1..OUTER_ITERS) {
	            	finish {
	                    Runtime.runAsyncSPMD(Place.places(), ()=>{
	                    	think(t);
	                	}, null);
	                }
	            }
	            time1 = System.nanoTime();
	            iterCount++;
	        } while (time1-time0 < minTime);
	        if (print) Console.OUT.println(prefix+"flat fan out - with SPMD optimization: "+(time1-time0)/1E9/OUTER_ITERS/iterCount+" seconds");
        }
        else {
	        iterCount = 0;
	        time0 = System.nanoTime();
	        do {
	            for (i in 1..OUTER_ITERS) {
	            	@Pragma(Pragma.FINISH_SPMD) finish {
	                    for (p in Place.places()) {
	                        at (p) async { think(t); }
	                    }
	                }
	            }
	            time1 = System.nanoTime();
	            iterCount++;
	        } while (time1-time0 < minTime);
	        if (print) Console.OUT.println(prefix+"flat fan out - with SPMD optimization: "+(time1-time0)/1E9/OUTER_ITERS/iterCount+" seconds");
        }
    }



    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
}
