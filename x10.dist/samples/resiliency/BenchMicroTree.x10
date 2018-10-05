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

/**
 * This is a microbenchmark used to isolate the
 * overheads of different resilient finish implementations
 * for common combinations of finish/async/at.
 * An earlier version of this code was used to 
 * generate figure 6 in the PPoPP'14 paper on Resilient X10
 * (http://dx.doi.org/10.1145/2555243.2555248).
 */
public class BenchMicroTree {

    static OUTER_ITERS = System.getenv("BENCHMICRO_ITER") == null ? 10 : Long.parseLong(System.getenv("BENCHMICRO_ITER"));
    static INNER_ITERS = 100;
    static MIN_NANOS = -1; //control the numbers of repeatitions using OUTER_ITERS only
                      //(10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)

    @Native("c++", "true")
    @Native("java", "true")
    static native def needsWarmup():Boolean;

    public static def main(args:Rail[String]){here==Place.FIRST_PLACE}{
        val refTime = System.currentTimeMillis();
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
        Console.OUT.println("OUTER_ITERS: "+OUTER_ITERS);
        Console.OUT.println("Min elapsed time for each test: "+MIN_NANOS/1e9+" seconds.");
        Console.OUT.println("Think time for each activity: "+think+" nanoseconds.");

        if (needsWarmup()) {
            Console.OUT.println("Doing warmup");
            warmpUp(refTime, "warmup", think, false, MIN_NANOS/2);
            Console.OUT.println("Warmup complete");
        }
        
        val basePlace = System.getenv("TEST_BASED_FROM") == null ? Place.numPlaces()/2 : Long.parseLong(System.getenv("TEST_BASED_FROM"));
        if (basePlace >= Place.numPlaces()) {
            Console.ERR.println("invalid base place value");
            System.setExitCode(1n);
            return;
        }
        
        var t0:Long = System.nanoTime();
        if (basePlace == 0) {
            Console.OUT.println("Test based from place 0");
            doTestTree(refTime, "place 0 -- ", think, true, MIN_NANOS);
        } else {
            Console.OUT.println("Test based from place " + basePlace);
            doTestTree(refTime, "place 0 -- ", think, true, MIN_NANOS);
        }
        Console.OUT.printf("Test based from place "+basePlace+" completed in %f seconds\n", (System.nanoTime()-t0)/1e9);
        Console.OUT.println();
    }
    
    static def println(time0:Long, message:String) {
        val time = System.currentTimeMillis();
        val s = "        " + (time - time0);
        val s1 = s.substring(s.length() - 9n, s.length() - 3n);
        val s2 = s.substring(s.length() - 3n, s.length());
        Console.OUT.println(s1 + "." + s2 + ": " + message);
        return time;
    }
    
    public static def warmpUp(refTime:Long, prefix:String, t:Long, print:Boolean, minTime:Long) {
        finish {
            for (p in Place.places()) {
                at (p) async {
                    finish {
                        for (q in Place.places()) at (q) async {
                            think(t);
                        }
                    }
                }
            }
        }
    }

    public static def doTestTree(refTime:Long, prefix:String, t:Long, print:Boolean, minTime:Long) {
        var time0:Long, time1:Long;
        var iterCount:Long = 0;
        val home = here;
        time0 = System.nanoTime();
        do {
            for (i in 1..OUTER_ITERS) {
                val t1 = System.nanoTime();
                downTree(t);
                val t2 = System.nanoTime();
                if (print) println(refTime, prefix+"["+i+"] tree fan out: "+(t2-t1)/1E9+" seconds");
            }
            time1 = System.nanoTime();
            iterCount++;
        } while (time1-time0 < minTime);
        if (print) println(refTime, prefix+"average tree fan out: "+(time1-time0)/1E9/OUTER_ITERS/iterCount+" seconds");
    }
    
    private static def downTree(thinkTime:long):void {
        think(thinkTime);
        val parent = here.id;
        val child1 = parent*2 + 1;
        val child2 = child1 + 1;
        if (parent == 1 && Place.numPlaces() == 2) {
            // special case to invert tree so scaling calcuation is reasonable.
            finish at (Place(0)) async think(thinkTime);
        }
        if (child1 < Place.numPlaces() || child2 < Place.numPlaces()) {
            finish {
                if (child1 < Place.numPlaces()) at (Place(child1)) async downTree(thinkTime);
                if (child2 < Place.numPlaces()) at (Place(child2)) async downTree(thinkTime);
            }
        }
    }

    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
}