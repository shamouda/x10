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
import x10.compiler.Immediate;
import x10.util.concurrent.Condition;

public class PingPong {

    static OUTER_ITERS = 10;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)

    @Native("c++", "true")
    @Native("java", "true")
    static native def needsWarmup():Boolean;

    public static def main(args:Rail[String]){here==Place.FIRST_PLACE}{
        val refTime = System.currentTimeMillis();
        if (Place.numPlaces() < 2) {
            Console.ERR.println("At least 2 places required for PingPong benchmark");
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
        
        var t0:Long = System.nanoTime();
        Console.OUT.println("Test based from place 0");
        doPingPong(refTime, "place 0 -- ", think, true, MIN_NANOS);
        Console.OUT.printf("PingPong completed in %f seconds\n", (System.nanoTime()-t0)/1e9);
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
        var time0:Long, time1:Long;
        var iterCount:Long;

        iterCount = 0;
        time0 = System.nanoTime();
        do {
            finish {
                for (p in Place.places()) {
                    at (p) async {
                        for (q in Place.places()) at (q) async {
                            think(t);
                        }
                    }
                }
            }
            time1 = System.nanoTime();
            iterCount++;
        } while (time1-time0 < minTime);
        if (print) println(refTime, prefix+"fan out - broadcast: "+(time1-time0)/1E9/iterCount+" seconds");
    }

    public static def doPingPong(refTime:Long, prefix:String, t:Long, print:Boolean, minTime:Long) {
        var time0:Long, time1:Long;
        var iterCount:Long;
        val home = here;
        for (other in Place.places()) {
            if (other.id == here.id)
                continue;
            iterCount = 0;
            time0 = System.nanoTime();
            do {
                for (i in 1..OUTER_ITERS) {
                    val gr = new GlobalRef[Condition](new Condition());
                    at (other) @Immediate("ping_next") async {
                        at (home) @Immediate("pong_home") async {
                            gr().release();
                        }                    
                    }
                    gr().await();
                }
                time1 = System.nanoTime();
            iterCount++;
            } while (time1-time0 < minTime);
            if (print) println(refTime, prefix+"ping pong time:"+home.id+":"+other.id+": "+(time1-time0)/OUTER_ITERS/iterCount+" nanoseconds");
        }

    }

    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
}
