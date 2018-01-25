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

    static REPS = 1000;

    public static def main(args:Rail[String]){here==Place.FIRST_PLACE}{
        val refTime = System.currentTimeMillis();
        if (Place.numPlaces() < 2) {
            Console.ERR.println("At least 2 places required for PingPong benchmark");
            System.setExitCode(1n);
            return;
        }

        val think = 0;
        Console.OUT.println("Running with "+Place.numPlaces()+" places.");
        Console.OUT.println("REPS: "+REPS);

        Console.OUT.println("Doing warmup");
        warmpUp(think);
        Console.OUT.println("Warmup complete");
        
        var t0:Long = System.nanoTime();
        Console.OUT.println("Test based from place 0");
        doPingPong(refTime, "place 0 -- ", think);
        Console.OUT.printf("PingPong completed in %f seconds\n", (System.nanoTime()-t0)/1e9);
        Console.OUT.println();
    }
    
    public static def doPingPong(refTime:Long, prefix:String, t:Long) {
        var time0:Long, time1:Long;
        val home = here;
        for (other in Place.places()) {
            if (other.id == here.id)
                continue;
            time0 = System.nanoTime();
            for (i in 1..REPS) {
                val gr = new GlobalRef[Condition](new Condition());
                at (other) @Immediate("ping_next") async { //sending a 28 byte message to other
                    at (home) @Immediate("pong_home") async { //sending a 20 byte message to home
                        gr().release();
                    }                    
                }
                gr().await();
            }
            time1 = System.nanoTime();
            println(refTime, prefix+"ping pong time:"+home.id+":"+other.id+": "+(time1-time0)/1E9/REPS+" seconds");
        }
    }
    
    static def println(time0:Long, message:String) {
        val time = System.currentTimeMillis();
        val s = "        " + (time - time0);
        val s1 = s.substring(s.length() - 9n, s.length() - 3n);
        val s2 = s.substring(s.length() - 3n, s.length());
        Console.OUT.println(s1 + "." + s2 + ": " + message);
        return time;
    }
    
    public static def warmpUp(t:Long) {
        finish {
            for (p in Place.places()) {
                at (p) async {
                    for (q in Place.places()) at (q) async {
                        think(t);
                    }
                }
            }
        }
    }

    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
}
