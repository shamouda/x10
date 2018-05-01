/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2014-2016.
 */
import harness.x10Test;

import x10.util.Team;

/**
 * Benchmarks performance of Team.bcast for varying data size
 */
public class BenchmarkBcast extends x10Test {
    private static ITERS = 10;
    //private static MAX_SIZE = 2<<19;
    private static MAX_SIZE = 2<<10; 
    private static VALIDATE = false;
    
	public def run(): Boolean {
        val root = Place(Place.numPlaces()-1);
        finish for (place in Place.places()) at (place) async {
            val warmup = new Rail[Double](1);
            Team.WORLD.bcast(root, warmup, 0, warmup, 0, 1); // warm up comms layer
            //for (var s:Long= 1; s <= MAX_SIZE; s *= 2) 
            val s = MAX_SIZE;
            {
                var timesStr:String = "";
                val src = new Rail[Double](s, (i:Long) => i as Double);
                val dst = new Rail[Double](s);
                val start = System.nanoTime();
                for (iter in 1..ITERS) {
                    val startX = System.nanoTime();
                    Team.WORLD.bcast(root, src, 0, dst, 0, s);
                    val stopX = System.nanoTime();
                    timesStr += (((stopX-startX) as Double) / 1e6) + ":";
                }
                val stop = System.nanoTime();

                if (VALIDATE) {
                    // check correctness
                    for (i in 0..(s-1)) {
                        chk(dst(i) == src(i), "elem " + i + " is " + dst(i) + " should be " + src(i));
                    }
                }

                if (here == Place.FIRST_PLACE) {
                    Console.OUT.printf("bcast %d: %g ms\n", s, ((stop-start) as Double) / 1e6 / ITERS);
                    Console.OUT.println("bcastAllValues (ms):" + timesStr);
                }
            }
        }

        return true;
	}

	public static def main(var args: Rail[String]): void {
        Console.OUT.println("RESILIENT_MODE="+x10.xrx.Runtime.RESILIENT_MODE + " ITER=" + ITERS);
		new BenchmarkBcast().execute();
	}
}
