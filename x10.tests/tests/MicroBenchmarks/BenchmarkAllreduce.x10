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
 * Benchmarks performance of Team.allreduce for varying data size
 */
public class BenchmarkAllreduce extends x10Test {
    private static ITERS = 10;
    private static MAX_SIZE = 2<<19;

	public def run(): Boolean {
	    val refTime = System.currentTimeMillis();
		Console.OUT.println("Running with "+Place.numPlaces()+" places.");
        finish for (place in Place.places()) at (place) async {
            Team.WORLD.allreduce(1.0, Team.ADD); // warm up comms layer
            for (var s:Long= 1; s <= MAX_SIZE; s *= 8) {
                val src = new Rail[Double](s, (i:Long) => i as Double);
                val dst = new Rail[Double](s);
                val start = System.nanoTime();
                for (iter in 1..ITERS) {
                    Team.WORLD.allreduce(src, 0, dst, 0, s, Team.ADD);
                }
                val stop = System.nanoTime();

                // check correctness
                for (i in 0..(s-1)) {
                    chk(dst(i) == src(i)*Place.numPlaces(), "elem " + i + " is " + dst(i) + " should be " + src(i)*Place.numPlaces());
                }

                if (here == Place.FIRST_PLACE) Console.OUT.printf(timeprefix(refTime) + ": allreduce %d: %g ms\n", s, ((stop-start) as Double) / 1e6 / ITERS);
            }
        }

        return true;
	}

    static def timeprefix(time0:Long) {
        val time = System.currentTimeMillis();
        val s = "        " + (time - time0);
        val s1 = s.substring(s.length() - 9n, s.length() - 3n);
        val s2 = s.substring(s.length() - 3n, s.length());
        return (s1 + "." + s2);
    }
    
	public static def main(var args: Rail[String]): void {
		new BenchmarkAllreduce().execute();
	}
}
