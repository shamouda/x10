/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2014-2016.
 *  (C) Copyright Sara Salem Hamouda 2018.
 */
import harness.x10Test;

import x10.util.Team;

/**
 * Benchmarks performance of Team.agree
 */
public class BenchmarkAgree extends x10Test {
    private static ITERS = 30;

	public def run(): Boolean {
        finish for (place in Place.places()) at (place) async {
            Team.WORLD.agree(0n); // warm up comms layer
            val start = System.nanoTime();
            for (iter in 1..ITERS) {
                val startX = System.nanoTime();
                val out = Team.WORLD.agree(iter as Int);
                val stopX = System.nanoTime();
                
                if (here == Place.FIRST_PLACE) {
                    Console.OUT.printf("["+iter+"] agree: %g ms\n", ((stopX-startX) as Double) / 1e6);
                }
            }
            val stop = System.nanoTime();
            
            if (here == Place.FIRST_PLACE) {
                Console.OUT.printf("agree: %g ms\n", ((stop-start) as Double) / 1e6 / ITERS);
            }
        }

        return true;
	}

	public static def main(var args: Rail[String]): void {
 	    Console.OUT.println("RESILIENT_MODE="+x10.xrx.Runtime.RESILIENT_MODE + " ITER=" + ITERS);
		new BenchmarkAgree().execute();
	}
}
