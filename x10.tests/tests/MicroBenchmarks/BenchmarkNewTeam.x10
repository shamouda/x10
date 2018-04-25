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
 * Benchmarks performance of new Team()
 */
public class BenchmarkNewTeam extends x10Test {
    private static ITERS = 10;
    
	public def run(): Boolean {
        new Team(Place.places());
        val start = System.nanoTime();
        for (iter in 1..ITERS) {
            new Team(Place.places());
        }
        val stop = System.nanoTime();
        if (here == Place.FIRST_PLACE) Console.OUT.printf("newTeam %d places: %g ms\n", Place.numPlaces(), ((stop-start) as Double) / 1e6 / ITERS);
        return true;
	}

	public static def main(var args: Rail[String]): void {
        Console.OUT.println("RESILIENT_MODE="+x10.xrx.Runtime.RESILIENT_MODE + " ITER=" + ITERS);
		new BenchmarkNewTeam().execute();
	}
}
