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
	    
	    //warm-up
	    val startW = System.nanoTime();
        new Team(Place.places());
        val stopW = System.nanoTime();
        
        var totalNative:Long = 0;
        var totalBcast:Long = 0;
        val start = System.nanoTime();
        for (iter in 1..ITERS) {
            val team = new Team(Place.places());
            totalNative += team.thisNativeCreateNano;
            totalBcast += team.thisBcastNano;
        }
        val stop = System.nanoTime();
        Console.OUT.printf("newTeam warmup time: %g ms \n", ((stopW-startW) as Double) / 1e6);
        Console.OUT.printf("newTeam %d places: %g ms \n", Place.numPlaces(), ((stop-start) as Double) / 1e6 / ITERS);
        Console.OUT.printf("newTeam %d places thisNativeCreateNano: %g ms \n", Place.numPlaces(), (totalNative as Double) / 1e6 / ITERS);
        Console.OUT.printf("newTeam %d places thisBcastNano: %g ms \n", Place.numPlaces(), (totalBcast as Double) / 1e6 / ITERS);
        
        return true;
	}

	public static def main(var args: Rail[String]): void {
        Console.OUT.println("RESILIENT_MODE="+x10.xrx.Runtime.RESILIENT_MODE + " ITER=" + ITERS);
		new BenchmarkNewTeam().execute();
	}
}
