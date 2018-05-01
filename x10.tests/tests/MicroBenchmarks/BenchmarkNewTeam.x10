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
        new Team(Place.places());
        
        var totalNative:Long = 0;
        var totalBcast:Long = 0;
        
	    var times1Str:String = "";
        var times2Str:String = "";
        var timesTotalStr:String = "";
        
        val start = System.nanoTime();
        for (iter in 1..ITERS) {
            val team = new Team(Place.places());
            
            totalNative += team.thisNativeCreateNano;
            times1Str += team.thisNativeCreateNano + ":";
            
            totalBcast += team.thisBcastNano;
            times2Str += team.thisBcastNano + ":";
            
            timesTotalStr += (team.thisNativeCreateNano + team.thisBcastNano) + ":";
        }
        val stop = System.nanoTime();
        Console.OUT.printf("newTeam %d places thisNativeCreate: %g ms \n", Place.numPlaces(), (totalNative as Double) / 1e6 / ITERS);
        Console.OUT.printf("newTeam %d places thisBcast: %g ms \n", Place.numPlaces(), (totalBcast as Double) / 1e6 / ITERS);
        
        Console.OUT.println("newTeamAllValues thisNativeCreate (ms):" + times1Str);
        Console.OUT.println("newTeamAllValues thisBcast (ms):" + times2Str);
        Console.OUT.println("newTeamAllValues total (ms):" + timesTotalStr);
        
        return true;
	}

	public static def main(var args: Rail[String]): void {
        Console.OUT.println("RESILIENT_MODE="+x10.xrx.Runtime.RESILIENT_MODE + " ITER=" + ITERS);
		new BenchmarkNewTeam().execute();
	}
}
