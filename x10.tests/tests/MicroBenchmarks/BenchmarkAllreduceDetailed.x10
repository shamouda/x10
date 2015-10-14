/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2014-2015.
 */
import harness.x10Test;

import x10.util.Team;
import x10.util.Timer;

/**
 * Benchmarks performance of Team.allreduce for varying data size
 */
public class BenchmarkAllreduceDetailed extends x10Test {
    private static ITERS = 30;
    private static MAX_SIZE = 143000;

        public def run(): Boolean {
        val hdv = PlaceLocalHandle.make[LocalReduceTime](Place.places(), ()=>new LocalReduceTime(ITERS));
        finish for (place in Place.places()) at (place) async {
            Team.WORLD.allreduce(1.0, Team.ADD); // warm up comms layer
            for (var s:Long= MAX_SIZE; s <= MAX_SIZE; s *= 2) {
                val src = new Rail[Double](s, (i:Long) => i as Double);
                val dst = new Rail[Double](s);
                val start = Timer.milliTime();
                for (iter in 0..(ITERS-1)) {
                    hdv().allReduceTime(iter) -= Timer.milliTime();
                    Team.WORLD.allreduce(src, 0, dst, 0, s, Team.ADD);
                    hdv().allReduceTime(iter) += Timer.milliTime();
                }
                val stop = Timer.milliTime();

                var allTimes:String = "";
                for (iter in 0..(ITERS-1)){
                    allTimes += hdv().allReduceTime(iter) + ";";
                }

                Console.OUT.println(here.id+" ;" +  allTimes );

                val size = ITERS;

                val dstMax = new Rail[Long](size);
                val dstMin = new Rail[Long](size);
                Team.WORLD.allreduce(hdv().allReduceTime, 0, dstMax, 0, size, Team.MAX);
                Team.WORLD.allreduce(hdv().allReduceTime, 0, dstMin, 0, size, Team.MIN);


                // check correctness
                for (i in 0..(s-1)) {
                    chk(dst(i) == src(i)*Place.numPlaces(), "elem " + i + " is " + dst(i) + " should be " + src(i)*Place.numPlaces());
                }

                if (here == Place.FIRST_PLACE) {
                    var maxString:String = "";
                    for (iter in 0..(ITERS-1)){
                        maxString += dstMax(iter) + ";";
                    }

                    var minString:String = "";
                    for (iter in 0..(ITERS-1)){
                        minString += dstMin(iter) + ";";
                    }

                    Console.OUT.printf("allreduce %d: %g ms (1 allreduce) \n", s, ((stop-start) as Double) / ITERS);
                    Console.OUT.println("Max:" + maxString);
                    Console.OUT.println("Min:" + minString);
                }
            }
        }

        return true;
        }

        public static def main(var args: Rail[String]): void {
                new BenchmarkAllreduceDetailed().execute();
        }
}

class LocalReduceTime{
    public var allReduceTime:Rail[Long];
    public def this(size:Long){
        allReduceTime = new Rail[Long](size);
    }
}
