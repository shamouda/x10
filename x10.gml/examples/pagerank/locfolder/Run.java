/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2011-2016.
 */

import x10.util.Option;
import x10.util.OptionsParser;
import x10.util.Timer;

import x10.util.resilient.iterative.*;

import x10.matrix.Vector;
import x10.matrix.util.Debug;
import x10.matrix.util.VerifyTool;
import x10.matrix.distblock.DistBlockMatrix;
import x10.util.Team;

/**
 * Page Rank demo
 */
public class RunPageRank {
    public static def main(args:Rail[String]): void {
        val opts = new OptionsParser(args, [
            Option("h","help","this information"),
            Option("v","verify","verify the parallel result against sequential computation"),
            Option("p","print","print matrix V, vectors d and w on completion")
        ], [
            Option("m","rows","number of rows, default = 100000"),
            Option("mepp","millionEdgesPerPlace","number of million edges per place assuming density=0.001, default = 2 (i.e. 2M per place) "),
            Option("r","rowBlocks","number of row blocks, default = X10_NPLACES"),
            Option("c","colBlocks","number of columnn blocks; default = 1"),
            Option("d","density","nonzero density, default = log-normal"),
            Option("i","iterations","number of iterations, default = 0 (run until convergence)"),
            Option("t","tolerance","convergence tolerance, default = 0.0001"),
            Option("s","spare","spare places count (at least one place should remain), default = 0"),
            Option("k", "checkpointFreq","checkpoint iteration frequency")
        ]);

        if (opts.filteredArgs().size!=0) {
            Console.ERR.println("Unexpected arguments: "+opts.filteredArgs());
            Console.ERR.println("Use -h or --help.");
            System.setExitCode(1n);
            return;
        }
        if (opts("h")) {
            Console.OUT.println(opts.usage(""));
            return;
        }

        var nonzeroDensity:Float = opts("d", 0.0f);
        val iterations = opts("i", 0n);
        val tolerance = opts("t", 0.0001f);
        val verify = opts("v");
        val print = opts("p");
        val sparePlaces = opts("s", 0n);
        val checkpointFreq = opts("checkpointFreq", -1n);
        val placesCount = Place.numPlaces() - sparePlaces;
        var mG:Long = -1;
        val millionEdgesPerPlace = opts("mepp", -1.0f);
        if (millionEdgesPerPlace != -1.0f) {
            nonzeroDensity = 0.001f;
            mG = (10000*Math.sqrt(placesCount*10*millionEdgesPerPlace)) as Long;
            Console.OUT.println("Running in weak scaling mode: density["+nonzeroDensity+"] mG["+mG+"] places["+Place.numPlaces()+"] spare["+sparePlaces+"]");
        } else {
            mG = opts("m", 100000);
            Console.OUT.println("Running in normal mode: density["+nonzeroDensity+"] mG["+mG+"] places["+Place.numPlaces()+"] spare["+sparePlaces+"]");
        }
        
        Console.OUT.printf("G: rows/cols %d iterations: %d\n", mG, iterations);
        if ((mG<=0) || sparePlaces < 0 || sparePlaces >= Place.numPlaces())
            Console.OUT.println("Error in settings");
        else {
            val disableWarmup = System.getenv("DISABLE_TEAM_WARMUP");
            if (disableWarmup == null || disableWarmup.equals("0")) {
                teamWarmup();
            } else {
                Console.OUT.println("Starting without warmpup!!");
            }
            
            val disableAgree = System.getenv("DISABLE_TEAM_AGREE") != null && Long.parseLong(System.getenv("DISABLE_TEAM_AGREE")) == 1;
            val startTime = Timer.milliTime();
            val executor:IterativeExecutor;
            if (x10.xrx.Runtime.x10rtAgreementSupport() && !disableAgree)
                executor = new SPMDAgreeResilientIterativeExecutor(checkpointFreq, sparePlaces, false);
            else
                executor = new SPMDResilientIterativeExecutor(checkpointFreq, sparePlaces, false);
            
            val places = executor.activePlaces();
            
            val rowBlocks = opts("r", places.size());
            val colBlocks = opts("c", 1);

            val paraPR:PageRank;
            if (nonzeroDensity > 0.0f) {
                paraPR = PageRank.makeRandom(mG, nonzeroDensity, iterations, tolerance, rowBlocks, colBlocks, executor);
                Console.OUT.printf("random edge graph (uniform distribution) density: %.3e non-zeros: %d\n",
                            nonzeroDensity, (nonzeroDensity*mG*mG) as Long);
            } else {
                paraPR = PageRank.makeLogNormal(mG, iterations, tolerance, rowBlocks, colBlocks, executor);
                Console.OUT.println("log-normal edge graph (mu=4.0, sigma=1.3) total non-zeros: " + paraPR.G.getTotalNonZeroCount());
            }
/*
            // toy example copied from Spark (users/followers)
            val M = 6;
            val G = DistBlockMatrix.makeDense(M, M, Place.numPlaces(), 1);
            G(0,1) = 1.0;
            G(0,3) = 1.0;
            G(1,0) = 1.0;
            G(2,4) = 1.0;
            G(2,5) = 1.0;
            G(4,5) = 1.0;
            G(5,4) = 1.0;
            G(5,2) = 1.0;
            val paraPR = new PageRank(G, iterations, tolerance, 1.0f, 0, Place.places());
            Console.OUT.println("P = " + paraPR.P);
*/

            if (print) paraPR.printInfo();

            val paraP = paraPR.run(startTime);
            
            if (print) {
                Console.OUT.println("Input G sparse matrix\n" + paraPR.G);
                Console.OUT.println("Output vector P\n" + paraP);
            }
            
            if (verify) {
                val g = paraPR.G;
                val localU = Vector.make(g.N);
                
                //paraPR.U.copyTo(localU);
                
                val seqPR = new SeqPageRank(g.toDense(), iterations, tolerance);
                Debug.flushln("Start sequential PageRank");
                val seqP = seqPR.run();
                if (print) {
                    Console.OUT.println("Seq output vector P\n" + seqP);
                }
                Debug.flushln("Verifying results against sequential version");
                val localP = Vector.make(g.N);
                paraP.copyTo(localP);
                if (VerifyTool.testSame(localP, seqP)) 
                    Console.OUT.println("Verification passed.");
                else
                    Console.OUT.println("Verification failed!!!!");
            }
        }
    }
    
    
    
/*    public static def teamWarmup(){
        val places = Place.places();
        val team = new Team(places);
        val startWarmupTime = Timer.milliTime();
        Console.OUT.println("Starting team warm up ...");
        // warm up comms layer
        val root = Place(0);
        
        
        finish for (place in places) at (place) async {
            team.reduce(root, 1.0, Team.ADD);
            if (here.id == 0) Console.OUT.println(here+" reduce done ...");
        
            team.allreduce(1.0, Team.ADD);
            if (here.id == 0) Console.OUT.println(here+" allreduce done ...");
        
            team.barrier(); 
            if (here.id == 0) Console.OUT.println(here+" barrier done ...");
        
            var scounts:Rail[Int] = new Rail[Int](Place.numPlaces(),1n);
            val warmupInScatter = new Rail[Double](Place.numPlaces());
            var warmupOutScatter:Rail[Double] = new Rail[Double](1);
            team.scatter(root,warmupInScatter, 0, warmupOutScatter, 0, 1);
            if (here.id == 0) Console.OUT.println(here+" scatter done ...");
        
            //team.scatterv(root, warmupInScatter, 0, warmupOutScatter, 0, scounts);
            //if (here.id == 0) Console.OUT.println(here+" scatterv done ...");
        
            val warmupInGather = new Rail[Double](1);
            var warmupOutGather:Rail[Double] = new Rail[Double](Place.numPlaces());
            team.gather(root,warmupInGather, 0, warmupOutGather, 0, 1);
            if (here.id == 0) Console.OUT.println(here+" gather done ...");
        
            team.gatherv(root, warmupInGather, 0, warmupOutGather, 0, scounts);
            if (here.id == 0) Console.OUT.println(here+" gatherv done ...");
        
            val warmupBcast = new Rail[Double](1);
            team.bcast(root, warmupBcast, 0, warmupBcast, 0, 1); 
            if (here.id == 0) Console.OUT.println(here+" bcast done ...");
            
            if (x10.xrx.Runtime.x10rtAgreementSupport()) {   
                try{
                    team.agree(1n);
                    if (here.id == 0) Console.OUT.println(here+" agree done ...");
                }catch(ex:Exception){
                    if (here.id == 0) {
                        Console.OUT.println("agree failed ...");
                        ex.printStackTrace();
                    }
                }
            }
        }
        Console.OUT.println("Team warm up succeeded , time elapsed ["+(Timer.milliTime()-startWarmupTime)+"] ...");
    }*/
}
