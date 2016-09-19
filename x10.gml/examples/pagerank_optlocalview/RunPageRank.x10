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
import x10.util.Team;
import x10.matrix.Vector;
import x10.matrix.util.Debug;
import x10.util.resilient.iterative.PlaceGroupBuilder;
import x10.matrix.util.VerifyTool;

/**
 * Page Rank demo
 * <p>
 * Execution input parameters:
 * <ol>
 * <li>Rows and columns of G. Default 10000</li>
 * <li>Iterations number. Default 20</li>
 * <li>Verification flag. Default 0 or false.</li>
 * <li>Row-wise partition of G. Default number of places</li>
 * <li>Column-wise partition of G. Default 1.</li>
 * <li>Nonzero density of G: Default 0.001f</li>
 * <li>Print output flag: Default false.</li>
 * </ol>
 */
public class RunPageRank {
    public static def main(args:Rail[String]): void {
    	
    	
    	
        val opts = new OptionsParser(args, [
            Option("h","help","this information"),
            Option("v","verify","verify the parallel result against sequential computation"),
            Option("p","print","print matrix V, vectors d and w on completion")
        ], [
            Option("m","rows","number of rows, default"),
            Option("r","rowBlocks","number of row blocks, default = X10_NPLACES"),
            Option("c","colBlocks","number of columnn blocks; default = 1"),
            Option("d","density","nonzero density, default = 0.001"),
            Option("i","iterations","number of iterations, default = 20"),
            Option("s","skip","skip places count (at least one place should remain), default = 0"),
            Option("", "checkpointFreq","checkpoint iteration frequency")
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

        
        val nonzeroDensity = opts("d", 0.001f);
        val iterations = opts("i", 30n);
        val verify = opts("v");
        val print = opts("p");
        val sparePlaces = opts("s", 0n);
        val checkpointFreq = opts("checkpointFreq", -1n);
        val placesCount = Place.numPlaces() - sparePlaces;
        
        val mG = opts("m", (20000*Math.sqrt(placesCount*5)) as Long );
        
        Console.OUT.printf("G: rows/cols %d density: %.3f (non-zeros: %ld) iterations: %d\n",
                            mG, nonzeroDensity, (nonzeroDensity*mG*mG) as Long, iterations);
	if ((mG<=0) || iterations < 1n || nonzeroDensity <= 0.0 || sparePlaces < 0 || sparePlaces >= Place.numPlaces())
            Console.OUT.println("Error in settings");
        else {
            
            val places = (sparePlaces==0n) ? Place.places() 
                                          : PlaceGroupBuilder.execludeSparePlaces(sparePlaces);
            val rowBlocks = opts("r", places.size());
            val colBlocks = opts("c", 1);

            
            val team = new Team(places);
            teamWarmup(team);
            
            val paraPR = PageRankResilient.make(mG, nonzeroDensity, iterations, rowBlocks, colBlocks, checkpointFreq, places, team);
            
            val startTime = Timer.milliTime(); //moved here to be similar to LULESH
            
            paraPR.init(nonzeroDensity);

            if (print) paraPR.printInfo();

            var origP:Vector(mG) = null;
            if (verify) {
                origP = paraPR.P.local().clone();
            }

            val paraP = paraPR.run(startTime);
         
            if (print) {
                Console.OUT.println("Input G sparse matrix\n" + paraPR.G);
                Console.OUT.println("Output vector P\n" + paraP);
            }
            
            if (verify) {
                val g = paraPR.G;
                val localU = Vector.make(g.N);
                
                paraPR.U.copyTo(localU);
                
                
                val seqPR = new SeqPageRank(g.toDense(), origP, 
                        localU, iterations);
		        Debug.flushln("Start sequential PageRank");
                val seqP = seqPR.run();
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
    
    public static def teamWarmup(){
    	val disableWarmup = System.getenv("DISABLE_TEAM_WARMUP");
    	if (disableWarmup != null && disableWarmup.equals("1"))
    	{
    		Console.OUT.println("Team warm up disabled ...");
    		return;
    	}
    		
    	val startWarmupTime = Timer.milliTime();
    	Console.OUT.println("Starting team warm up ...");
    	// warm up comms layer
    	val root = Place(0);
    	
    	
    	finish for (place in Place.places()) at (place) async {
    		Team.WORLD.reduce(root, 1.0, Team.ADD);
    		
    		if (here.id == 0) Console.OUT.println(here+" reduce done ...");
    	
    		Team.WORLD.allreduce(1.0, Team.ADD);
    		if (here.id == 0) Console.OUT.println(here+" allreduce done ...");
    	
    		Team.WORLD.barrier(); 
    		if (here.id == 0) Console.OUT.println(here+" barrier done ...");
    	
    		var scounts:Rail[Int] = new Rail[Int](Place.numPlaces(),1n);
    		val warmupInScatter = new Rail[Double](Place.numPlaces());
    		var warmupOutScatter:Rail[Double] = new Rail[Double](1);
    		Team.WORLD.scatter(root,warmupInScatter, 0, warmupOutScatter, 0, 1);
    		if (here.id == 0) Console.OUT.println(here+" scatter done ...");
        
    		//Team.WORLD.scatterv(root, warmupInScatter, 0, warmupOutScatter, 0, scounts);
    		//if (here.id == 0) Console.OUT.println(here+" scatterv done ...");
    	
    		val warmupInGather = new Rail[Double](1);
    		var warmupOutGather:Rail[Double] = new Rail[Double](Place.numPlaces());
    		Team.WORLD.gather(root,warmupInGather, 0, warmupOutGather, 0, 1);
    		if (here.id == 0) Console.OUT.println(here+" gather done ...");
        
    		Team.WORLD.gatherv(root, warmupInGather, 0, warmupOutGather, 0, scounts);
    		if (here.id == 0) Console.OUT.println(here+" gatherv done ...");
        
    		val warmupBcast = new Rail[Double](1);
    		Team.WORLD.bcast(root, warmupBcast, 0, warmupBcast, 0, 1); 
    		if (here.id == 0) Console.OUT.println(here+" bcast done ...");
    		
    		try{
    			Team.WORLD.agree(1n);
    			if (here.id == 0) Console.OUT.println(here+" agree done ...");
    		}catch(ex:Exception){
    			if (here.id == 0) {
    				Console.OUT.println("agree failed ...");
    				ex.printStackTrace();
    			}
    		}
    		
    	}
        Console.OUT.println("Team warm up succeeded , time elapsed ["+(Timer.milliTime()-startWarmupTime)+"] ...");
    }
    
    public static def teamWarmup(team:Team){
    	val disableWarmup = System.getenv("DISABLE_TEAM_WARMUP");
    	if (disableWarmup != null && disableWarmup.equals("1"))
    	{
    		Console.OUT.println("Team warm up disabled ...");
    		return;
    	}
    		
    	val startWarmupTime = Timer.milliTime();
    	Console.OUT.println("Starting team warm up ...");
    	// warm up comms layer
    	val root = Place(0);
    	
    	
    	finish for (place in Place.places()) at (place) async {
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
        Console.OUT.println("Team warm up succeeded , time elapsed ["+(Timer.milliTime()-startWarmupTime)+"] ...");
    }
}

