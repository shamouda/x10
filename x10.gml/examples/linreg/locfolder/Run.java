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
import x10.xrx.Runtime;

import x10.matrix.DenseMatrix;
import x10.matrix.Vector;
import x10.matrix.ElemType;

import x10.matrix.block.BlockMatrix;
import x10.matrix.distblock.DistBlockMatrix;
import x10.matrix.distblock.DistVector;
import x10.matrix.regression.RegressionInputData;
import x10.matrix.util.Debug;
import x10.matrix.util.MathTool;
import x10.util.Team;
import x10.util.resilient.iterative.SPMDResilientIterativeExecutor;
import x10.util.resilient.iterative.IterativeExecutor;
import x10.util.resilient.iterative.SPMDAgreeResilientIterativeExecutor;

/**
 * Test harness for Linear Regression using GML
 */
public class RunLinReg {

    public static def main(args:Rail[String]): void {
        val opts = new OptionsParser(args, [
            Option("h","help","this information"),
            Option("v","verify","verify the parallel result against sequential computation"),
            Option("p","print","print matrix V, vectors d and w on completion")
        ], [
			Option("f","featuresFile","input features file name"),
			Option("l","labelsFile","input labels file name"),
			Option("z","regularization","regularization parameter (lambda = 1/C); intercept is not regularized"),
            Option("m","rows","number of rows, default = 10"),
            Option("n","cols","number of columns, default = 10"),
            Option("r","rowBlocks","number of row blocks, default = X10_NPLACES"),
            Option("c","colBlocks","number of columnn blocks; default = 1"),
            Option("d","density","nonzero density, default = 0.9"),
            Option("i","iterations","number of iterations, default = 0 (no max)"),
            Option("t","tolerance","convergence tolerance, default = 0.000001"),
            Option("s","spare","spare places count (at least one place should remain), default = 0"),
            Option("k", "checkpointFreq","checkpoint iteration frequency")
        ]);

        if (opts.filteredArgs().size!=0) {
            Console.ERR.println("Unexpected arguments: "+opts.filteredArgs());
            Console.ERR.println("Use -h or --help.");
            System.setExitCode(1n);
            return;
        }
        if (opts.wantsUsageOnly("Options:\n")) {
            return;
        }

        val regularization:Float = opts("z", 0.000001f);
        var mX:Long = opts("m", 10);
        var nX:Long = opts("n", 10);
        var nonzeroDensity:Float = opts("d", 0.9f);
        val verify = opts("v");
        val print = opts("p");
        val iterations = opts("i", 0n);
        val tolerance = opts("t", 0.000001f);
        val sparePlaces = opts("s", 0n);
        val checkpointFreq = opts("checkpointFreq", -1n);

        if (nonzeroDensity<0.0f
         || sparePlaces < 0 || sparePlaces >= Place.numPlaces()) {
            Console.OUT.println("Error in settings");
            System.setExitCode(1n);
            return;
        }

        if (sparePlaces > 0)
            Console.OUT.println("Using "+sparePlaces+" spare place(s).");
            
        
        val disableWarmup = System.getenv("DISABLE_TEAM_WARMUP");
        if (disableWarmup == null || disableWarmup.equals("0")) {
            teamWarmup();
        }
        else {
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
        val team = executor.team();
        
        val rowBlocks = opts("r", places.size());
        val colBlocks = opts("c", 1);

        var pRL:LinearRegression = null;
        
        val featuresFile = opts("f", "");
        if (featuresFile.equals("")) {
            pRL = LinearRegression.makeRandom(mX, nX, rowBlocks, colBlocks, iterations, tolerance, nonzeroDensity, regularization, executor);
        } else {
            val labelsFile = opts("l", "");
            if (labelsFile.equals("")) {
                Console.ERR.println("RunLinReg: missing labels file\ntry `RunLinReg -h ' for more information");
                System.setExitCode(1n);
                return;
            }
            pRL = LinearRegression.makeFromFile(featuresFile, labelsFile, rowBlocks, colBlocks, iterations, tolerance, nonzeroDensity, regularization, executor);
        }
        val M = mX;
        val N = nX;
        val parLR = pRL;
        
        val X = pRL.X;
        val y = pRL.y;
        
        var localX:DenseMatrix(M, N) = null;
        var localY:Vector(M) = null;
        if (verify) {
            val bX:BlockMatrix(parLR.X.M, parLR.X.N);
            if (nonzeroDensity < 0.1f) {
                bX = BlockMatrix.makeSparse(parLR.X.getGrid(), nonzeroDensity);
            } else {
                bX = BlockMatrix.makeDense(parLR.X.getGrid());
            }
            localX = DenseMatrix.make(M, N);
            localY = Vector.make(M);

            X.copyTo(bX as BlockMatrix(parLR.X.M, parLR.X.N));
            bX.copyTo(localX);
            y.copyTo(localY as Vector(y.M));
        }

        Debug.flushln("Starting parallel linear regression");
        parLR.run(startTime);
        //val totalTime = Timer.milliTime() - startTime;
		//Console.OUT.printf("Parallel linear regression --- Total: %8d ms, parallel: %8d ms, sequential: %8d ms, communication: %8d ms\n",
		//		totalTime, parLR.parCompT, parLR.seqCompT, parLR.commT);
        //parLR.printTimes();

        if (print) {
            Console.OUT.println("Input sparse matrix X\n" + X);
            Console.OUT.println("Input dense matrix y\n" + y);
            Console.OUT.println("Output estimated weights: \n" + parLR.getResult());
        }

        if (verify) {
            // Create sequential version running on dense matrices
            val seqLR = new SeqLinearRegression(localX, localY, iterations, tolerance);

            Debug.flushln("Starting sequential linear regression");
            seqLR.run();
            Debug.flushln("Verifying results against sequential version");
            
            if (equalsRespectNaN(parLR.getResult(), seqLR.w as Vector(parLR.getResult().M))) {
                Console.OUT.println("Verification passed.");
            } else {
                Console.OUT.println("Verification failed!");
            }
        }
    }

    /*
     * Vector.equals(Vector) modified to allow NaN.
     */
    public static def equalsRespectNaN(w:Vector, v:Vector):Boolean {
        val M = w.M;
        if (M != v.M) return false;
        for (var c:Long=0; c< M; c++)
            if (MathTool.isZero(w.d(c) - v.d(c)) == false && !(w.d(c).isNaN() && v.d(c).isNaN())) {
                Console.OUT.println("Diff found [" + c + "] : "+
                                    w.d(c) + " <> "+ v.d(c));
                return false;
            }
        return true;
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
