/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2011-2016.
 *  (C) Copyright Sara Salem Hamouda 2014.
 */

import x10.matrix.Vector;
import x10.matrix.ElemType;
import x10.regionarray.Dist;
import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.matrix.distblock.DistBlockMatrix;
import x10.matrix.distblock.DupVector;
import x10.matrix.distblock.DistVector;
import x10.matrix.util.Debug;
import x10.util.Team;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.Snapshottable;
import x10.util.resilient.iterative.*;
import x10.matrix.regression.RegressionInputData;

/**
 * Parallel linear regression using a conjugate gradient solver
 * over distributed dense/sparse matrix
 * @see Elgohary et al. (2016). "Compressed linear algebra for large-scale
 *      machine learning". http://dx.doi.org/10.14778/2994509.2994515
 */
public class LinearRegression implements SPMDResilientIterativeApp {
	static val VERBOSE = System.getenv("LINREG_DEBUG") != null && System.getenv("LINREG_DEBUG").equals("1");
	static val CHECKPOINT_INPUT_MATRIX = System.getenv("CHECKPOINT_INPUT_MATRIX") != null && System.getenv("CHECKPOINT_INPUT_MATRIX").equals("1");
	
    public static val MAX_SPARSE_DENSITY = 0.1f;
    public val lambda:Float; // regularization parameter
    public val tolerance:Float;
    
    /** Matrix of training examples */
    public val X:DistBlockMatrix;
    /** Vector of training regression targets */
    public val y:DistVector(X.M);
    /** Learned model weight vector, used for future predictions */    
    public val d_w:DupVector(X.N);
    
    public val maxIterations:Long;
    
    val d_p:DupVector(X.N);
    val Xp:DistVector(X.M);
    
    val d_r:DupVector(X.N);
    val d_q:DupVector(X.N);
    
    var lastCheckpointNorm:ElemType;
    
    //----Profiling-----
    public var parCompT:Long=0;
    public var seqCompT:Long=0;
    public var commT:Long;
    private val nzd:Float;
    private val root:Place;

    private val executor:IterativeExecutor;
    private var plh:PlaceLocalHandle[AppTempData];
    var team:Team;
    var places:PlaceGroup;
    
    //used for initilizing the matrix from input files
    var featuresFile:String;
    var labelsFile:String;
    
    public def this(X:DistBlockMatrix, y:DistVector(X.M), it:Long, tolerance:Float, 
            sparseDensity:Float, regularization:Float,
            featuresFile:String, labelsFile:String,
            executor:IterativeExecutor) {
        this.X = X;
        this.y = y;
        if (it > 0) {
            this.maxIterations = it;
        } else {
            this.maxIterations = X.N; // number of features
        }
        this.tolerance = tolerance;
        this.lambda = regularization;
        this.featuresFile = featuresFile;
        this.labelsFile = labelsFile;
        this.nzd = sparseDensity;
        this.executor = executor;
        this.places = executor.activePlaces();
        this.team = executor.team();
        
        Xp = DistVector.make(X.M, X.getAggRowBs(), places, team);
        
        d_r  = DupVector.make(X.N, places, team);
        d_p= DupVector.make(X.N, places, team);
        
        d_q= DupVector.make(X.N, places, team);
        
        d_w = DupVector.make(X.N, places, team);  

        root = here;
    }
    
    private static def initRandom(X:DistBlockMatrix, y:DistVector(X.M), places:PlaceGroup) {
        val start = Timer.milliTime();
        finish for (place in places) at(place) async {
            x10.matrix.util.RandTool.reSeed(places.indexOf(here.id()));
            X.initRandom_local();
            y.initRandom_local();
        }
        Console.OUT.println("LinearRegression.initRandom() completed in "+(Timer.milliTime()-start)+" ms");
    }
    
    public static def makeRandom(mX:Long, nX:Long, rowBlocks:Long, colBlocks:Long, iterations:Long, tolerance:Float, 
            nonzeroDensity:Float, regularization:Float, executor:IterativeExecutor) {
        val places = executor.activePlaces();
        val team = executor.team();
        
        val X:DistBlockMatrix;
        val y:DistVector(X.M);

        Console.OUT.printf("Linear regression with random examples X(%d,%d) blocks(%dx%d) ", mX, nX, rowBlocks, colBlocks);
        Console.OUT.printf("dist(%dx%d) nonzeroDensity:%g\n", places.size(), 1, nonzeroDensity);

        if (nonzeroDensity < LinearRegression.MAX_SPARSE_DENSITY) {
            X = DistBlockMatrix.makeSparse(mX, nX, rowBlocks, colBlocks, places.size(), 1, nonzeroDensity, places, team);
        } else {
            Console.OUT.println("Using dense matrix as non-zero density = " + nonzeroDensity);
            X = DistBlockMatrix.makeDense(mX, nX, rowBlocks, colBlocks, places.size(), 1, places, team);
        }
        y = DistVector.make(X.M, places, team);
        initRandom(X, y, places);
        return new LinearRegression(X, y, iterations, tolerance, nonzeroDensity, regularization, null, null, executor);
    }
    
    private static def initFromFile(X:DistBlockMatrix, y:DistVector(X.M), inData:RegressionInputData, 
            featuresFile:String, labelsFile:String, places:PlaceGroup) {
        val start = Timer.milliTime();
        var inD:RegressionInputData;
        if (inData == null) {
            val addBias = true;
            val trainingFraction = 1.0;
            inD = RegressionInputData.readFromSystemMLFile(featuresFile, labelsFile, places, trainingFraction, addBias);
        }
        else
            inD = inData;
        
        val inputData = inD;
        // initialize labels, examples at each place
        finish for (place in places) at(place) async {
            val trainingLabels = inputData.local().trainingLabels;
            val trainingExamples = inputData.local().trainingExamples;
            val startRow = X.getGrid().startRow(places.indexOf(place));
            val blks = X.handleBS();
            val blkitr = blks.iterator();
            while (blkitr.hasNext()) {
                val blk = blkitr.next();              
                blk.init((i:Long, j:Long)=> trainingExamples((i-startRow)*X.N+j));
            }
            y.init_local((i:Long)=> trainingLabels(i));
        }
        Console.OUT.println("LinearRegression.initFromFile() completed in "+(Timer.milliTime()-start)+" ms");
    }
    
    // TODO allow sparse input
    public static def makeFromFile(featuresFile:String, labelsFile:String,
            rowBlocks:Long, colBlocks:Long, iterations:Long, tolerance:Float, 
            nonzeroDensity:Float, regularization:Float, executor:IterativeExecutor) {
        val places = executor.activePlaces();
        val team = executor.team();
        
        val X:DistBlockMatrix;
        val y:DistVector(X.M);
        
        val addBias = true;
        val trainingFraction = 1.0;
        val inputData = RegressionInputData.readFromSystemMLFile(featuresFile, labelsFile, places, trainingFraction, addBias);
        val mX = inputData.numTraining;
        val nX = inputData.numFeatures+1; // including bias
        
        X = DistBlockMatrix.makeDense(mX, nX, rowBlocks, colBlocks, places.size(), 1, places, team);
        y = DistVector.make(X.M, places, team);
        
        initFromFile(X, y, inputData, featuresFile, labelsFile, places);
        
        return new LinearRegression(X, y, iterations, tolerance, nonzeroDensity, regularization, featuresFile, labelsFile, executor);
    }
    
    /*public def isFinished_local() {
        return plh().iter >= maxIterations
            || plh().norm_r2 <= plh().norm_r2_target;
    }*/
    
    //for performance benchmarking with fixed number of iterations
    public def isFinished_local() {
        return plh().iter >= maxIterations;
    }
    
    //startTime parameter added to account for the time taken by RunLinReg to initialize the input data
    public def run(startTime:Long) {
        val start = (startTime != 0)?startTime:Timer.milliTime();  
        assert (X.isDistVertical()) : "dist block matrix must have vertical distribution";
        plh = PlaceLocalHandle.make[AppTempData](places, ()=>new AppTempData());
        
        init();
        
        executor.run(this, start);
        
        return d_w.local();
    }
    
    public def init() {
        finish ateach(Dist.makeUnique(places)) {
             team.barrier();
            // 4: r = -(t(X) %*% y);
            d_r.mult_local(root, y, X);
            d_r.scale_local(-1.0 as ElemType);
            
            val r = d_r.local(); 
        
            // 5: norm_r2 = sum(r * r); p = -r;
            r.copyTo(d_p.local());
            d_p.scale_local(-1.0 as ElemType);
            val norm_r2_initial = r.dot(r);

            plh().norm_r2 = norm_r2_initial;
            plh().norm_r2_initial = norm_r2_initial;
            plh().norm_r2_target = norm_r2_initial * tolerance * tolerance;

            if (root == here) {
                Console.OUT.println("||r|| initial value = " + Math.sqrt(norm_r2_initial)
                 + ",  target value = " + Math.sqrt(plh().norm_r2_target));
            }
        }
    }
    
    public def getResult() = d_w.local();
    
    public def step_local() {
        // compute conjugate gradient
        // 9: q = ((t(X) %*% (X %*% p)) + lambda * p);

        //////Global view step:  d_q.mult(Xp.mult(X, d_p), X);
        Xp.mult_local(X, d_p);
        d_q.mult_local(root, Xp, X);
        
        // Replicated Computation at each place
        var ct:Long = Timer.milliTime();
        val p = d_p.local();
        val q = d_q.local();
        val r = d_r.local(); 
        q.scaleAdd(lambda, p);
        q(q.M-1) -= lambda * p(q.M-1); // don't regularize intercept!

        // 11: alpha = norm_r2 / sum(p * q);
        val alpha = plh().norm_r2 / p.dotProd(q);

        // update model and residuals
        // 13: w = w + alpha * p;
        d_w.local().scaleAdd(alpha, p);
            
        // 14: r = r + alpha * q;
        r.scaleAdd(alpha, q);

        // 15: old_norm_r2 = norm_r2;
        val old_norm_r2 = plh().norm_r2;

        // 16: norm_r2 = sum(r^2);
        plh().norm_r2 = r.dot(r);

        // 17: p = -r + norm_r2/old_norm_r2 * p;
        p.scale(plh().norm_r2/old_norm_r2).cellSub(r);

        if (root == here) {
            Console.OUT.println("Iteration " + plh().iter
             + ":  ||r|| / ||r init|| = "
                 + Math.sqrt(plh().norm_r2 / plh().norm_r2_initial));
        }
       
        plh().iter++;        
    }
   
    public def getCheckpointData_local():HashMap[String,Cloneable] {
    	val map = new HashMap[String,Cloneable]();
    	if (CHECKPOINT_INPUT_MATRIX && plh().iter == 0) {
    		map.put("X", X.makeSnapshot_local());
    	}
    	map.put("d_p", d_p.makeSnapshot_local());
    	map.put("d_q", d_q.makeSnapshot_local());
    	map.put("d_r", d_r.makeSnapshot_local());
    	map.put("d_w", d_w.makeSnapshot_local());
    	map.put("app", plh().makeSnapshot_local());
    	if (VERBOSE) Console.OUT.println(here + "Checkpointing at iter ["+plh().iter+"] norm["+plh().norm_r2+"] ...");
    	return map;
    }
    
    public def restore_local(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) {
    	if (CHECKPOINT_INPUT_MATRIX) {
    		X.restoreSnapshot_local(restoreDataMap.getOrThrow("X"));
    	}
    	d_p.restoreSnapshot_local(restoreDataMap.getOrThrow("d_p"));
        d_q.restoreSnapshot_local(restoreDataMap.getOrThrow("d_q"));
        d_r.restoreSnapshot_local(restoreDataMap.getOrThrow("d_r"));
        d_w.restoreSnapshot_local(restoreDataMap.getOrThrow("d_w"));
        plh().restoreSnapshot_local(restoreDataMap.getOrThrow("app"));        
        if (VERBOSE) Console.OUT.println(here + "Restore succeeded. Restarting from iteration["+plh().iter+"] norm["+plh().norm_r2+"] ...");
    }
    
    public def remake(changes:ChangeDescription, newTeam:Team) {
        this.team = newTeam;
        this.places = changes.newActivePlaces;
        val newRowPs = changes.newActivePlaces.size();
        val newColPs = 1;
        //remake all the distributed data structures
        X.remake(newRowPs, newColPs, changes.newActivePlaces, newTeam, changes.addedPlaces);
        d_p.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        d_q.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        d_r.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        d_w.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        Xp.remake(X.getAggRowBs(), changes.newActivePlaces, newTeam, changes.addedPlaces);
        y.remake(X.getAggRowBs(), changes.newActivePlaces, newTeam, changes.addedPlaces);
        for (sparePlace in changes.addedPlaces){
    		if (VERBOSE) Console.OUT.println("Adding place["+sparePlace+"] to plh ...");
    		PlaceLocalHandle.addPlace[AppTempData](plh, sparePlace, ()=>new AppTempData());
    	}
        if (!CHECKPOINT_INPUT_MATRIX) {
	        if (featuresFile == null){
	            if (this.nzd < LinearRegression.MAX_SPARSE_DENSITY) {
	                X.allocSparseBlocks(this.nzd, changes.addedPlaces);
	            } else {
	                X.allocDenseBlocks(changes.addedPlaces);
	            }
	            initRandom(X, y, places);
	        } else {
	            initFromFile(X, y, null, featuresFile, labelsFile, places);
	        }
        }
    }
    
    class AppTempData implements Cloneable, Snapshottable {
        public var norm_r2:ElemType = 1.0 as ElemType;
        public var norm_r2_initial:ElemType;
        public var norm_r2_target:ElemType = 0.0 as ElemType;
        public var iter:Long;
        
        public def this() { }
        
        def this(norm_r2:ElemType, norm_r2_initial:ElemType, norm_r2_target:ElemType, iter:Long) {
    	    this.norm_r2 = norm_r2;
    	    this.norm_r2_initial = norm_r2_initial;
    	    this.norm_r2_target = norm_r2_target;
    	    this.iter = iter;
        }
        
        public def clone():Cloneable {
        	return new AppTempData(norm_r2, norm_r2_initial, norm_r2_target, iter);
        }
        
        public def makeSnapshot_local() = this;
        
        public def restoreSnapshot_local(o:Cloneable) {
        	val other = o as AppTempData;
        	this.norm_r2 = other.norm_r2;
    	    this.norm_r2_initial = other.norm_r2_initial;
    	    this.norm_r2_target = other.norm_r2_target;
    	    this.iter = other.iter;
        }
    }
}
