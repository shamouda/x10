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
import x10.util.resilient.iterative.PlaceGroupBuilder;
import x10.util.resilient.localstore.SPMDResilientIterativeExecutorULFM;
import x10.util.resilient.localstore.SPMDResilientIterativeExecutor;
import x10.util.resilient.localstore.SPMDResilientIterativeApp;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.resilient.localstore.Cloneable;

import x10.util.Team;

/**
 * Parallel linear regression using a conjugate gradient solver
 * over distributed dense/sparse matrix
 * @see Elgohary et al. (2016). "Compressed linear algebra for large-scale
 *      machine learning". http://dx.doi.org/10.14778/2994509.2994515
 */
public class LinearRegression implements SPMDResilientIterativeApp {
	static val DISABLE_ULFM_AGREEMENT = System.getenv("DISABLE_ULFM_AGREEMENT") != null && System.getenv("DISABLE_ULFM_AGREEMENT").equals("1");
	
    public static val MAX_SPARSE_DENSITY = 0.1f;
    public val lambda:Float; // regularization parameter
    public val tolerance:Float = 0.000001f;
    
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
    
    private val checkpointFreq:Long;
    var lastCheckpointNorm:ElemType;
    
    //----Profiling-----
    public var parCompT:Long=0;
    public var seqCompT:Long=0;
    public var commT:Long;
    private val nzd:Float;
    private val root:Place;

    private val resilientStore:ResilientStore;
    private var appTempDataPLH:PlaceLocalHandle[AppTempData];
    var team:Team;
    
    public def this(X:DistBlockMatrix, y:DistVector(X.M), it:Long, chkpntIter:Long, sparseDensity:Float, regularization:Float, places:PlaceGroup, team:Team, resilientStore:ResilientStore) {
        if (it > 0) {
            this.maxIterations = it;
        } else {
            this.maxIterations = X.N; // number of features
        }
        this.X = X;
        this.y = y;
        this.lambda = regularization;
        
        Xp = DistVector.make(X.M, X.getAggRowBs(), places, team);
        
        d_r  = DupVector.make(X.N, places, team);
        d_p= DupVector.make(X.N, places, team);
        
        d_q= DupVector.make(X.N, places, team);
        
        d_w = DupVector.make(X.N, places, team);  

        this.checkpointFreq = chkpntIter;
        
        nzd = sparseDensity;
        root = here;
        this.team = team;
        this.resilientStore = resilientStore;
    }
    
    public def isFinished() {
        return appTempDataPLH().iter >= maxIterations
            || appTempDataPLH().norm_r2 <= appTempDataPLH().norm_r2_target;
    }
    
    //startTime parameter added to account for the time taken by RunLinReg to initialize the input data
    public def run(startTime:Long) {
        val start = (startTime != 0)?startTime:Timer.milliTime();  
        assert (X.isDistVertical()) : "dist block matrix must have vertical distribution";
        val places = X.places();
        appTempDataPLH = PlaceLocalHandle.make[AppTempData](places, ()=>new AppTempData());
        
        init();
        
        
        if (x10.xrx.Runtime.x10rtAgreementSupport() && !DISABLE_ULFM_AGREEMENT){
            new SPMDResilientIterativeExecutorULFM(checkpointFreq, resilientStore, true).run(this, start);
        }
        else {
            new SPMDResilientIterativeExecutor(checkpointFreq, resilientStore, true).run(this, start);
        }
        
        return d_w.local();
    }
    
    public def init() {
        val places = X.places();
        finish ateach(Dist.makeUnique(places)) {
             team.barrier();
            // 4: r = -(t(X) %*% y);
            d_r.mult_local(root, y, X);
            d_r.scale_local(-1.0 as ElemType);
appTempDataPLH().globalCompTime += Timer.milliTime();
            
appTempDataPLH().localCompTime -= Timer.milliTime();
            val r = d_r.local(); 
        
            // 5: norm_r2 = sum(r * r); p = -r;
            r.copyTo(d_p.local());
            d_p.scale_local(-1.0 as ElemType);
            val norm_r2_initial = r.dot(r);

            appTempDataPLH().norm_r2 = norm_r2_initial;
            appTempDataPLH().norm_r2_initial = norm_r2_initial;
            appTempDataPLH().norm_r2_target = norm_r2_initial * tolerance * tolerance;

            if (root == here) {
                Console.OUT.println("||r|| initial value = " + Math.sqrt(norm_r2_initial)
                 + ",  target value = " + Math.sqrt(appTempDataPLH().norm_r2_target));
            }
        }
    }
    
    public def getResult() = d_w.local();
    
    public def step() {
        // compute conjugate gradient
        // 9: q = ((t(X) %*% (X %*% p)) + lambda * p);

        //////Global view step:  d_q.mult(Xp.mult(X, d_p), X);
appTempDataPLH().globalCompTime -= Timer.milliTime();
        Xp.mult_local(X, d_p);
        d_q.mult_local(root, Xp, X);
appTempDataPLH().globalCompTime += Timer.milliTime();
        
        // Replicated Computation at each place
appTempDataPLH().localCompTime -= Timer.milliTime();            
        var ct:Long = Timer.milliTime();
        val p = d_p.local();
        val q = d_q.local();
        val r = d_r.local(); 
        q.scaleAdd(lambda, p);
        q(q.M-1) -= lambda * p(q.M-1); // don't regularize intercept!

        // 11: alpha = norm_r2 / sum(p * q);
        val alpha = appTempDataPLH().norm_r2 / p.dotProd(q);

        // update model and residuals
        // 13: w = w + alpha * p;
        d_w.local().scaleAdd(alpha, p);
            
        // 14: r = r + alpha * q;
        r.scaleAdd(alpha, q);

        // 15: old_norm_r2 = norm_r2;
        val old_norm_r2 = appTempDataPLH().norm_r2;

        // 16: norm_r2 = sum(r^2);
        appTempDataPLH().norm_r2 = r.dot(r);

        // 17: p = -r + norm_r2/old_norm_r2 * p;
        p.scale(appTempDataPLH().norm_r2/old_norm_r2).cellSub(r);

        if (root == here) {
            Console.OUT.println("Iteration " + appTempDataPLH().iter
             + ":  ||r|| / ||r init|| = "
                 + Math.sqrt(appTempDataPLH().norm_r2 / appTempDataPLH().norm_r2_initial));
        }
       
        appTempDataPLH().iter++;        
appTempDataPLH().localCompTime += Timer.milliTime();
    }
    
   
    public def getCheckpointData():HashMap[String,Cloneable] {
    	Console.OUT.println(here + " ## start check point " );
    	val map = new HashMap[String,Cloneable]();
    	if (appTempDataPLH().iter == 0) {    		
    		map.put("X", X.getCheckpoint_local());
    		Console.OUT.println(here + " -- start check point X done" );
    	}
    	map.put("d_p", d_p.getCheckpoint_local());
    	Console.OUT.println(here + " -- start check point d_p done" );
    	map.put("d_q", d_q.getCheckpoint_local());
    	Console.OUT.println(here + " -- start check point d_q done" );
    	map.put("d_r", d_r.getCheckpoint_local());
    	Console.OUT.println(here + " -- start check point d_r done" );
    	map.put("d_w", d_w.getCheckpoint_local());
    	Console.OUT.println(here + " -- start check point d_w done" );
    	appTempDataPLH().norm_r2_ckpt = appTempDataPLH().norm_r2;
    	appTempDataPLH().iter_ckpt = appTempDataPLH().iter;
    	Console.OUT.println(here + " ## end check point " );
    	return map;
    }
    
    public def remake(newPlaces:PlaceGroup, newTeam:Team, newAddedPlaces:ArrayList[Place]) {    	
        this.team = newTeam;
        val newRowPs = newPlaces.size();
        val newColPs = 1;
        //remake all the distributed data structures
        if (nzd < MAX_SPARSE_DENSITY) {
            X.remakeSparse(newRowPs, newColPs, nzd, newPlaces, newAddedPlaces);
        } else {
            X.remakeDense(newRowPs, newColPs, newPlaces, newAddedPlaces);
        }
        d_p.remake(newPlaces, newTeam, newAddedPlaces);
        d_q.remake(newPlaces, newTeam, newAddedPlaces);
        d_r.remake(newPlaces, newTeam, newAddedPlaces);
        d_w.remake(newPlaces, newTeam, newAddedPlaces);
        Xp.remake(X.getAggRowBs(), newPlaces, newTeam, newAddedPlaces);
        
        
        val p0_norm_init = appTempDataPLH().norm_r2_initial;
        val p0_norm_target = appTempDataPLH().norm_r2_target;
        val p0_norm_ckpt = appTempDataPLH().norm_r2_ckpt;
        val p0_iter_ckpt = appTempDataPLH().iter_ckpt;        
    	for (sparePlace in newAddedPlaces){
    		Console.OUT.println("Adding place["+sparePlace+"] to appTempDataPLH ...");
    		PlaceLocalHandle.addPlace[AppTempData](appTempDataPLH, sparePlace, ()=>new AppTempData(p0_norm_ckpt, p0_norm_init, p0_norm_target, p0_iter_ckpt));
    	}
    }
   
    /**
     * Restore from the snapshot with new PlaceGroup
     */
    public def restore(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) {
    	X.restore_local(restoreDataMap.getOrThrow("X"));
    	d_p.restore_local(restoreDataMap.getOrThrow("d_p"));
        d_q.restore_local(restoreDataMap.getOrThrow("d_q"));
        d_r.restore_local(restoreDataMap.getOrThrow("d_r"));
        d_w.restore_local(restoreDataMap.getOrThrow("d_w"));
        
        appTempDataPLH().norm_r2 = appTempDataPLH().norm_r2_ckpt;
    	appTempDataPLH().iter = appTempDataPLH().iter_ckpt;
    	
        Console.OUT.println("Restore succeeded. Restarting from iteration["+appTempDataPLH().iter+"] norm["+appTempDataPLH().norm_r2+"] ...");
    }    
    
    class AppTempData {
        public var norm_r2:ElemType = 1.0 as ElemType;
        public var norm_r2_initial:ElemType;
        public var norm_r2_target:ElemType = 0.0 as ElemType;
        public var iter:Long;
        
        public var norm_r2_ckpt:ElemType;
        public var iter_ckpt:Long;
    
        public var localCompTime:Long;
        public var globalCompTime:Long;
    
                
        public def this() {
        	
        }
        
        def this(norm_r2:ElemType, norm_r2_initial:ElemType, norm_r2_target:ElemType, iter:Long) {
    	    this.norm_r2 = norm_r2;
    	    this.norm_r2_initial = norm_r2_initial;
    	    this.norm_r2_target = norm_r2_target;
    	    this.iter = iter;
        }
    }
}
