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
import x10.matrix.ElemType;
import x10.matrix.Vector;
import x10.matrix.ElemType;
import x10.regionarray.Dist;

import x10.util.ArrayList;

import x10.matrix.distblock.DistVector;
import x10.matrix.distblock.DupVector;
import x10.matrix.distblock.DistBlockMatrix;

import x10.util.Timer;

import x10.matrix.util.Debug;
import x10.util.HashMap;
import x10.util.Team;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.Snapshottable;
import x10.util.resilient.iterative.*;
import x10.matrix.regression.RegressionInputData;

/*
 * Multinomial Logistic Regression 
 * Based on the implementation provided by SystemML version 0.10.0
 *  
 * https://apache.github.io/incubator-systemml/algorithms-classification.html
 * https://github.com/apache/incubator-systemml/blob/master/scripts/algorithms/MultiLogReg.dml
 * */
public class LogisticRegression(N:Long /*nrow (X)*/, D:Long /*ncol (X)*/) implements SPMDResilientIterativeApp {
    static val CHECKPOINT_INPUT_MATRIX = System.getenv("CHECKPOINT_INPUT_MATRIX") != null && System.getenv("CHECKPOINT_INPUT_MATRIX").equals("1");
    
    
    static val MAX_SPARSE_DENSITY = 0.1f;
    private val C = 2;
    private val tolerance = 0.000001f;
    private val eta0 = 0.0001f;
    private val eta1 = 0.25f;
    private val eta2 = 0.75f;
    private val sigma1 = 0.25f;
    private val sigma2 = 0.5f;
    private val sigma3 = 4.0f;
    private val psi = 0.1f;
    
    private val regularization:Float;
    private val maxiter:Long;
    private val maxinneriter:Long;    
    private val nzd:Float;
    private val root:Place;
    private val bias:Boolean;
    
    private var plh:PlaceLocalHandle[AppTempData];
    private val executor:IterativeExecutor;
    private var team:Team;
    private var places:PlaceGroup;
    
    /** Matrix of training examples */
    public val X:DistBlockMatrix;
    /** Vector of training regression targets */
    public val y:DistVector(N);
    /** Learned model weight vector, used for future predictions */
    private val B:DupVector(D);
    private val Bnew:DupVector(D);
        
    private val lambda:DupVector(D);
    private val Grad:DupVector(D);
    
    private val P:DistVector(N);
    private val Pnew:DistVector(N);
        
    private val S:DupVector(D);
    private val Snew:DupVector(D);
    
    private val R:DupVector(D);
    private val V:DupVector(D);
    private val Q:DistVector(N);
    private val HV:DupVector(D);
    
    private val tmpDist:DistVector(N);
    private val tmpDup:DupVector(D);
    
    private val LT1:DistVector(N);
    private val LT2:DistVector(N);
    

    //used for initilizing the matrix from input files
    var featuresFile:String;
    var labelsFile:String;
    
    public def this(N:Long, D:Long, x_:DistBlockMatrix, y:DistVector, it:Int, nit:Int, nzd:Float, reg:Float, bias:Boolean, 
                    featuresFile:String, labelsFile:String,
                    executor:IterativeExecutor) {
        property(N, D);
        
        this.nzd = nzd;
        this.maxiter = it;
        this.maxinneriter = (nit == 0n) ? D : nit as Long; //[SystemML] if (maxinneriter == 0) maxinneriter = D * K; 
        this.regularization = reg;
        this.root = here;
        this.bias = bias;
        this.featuresFile = featuresFile;
        this.labelsFile = labelsFile;
        this.executor = executor;
        this.places = executor.activePlaces();
        this.team = executor.team();
        
        this.X=x_; //readonly
        this.y = y as DistVector(N); //readonly
        lambda = DupVector.make(D, places, team); //readonly
        B = DupVector.make(D, places, team); //checkpoint
        
        val rowBs = X.getAggRowBs();
        P = DistVector.make(N, rowBs, places, team); //checkpoint
        Grad = DupVector.make(D, places, team); //checkpoint
        
        //temp data ( no need to checkpoint)
        tmpDist = DistVector.make(N, rowBs, places, team);
        tmpDup = DupVector.make(D, places, team);
        S = DupVector.make(D, places, team);
        R = DupVector.make(D, places, team);
        V = DupVector.make(D, places, team);
        Q = DistVector.make(N, rowBs, places, team);
        HV = DupVector.make(D, places, team);
        Snew = DupVector.make(D, places, team);
        Bnew = DupVector.make(D, places, team);
        Pnew = DistVector.make(N, rowBs, places, team);
        //SystemML 2-column table (LT) transformed into 2 vectors (LT1, LT2)
        LT1 = DistVector.make(N, rowBs, places, team);
        LT2 = DistVector.make(N, rowBs, places, team);
    }
    
    
    private static def initRandom(X:DistBlockMatrix, y:DistVector(X.M), places:PlaceGroup) {
        val start = Timer.milliTime();
        finish for (place in places) at(place) async {
            x10.matrix.util.RandTool.reSeed(places.indexOf(here.id()));
            X.initRandom_local(1, 10);
            y.initRandom_local(1, 10);
        }
        Console.OUT.println("LogisticRegression.initRandom() completed in "+(Timer.milliTime()-start)+" ms");
    }
    
    public static def makeRandom(mX:Long, nX:Long, rowBlocks:Long, colBlocks:Long, iterations:Int, innerIterations:Int, 
            nonzeroDensity:Float, regularization:Float, bias:Boolean, executor:IterativeExecutor) {
        val places = executor.activePlaces();
        val team = executor.team();
        
        val X:DistBlockMatrix;
        val y:DistVector(X.M);
        
        Console.OUT.printf("Logistic regression with random examples X(%d,%d) blocks(%dx%d) ", mX, nX, rowBlocks, colBlocks);
        Console.OUT.printf("dist(%dx%d) nonzeroDensity:%g\n", places.size(), 1, nonzeroDensity);

        if (nonzeroDensity < LogisticRegression.MAX_SPARSE_DENSITY) {
            X = DistBlockMatrix.makeSparse(mX, nX, rowBlocks, colBlocks, places.size(), 1, nonzeroDensity, places, team);
        } else {
            Console.OUT.println("Using dense matrix as non-zero density = " + nonzeroDensity);
            X = DistBlockMatrix.makeDense(mX, nX, rowBlocks, colBlocks, places.size(), 1, places, team);
        }
        y = DistVector.make(X.M, places, team);
        initRandom(X, y, places);
        return new LogisticRegression(mX, nX, X, y, iterations, innerIterations, nonzeroDensity, regularization, bias, null, null, executor);
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
            
            /*
             #[SYSTEMML] Convert "Y_vec" into indicator matrice:
             if (min (Y_vec) <= 0) { 
                # Category labels "0", "-1" etc. are converted into the largest label
                max_y = max (Y_vec);
                Y_vec  = Y_vec  + (- Y_vec  + max_y + 1) * ppred (Y_vec , 0.0, "<=");
             }
             **/
            val max_y = y.max_local();
            y.map_local((a:ElemType)=>{ (a <= 0.0)? max_y + 1.0 : a} );
        }
        Console.OUT.println("LogisticRegression.initFromFile() completed in "+(Timer.milliTime()-start)+" ms");
    }
    
 // TODO allow sparse input
    public static def makeFromFile(featuresFile:String, labelsFile:String,
            rowBlocks:Long, colBlocks:Long, iterations:Int, innerIterations:Int, 
            nonzeroDensity:Float, regularization:Float, bias:Boolean, executor:IterativeExecutor) {
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
        return new LogisticRegression(mX, nX, X, y, iterations, innerIterations, nonzeroDensity, regularization, bias, featuresFile, labelsFile, executor);
    }
    
    /*
    public def isFinished_local() {
        return plh().converge;// || plh().iter > maxiter;
    }*/
    
    //for performance evaluation with fixed number of iterations
    public def isFinished_local() {
        return plh().iter > maxiter;
    }
    
    public def train(startTime:Long):Vector(D) {
        val start = (startTime != 0)?startTime:Timer.milliTime();  
        assert (X.isDistVertical()) : "dist block matrix must have vertical distribution";
        plh = PlaceLocalHandle.make[AppTempData](places, ()=>new AppTempData());
        init();
        executor.run(this, start);
        return B.local();
    }
    
    public def init() {
        finish ateach(Dist.makeUnique(places)) {
            // K = 1, Table Y in systemML is composed of two columns, first = 2-y, second = y-1
            
            plh().iter = 1;
            
            //scale_lambda = matrix (1, rows = D, cols = 1); scale_lambda [D, 1] = 0;
            tmpDup.init_local(root, (i:Long)=>{ 
                    if (!bias) return 1.0;
                    else return (i==D-1)? 0.0 : 1.0; 
            });
            
            //rowSums_X_sq = rowSums (X ^ 2);
            X.rowSumTo_local(tmpDist, (a:ElemType)=>{ a * a });
            
            //lambda = (scale_lambda %*% matrix (1, rows = 1, cols = K)) * regularization;
            lambda.scale_local(regularization, tmpDup); 
            
            //delta = 0.5 * sqrt (D) / max (sqrt (rowSums_X_sq));
            plh().delta = 0.5 * Math.sqrt (D) / tmpDist.max_local((a:ElemType)=>{ Math.sqrt(a) });
            
            //B = matrix (0, rows = D, cols = K);
            //B.init_local(root, 0.0);
            
            //P = matrix (1, rows = N, cols = K+1); P = P / (K + 1); 
            P.init_local(0.5);
            
            //obj = N * log (K + 1);
            plh().obj = N * Math.log(C);
            
            // Grad = t(X) %*% (P [, 1:K] - Y [, 1:K]);    //GML note: Y[,1:K] is 2-y
            // Grad = Grad + lambda * B;   (B is Zero, nothing to be done)
            tmpDist.copyFrom_local(P).cellAdd_local(y).cellSub_local(2.0); 
            Grad.mult_local(root, tmpDist, X);                                  
            
            //norm_Grad = sqrt (sum (Grad ^ 2));
            plh().norm_Grad = Math.sqrt ( Grad.sum_local((a:ElemType)=> { a * a }) ); 
            
            //norm_Grad_initial = norm_Grad;
            plh().norm_Grad_initial = plh().norm_Grad;
            
            //converge = (norm_Grad < tol) | (iter > maxiter);
            plh().converge = (plh().norm_Grad < tolerance) | (plh().iter > maxiter) ;
        }
        
        Console.OUT.println ("-- Initially:  Objective = " + plh().obj + ",  Gradient Norm = " + plh().norm_Grad + ",  Trust Delta = " + plh().delta);
    }
    
    public def step_local() {
        // # SOLVE TRUST REGION SUB-PROBLEM  //  
        var alpha:ElemType = 0.0;
    
        // S = matrix (0, rows = D, cols = K);
        S.reset_local();                   
        
        // R = - Grad;
        R.scale_local(-1.0 as ElemType, Grad);    
        
        // V = R;
        V.copyFrom_local(R); 
        
        // delta2 = delta ^ 2;
        val delta2 = plh().delta * plh().delta ;
        
        // inneriter = 1;
        var inneriter:Long = 1;                      
        
        // norm_R2 = sum (R ^ 2);
        var norm_R2:ElemType = R.sum_local( (a:ElemType)=>{ a * a }  );  
        
        // innerconverge = (sqrt (norm_R2) <= psi * norm_Grad);
        var innerconverge:Boolean = (Math.sqrt (norm_R2) <= psi * plh().norm_Grad);   
        
        // is_trust_boundary_reached = 0;
        var is_trust_boundary_reached:Boolean = false;                                
        
        while (! innerconverge){
            // ssX_V = V;
            tmpDup.copyFrom_local(V);
            
            // Q = P [, 1:K] * (X %*% ssX_V);
            tmpDist.mult_local(X, tmpDup);                                        
            Q.copyFrom_local(tmpDist).cellMult_local(P);
            
            // HV = t(X) %*% (Q - P [, 1:K] * (rowSums (Q) %*% matrix (1, rows = 1, cols = K)));
            tmpDist.copyFrom_local(P).cellMult_local(Q);
            Q.cellSub_local(tmpDist);
            HV.mult_local(root, Q, X);                                               
            
            // HV = HV + lambda * V;
            tmpDup.copyFrom_local(V).cellMult_local(lambda);
            HV.cellAdd_local(tmpDup);
            
            // alpha = norm_R2 / sum (V * HV);
            tmpDup.copyFrom_local(V).cellMult_local(HV);
            val VHVsum = tmpDup.sum_local();                        
            alpha = norm_R2 / VHVsum;
            
            //Snew = S + alpha * V;
            tmpDup.copyFrom_local(V).scale_local(alpha);
            Snew.copyFrom_local(S).cellAdd_local(tmpDup);
            
            //norm_Snew2 = sum (Snew ^ 2);
            val norm_Snew2 = Snew.sum_local( (a:ElemType)=>{ a * a }  );
            
            if (norm_Snew2 <= delta2) {
                //S = Snew;
                S.copyFrom_local(Snew);
                
                //R = R - alpha * HV;
                tmpDup.copyFrom_local(HV).scale_local(alpha);
                R.cellSub_local(tmpDup);
                
                //old_norm_R2 = norm_R2 
                val old_norm_R2 = norm_R2;
                        
                //norm_R2 = sum (R ^ 2);
                norm_R2 = R.sum_local( (a:ElemType)=>{ a * a }  ); 
                
                //V = R + (norm_R2 / old_norm_R2) * V;
                val beta = norm_R2 / old_norm_R2;
                tmpDup.copyFrom_local(V).scale_local(beta).cellAdd_local(R);
                V.copyFrom_local(tmpDup);
                
                //innerconverge = (sqrt (norm_R2) <= psi * norm_Grad);
                innerconverge = (Math.sqrt (norm_R2) <= psi * plh().norm_Grad);
            } else {
                //is_trust_boundary_reached = 1;
                is_trust_boundary_reached = true;
                
                //sv = sum (S * V);
                tmpDup.copyFrom_local(V).cellMult_local(S);
                val sv = tmpDup.sum_local();  
                
                //v2 = sum (V ^ 2);
                val v2 = V.sum_local( (a:ElemType)=>{ a * a }  );
                
                //s2 = sum (S ^ 2);
                val s2 = S.sum_local( (a:ElemType)=>{ a * a }  );
                
                //rad = sqrt (sv ^ 2 + v2 * (delta2 - s2));
                val rad = Math.sqrt (sv * sv + v2 * (delta2 - s2));
                
                //same if-else from system-ml code
                if (sv >= 0.0) {
                    alpha = (delta2 - s2) / (sv + rad);
                } else {
                    alpha = (rad - sv) / v2;
                }
                
                //S = S + alpha * V;
                tmpDup.copyFrom_local(V).scale_local(alpha);
                S.cellAdd_local(tmpDup);
                
                //R = R - alpha * HV;
                tmpDup.copyFrom_local(HV).scale_local(alpha);
                R.cellSub_local(tmpDup);
                
                innerconverge = true;
            }
            
            inneriter = inneriter + 1;
            innerconverge = innerconverge | (inneriter > maxinneriter);
        }
        // # END TRUST REGION SUB-PROBLEM
        
        
        //# compute rho, update B, obtain delta
        //gs = sum (S * Grad);
        tmpDup.copyFrom_local(S).cellMult_local(Grad);
        val gs = tmpDup.sum_local();
        
        //qk = - 0.5 * (gs - sum (S * R));
        tmpDup.copyFrom_local(S).cellMult_local(R);
        val SRsum = tmpDup.sum_local();
        val qk = - 0.5 * (gs - SRsum);
        
        //B_new = B + S;
        Bnew.copyFrom_local(B).cellAdd_local(S);
        
        //ssX_B_new = B_new;
        //LT = append ((X %*% ssX_B_new), matrix (0, rows = N, cols = 1));
        LT1.mult_local(X, Bnew);
        
        //LT = LT - rowMaxs (LT) %*% matrix (1, rows = 1, cols = K+1);
        LT2.map_local(LT1, (a:ElemType)=>{  a > 0 ? -1 * a  : 0.0 } );
        LT1.map_local( (a:ElemType)=>{ a >= 0 ? 0.0 : a  } );
        
        //sum (Y * LT) 
        tmpDist.copyFrom_local(y).map_local( (a:ElemType)=>{ 2 - a } ); // first column is Y1 = 2-y
        val Y1dot = tmpDist.dot_local(LT1);
        tmpDist.map_local( (a:ElemType)=>{ (a * -1) + 1 } ); // second column is Y2 = y-1 (compute it from first column by (-Y1 +1) 
        val Y2dot = tmpDist.dot_local(LT2);
        val YLTsum = Y1dot + Y2dot;
        
        //exp_LT = exp (LT);
        LT1.map_local( (a:ElemType)=>{ Math.exp(a) } );
        LT2.map_local( (a:ElemType)=>{ Math.exp(a) } );
        
        //P_new  = exp_LT / (rowSums (exp_LT) %*% matrix (1, rows = 1, cols = K+1));
        //rowSums (exp_LT)
        tmpDist.copyFrom_local(LT1).cellAdd_local(LT2);
        
        //sum (log (rowSums (exp_LT)))
        val L1L2ExpLogsum = tmpDist.sum_local( (a:ElemType)=>{ Math.log(a) } ) ;
        
        //P_new  = exp_LT / rowSums (exp_LT)
        Pnew.copyFrom_local(LT1).cellDiv_local(tmpDist);
        
        //obj_new = - sum (Y * LT) + sum (log (rowSums (exp_LT))) + 0.5 * sum (lambda * (B_new ^ 2));
        //lambda * (B_new ^ 2)
        tmpDup.copyFrom_local(Bnew).map_local((a:ElemType)=>{ a * a }).cellMult_local(lambda);
        val lambdaBnew_sum = tmpDup.sum_local();
        
        val obj_new = -1 * YLTsum + L1L2ExpLogsum + 0.5 * lambdaBnew_sum;
         
        //# Consider updating LT in the inner loop
        //# Consider the big "obj" and "obj_new" rounding-off their small difference below:

        //actred = (obj - obj_new);
        val actred = (plh().obj - obj_new);
        
        //rho = actred / qk;
        val rho = actred / qk;
        
        //is_rho_accepted = (rho > eta0);
        val is_rho_accepted = (rho > eta0);
        
        //snorm = sqrt (sum (S ^ 2));
        val snorm = Math.sqrt( S.sum_local( (a:ElemType)=>{ a * a } ) );
        
        if (plh().iter == 1) {
           plh().delta = Math.min (plh().delta, snorm);
        }

        val alpha2 = obj_new - plh().obj - gs;
        if (alpha2 <= 0) {
           alpha = sigma3;
        } 
        else {
           alpha = Math.max (sigma1, -0.5 * gs / alpha2);
        }
        
        if (rho < eta0) {
            plh().delta = Math.min (Math.max (alpha, sigma1) * snorm, sigma2 * plh().delta);
        }
        else {
            if (rho < eta1) {
                plh().delta = Math.max (sigma1 * plh().delta, Math.min (alpha * snorm, sigma2 * plh().delta));
            }
            else { 
                if (rho < eta2) {
                    plh().delta = Math.max (sigma1 * plh().delta, Math.min (alpha * snorm, sigma3 * plh().delta));
                }
                else {
                    plh().delta = Math.max (plh().delta, Math.min (alpha * snorm, sigma3 * plh().delta));
                }
            }
        } 
        
        if (here.id == root.id) {
            if (is_trust_boundary_reached) {
                Console.OUT.println ("-- Outer Iteration " + plh().iter + ": Had " + (inneriter - 1) + " CG iterations, trust bound REACHED");
            } else {
                Console.OUT.println ("-- Outer Iteration " + plh().iter + ": Had " + (inneriter - 1) + " CG iterations");
            }
        
            Console.OUT.println ("   -- Obj.Reduction:  Actual = " + actred + ",  Predicted = " + qk + 
                    "  (A/P: " + (Math.round (10000.0 * rho) / 10000.0) + "),  Trust Delta = " + plh().delta);
        }
        
        if (is_rho_accepted) {
            //B = B_new;
            B.copyFrom_local(Bnew);
            
            //P = P_new;
            P.copyFrom_local(Pnew);
            
            //Grad = t(X) %*% (P [, 1:K] - Y [, 1:K]);
            tmpDist.copyFrom_local(P).cellAdd_local(y).cellSub_local(2.0); 
            Grad.mult_local(root, tmpDist, X); 
            
            //Grad = Grad + lambda * B;
            tmpDup.copyFrom_local(B).cellMult_local(lambda);
            Grad.cellAdd_local(tmpDup);
                         
            //norm_Grad = sqrt (sum (Grad ^ 2));
            plh().norm_Grad = Math.sqrt ( Grad.sum_local( (a:ElemType)=>{ a * a } ) );
            
            //obj = obj_new;
            plh().obj = obj_new;
            if (here.id == 0) 
                Console.OUT.println ("   -- New Objective = " + plh().obj + ",  Beta Change Norm = " + snorm + ",  Gradient Norm = " + plh().norm_Grad);
        }
        
        //iter = iter + 1;
        plh().iter = plh().iter + 1;

        /*converge = ((norm_Grad < (tol * norm_Grad_initial)) | (iter > maxiter) |
                   ((is_trust_boundary_reached == 0) & (Math.abs (actred) < (Math.abs (obj) + Math.abs (obj_new)) * 0.00000000000001)));*/
       
        plh().converge = ((plh().norm_Grad < (tolerance * plh().norm_Grad_initial)) | (plh().iter > maxiter) |
            ((is_trust_boundary_reached == false) & (Math.abs (actred) < (Math.abs (plh().obj) + Math.abs (obj_new)) * 0.00000000000001)));
        
        if (plh().converge) { 
            if (here.id == root.id) 
                Console.OUT.println ("Termination / Convergence condition satisfied."); 
        } else { 
             if (here.id == root.id) 
                 Console.OUT.println (" "); 
        }
        
    }

    
    public def getCheckpointData_local() {
        val map = new HashMap[String,Cloneable]();
        if (plh().iter == 1) {
            if (CHECKPOINT_INPUT_MATRIX){
                map.put("X", X.makeSnapshot_local());
            }
            map.put("y", y.makeSnapshot_local());
            map.put("lambda", lambda.makeSnapshot_local());
        }
        map.put("B", B.makeSnapshot_local());
        map.put("P", P.makeSnapshot_local());
        map.put("Grad", Grad.makeSnapshot_local());
        map.put("app", plh().makeSnapshot_local());
        if (here.id == root.id) 
            Console.OUT.println(here + " Checkpointing at iter ["+plh().iter+"] delta["+plh().delta+"] obj["+plh().obj+"] ...");
        
        return map;
    }
    
    public def restore_local(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) {
    	if (CHECKPOINT_INPUT_MATRIX) {
            X.restoreSnapshot_local(restoreDataMap.getOrThrow("X"));
        }
        y.restoreSnapshot_local(restoreDataMap.getOrThrow("y"));
        lambda.restoreSnapshot_local(restoreDataMap.getOrThrow("lambda"));
        B.restoreSnapshot_local(restoreDataMap.getOrThrow("B"));
        P.restoreSnapshot_local(restoreDataMap.getOrThrow("P"));
        Grad.restoreSnapshot_local(restoreDataMap.getOrThrow("Grad"));
        plh().restoreSnapshot_local(restoreDataMap.getOrThrow("app"));
        if (here.id == root.id) 
            Console.OUT.println(here + " Restore succeeded. Restarting from iter ["+plh().iter+"] delta["+plh().delta+"] obj["+plh().obj+"] ...");
    }
    
    public def remake(changes:ChangeDescription, newTeam:Team) {
        this.team = newTeam;
        this.places = changes.newActivePlaces;
        val newRowPs = changes.newActivePlaces.size();        
        val newColPs = 1;
        //remake all the distributed data structures
        X.remake(newRowPs, newColPs, changes.newActivePlaces, newTeam, changes.addedPlaces);
        
        val rowBs = X.getAggRowBs();
        B.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        tmpDup.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        lambda.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        Grad.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        S.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        R.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        V.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        HV.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        Snew.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        Bnew.remake(changes.newActivePlaces, newTeam, changes.addedPlaces);
        
        y.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces);
        Q.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces); 
        P.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces);
        Pnew.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces); 
        tmpDist.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces); 
        LT1.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces); 
        LT2.remake(rowBs, changes.newActivePlaces, newTeam, changes.addedPlaces); 
        for (sparePlace in changes.addedPlaces){
            PlaceLocalHandle.addPlace[AppTempData](plh, sparePlace, ()=>new AppTempData());
        }
        if (!CHECKPOINT_INPUT_MATRIX) {
	        if (featuresFile == null){
	            if (this.nzd < LogisticRegression.MAX_SPARSE_DENSITY) {
	                X.allocSparseBlocks(this.nzd, changes.addedPlaces);
	            } else {
	                X.allocDenseBlocks(changes.addedPlaces);
	            }
	            initRandom(X, y, places);
	        } else {
	            initFromFile(X, y, null, featuresFile, labelsFile, places);
	        }
        }
        Console.OUT.println(here + " Remake succeeded ...");
    }
    
    class AppTempData implements Cloneable, Snapshottable {
        public var delta:ElemType;
        public var obj:ElemType;
        public var norm_Grad:ElemType;
        public var norm_Grad_initial:ElemType;
        public var norm_R2:ElemType;
        public var iter:Long;        
        public var converge:Boolean;
    
        public def this() { 
            
        }
    
        def this(delta:ElemType, iter:Long, obj:ElemType, norm_Grad:ElemType, 
                norm_Grad_initial:ElemType, norm_R2:ElemType, converge:Boolean) {
            this.delta = delta;
            this.iter = iter;
            this.obj = obj;
            this.norm_Grad = norm_Grad;
            this.norm_Grad_initial = norm_Grad_initial;
            this.norm_R2 = norm_R2;
            this.converge = converge;
        }
    
        public def clone():Cloneable {
            return new AppTempData(delta, iter, obj, norm_Grad, norm_Grad_initial, norm_R2, converge);
        }
    
        public def makeSnapshot_local() = this;
    
        public def restoreSnapshot_local(o:Cloneable) {
            val other = o as AppTempData;
            this.delta = other.delta;
            this.iter = other.iter;
            this.obj = other.obj;
            this.norm_Grad = other.norm_Grad;
            this.norm_Grad_initial = other.norm_Grad_initial;
            this.norm_R2 = other.norm_R2;
            this.converge = other.converge;
        }
    }
}
