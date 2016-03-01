/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2011-2014.
 *  (C) Copyright Sara Salem Hamouda 2014.
 */

import x10.util.Timer;

import x10.matrix.util.Debug;
import x10.matrix.Vector;
import x10.matrix.distblock.DistGrid;
import x10.regionarray.Dist;

import x10.matrix.distblock.DistVector;
import x10.matrix.distblock.DupVector;
import x10.matrix.distblock.DistBlockMatrix;
import x10.util.resilient.iterative.LocalViewResilientIterativeAppOpt;
import x10.util.resilient.iterative.LocalViewResilientExecutorOpt;
import x10.util.resilient.iterative.ApplicationSnapshotStore;
import x10.util.Team;
import x10.util.ArrayList;
import x10.util.resilient.iterative.DistObjectSnapshot;
import x10.util.concurrent.AtomicInteger;

/**
 * Parallel Page Rank algorithm based on GML distributed block matrix.
 * <p> Input matrix G is partitioned into (numRowBsG &#42 numColBsG) blocks. All blocks
 * are distributed to (Place.numPlaces(), 1) places, or vertical distribution.
 * <p>[g_(0,0),           g_(0,1),           ..., g(0,numColBsG-1)]
 * <p>[g_(1,0),           g_(1,1),           ..., g(1,numColBsG-1)]
 * <p>......
 * <p>[g_(numRowBsG-1,0), g_(numRowBsG-1,1), ..., g(numRowBsG-1,numColBsG-1)]
 * <p>
 * <p> Vector P is partitioned into (numColBsG &#42 1) blocks, and replicated in all places.
 * <p>[p_(0)]
 * <p>[p_(1)]
 * <p>......
 * <p>[p_(numColBsG-1)]
 */
public class PageRankResilient implements LocalViewResilientIterativeAppOpt {
    public val iterations:Long;
    public val alpha:ElemType= 0.85 as ElemType;

    /** Google Matrix or link structure */
    public val G:DistBlockMatrix{self.M==self.N};
    /** PageRank vector */
    public val P:DupVector(G.N);
    /** Personalization vector */
    public val U:DistVector(G.N);

    /** temp data: G * P */
    val GP:DistVector(G.N);
    
    public var paraRunTime:Long = 0;
    public var commTime:Long;
    var seqTime:Long = 0;

    private val chkpntIterations:Long;
    private val nzd:Float;
    
    private var appTempDataPLH:PlaceLocalHandle[AppTempData];
    private val root:Place;
    private var team:Team;
    
    public def this(
            g:DistBlockMatrix{self.M==self.N}, 
            p:DupVector(g.N), 
            u:DistVector(g.N), 
            it:Long,
            sparseDensity:Float,
            chkpntIter:Long,
            places:PlaceGroup,
            team:Team) {
        Debug.assure(DistGrid.isVertical(g.getGrid(), g.getMap()), 
                "Input block matrix g does not have vertical distribution.");
        G = g;
        P = p as DupVector(G.N);
        U = u as DistVector(G.N);
        iterations = it;
        
        GP = DistVector.make(G.N, G.getAggRowBs(), places, team);//G must have vertical distribution

        chkpntIterations = chkpntIter;
        nzd = sparseDensity;
        root = here;
        this.team = team;
    }

    public static def make(gN:Long, nzd:Float, it:Long, numRowBs:Long, numColBs:Long, chkpntIter:Long, places:PlaceGroup) {
        //---- Distribution---
        val numRowPs = places.size();
        val numColPs = 1;
        val team = new Team(places);
        
        val g = DistBlockMatrix.makeSparse(gN, gN, numRowBs, numColBs, numRowPs, numColPs, nzd, places);
        val p = DupVector.make(gN, places, team);
        val u = DistVector.make(gN, g.getAggRowBs(), places, team);
        return new PageRankResilient(g, p, u, it, nzd, chkpntIter, places, team);
    } 
    
    public def init(nzd:Float):void {
        val places = G.places();
        finish ateach(Dist.makeUnique(places)) {
            G.initRandom_local();            
            // initialize pagerank to personalization vector-- does broadcast internally
            P.initRandom_local(root);
            
            val normalization = 1.0 / (0.5 * nzd * G.N);
            G.scale_local(normalization);

            val sum = P.local().sum();
            P.local().cellDiv(sum);            
            U.copyFrom_local(P.local());
        }
    }

    public def run(startTime:Long):Vector(G.N) {
        val start = (startTime != 0)?startTime:Timer.milliTime();  
        assert (G.isDistVertical()) : "dist block matrix must have vertical distribution";
    
        val places = G.places();
        appTempDataPLH = PlaceLocalHandle.make[AppTempData](places, ()=>new AppTempData());
    
        new LocalViewResilientExecutorOpt(chkpntIterations, places, true).run(this, start);
        
        return P.local();
    }

    public def printInfo() {
        val nzc =  G.getTotalNonZeroCount() ;
        val nzd =  nzc / (G.M * G.N);
        Console.OUT.printf("Input Matrix G:(%dx%d), partition:(%dx%d) blocks, ",
                G.M, G.N, G.getGrid().numRowBlocks, G.getGrid().numColBlocks);
        Console.OUT.printf("distribution:(%dx%d), nonzero density:%f count:%f\n", 
                Place.numPlaces(), 1,  nzd, nzc);

        Console.OUT.printf("Input duplicated vector P(%d), duplicated in all places\n", P.M);

        Console.OUT.printf("Input vector U(%d)\n", U.M);

        Console.OUT.flush();
    }
    
    public def isFinished_local():Boolean {
        return appTempDataPLH().iter >= iterations;
    }

    public def step_local():void {
        GP.mult_local(G, P);
        GP.scale_local(alpha);
    
        val teleport = U.dot_local(P) * (1-alpha);
        
        GP.copyTo(root, P.local());     
        if (here.id == root.id)
            P.local().cellAdd(teleport);

        P.sync_local(root);

        appTempDataPLH().iter++;
    }
    
    public def checkpoint_local(store:DistObjectSnapshot):void {
    	//using finish here causes deadlock
    	//Read only data will be saved only in the first checkpoint
    	val Gstatus = new AtomicInteger(0N);
    	val Ustatus = new AtomicInteger(0N);
    	
    	if (appTempDataPLH().iter == 0) {  
    	    async {
    	    	try{
    	    	    G.makeSnapshot_local("G", store);
    	    	    atomic Gstatus.set(1N);
    	    	}
    	    	catch(ex:Exception){
    	    		ex.printStackTrace();
    	    		atomic Gstatus.set(2N);
    	    	}
    	    }
    	    
    	    async {
    	    	try{
    	    	     U.makeSnapshot_local("U", store);
    	    	     atomic Ustatus.set(1N);
    	    	}catch(ex:Exception){
    	    		ex.printStackTrace();
    	    		atomic Ustatus.set(2N);
    	    	}
    	    }
        } else{
        	Gstatus.set(1N);
        	Ustatus.set(1N);
        }
    	
    	P.makeSnapshot_local("P", store);  
    	
    	Console.OUT.println(here + "Waiting on when >>>>>>>>>>>");
        when(Gstatus.get() > 0N && Ustatus.get() > 0N);
	    
	    if (Gstatus.get() == 2N || Ustatus.get() == 2N)
	    	throw new Exception(here + " Checkpoint failed  Gstatus["+Gstatus.get()+"]  Ustatus["+Ustatus.get()+"] ...");
	    
    }

    public def remake(newGroup:PlaceGroup, newTeam:Team, newAddedPlaces:ArrayList[Place]) {
        val oldPlaces = G.places();
        
        val newRowPs = newGroup.size();
        val newColPs = 1;
        Console.OUT.println("Going to restore PageRank app, newRowPs["+newRowPs+"], newColPs["+newColPs+"] ...");
        G.remakeSparse(newRowPs, newColPs, nzd, newGroup, newAddedPlaces);	
        U.remake(G.getAggRowBs(), newGroup, newTeam, newAddedPlaces);
        P.remake(newGroup, newTeam, newAddedPlaces);

        GP.remake(G.getAggRowBs(), newGroup, newTeam, newAddedPlaces);
        
        for (sparePlace in newAddedPlaces){
            Console.OUT.println("Adding place["+sparePlace+"] to appTempDataPLH ...");
            PlaceLocalHandle.addPlace[AppTempData](appTempDataPLH, sparePlace, ()=>new AppTempData());
        }    
        Console.OUT.println("Restore succeeded. Restarting from iteration["+appTempDataPLH().iter+"] ...");
    }
    
    public def restore_local(store:DistObjectSnapshot, lastCheckpointIter:Long):void {
    	//using finish here causes deadlock, we use when instead
    	val Gstatus = new AtomicInteger(0N);
    	val Ustatus = new AtomicInteger(0N);
    	
    	async {
    		try{
    			G.restoreSnapshot_local("G", store);
    			Gstatus.set(1N);
    		}
    		catch(ex:Exception){
    			ex.printStackTrace();
    			Gstatus.set(2N);
    		}
    	}
	    async {
	    	try{
	    		U.restoreSnapshot_local("U", store);
	    		Ustatus.set(1N);
	    	}
	    	catch(ex:Exception){
	    		ex.printStackTrace();
    			Ustatus.set(2N);
	    	}
	    }
	    
	    P.restoreSnapshot_local("P", store);
	    appTempDataPLH().iter = lastCheckpointIter; 
	    
	    when(Gstatus.get() > 0N && Ustatus.get() > 0N);
	    
	    if (Gstatus.get() == 2N || Ustatus.get() == 2N)
	    	throw new Exception(here + " Restore failed  Gstatus["+Gstatus.get()+"]  Ustatus["+Ustatus.get()+"] ...");
    }
    
    class AppTempData{
        public var iter:Long;
    }
}
