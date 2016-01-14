/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2015.
 *  (C) Copyright Sara Salem Hamouda 2014-2015.
 */

package x10.util.resilient.iterative;

import x10.util.Timer;
import x10.util.Random;
import x10.regionarray.Dist;
import x10.util.ArrayList;
import x10.util.Team;
import x10.util.GrowableRail;

public class LocalViewResilientExecutorOpt {
    private var placeTempData:PlaceLocalHandle[PlaceTempData];
    private transient var places:PlaceGroup;
    private var team:Team;
    private val itersPerCheckpoint:Long;
    private var isResilient:Boolean = false;
    private val VERBOSE = (System.getenv("DEBUG_RESILIENT_EXECUTOR") != null 
                        && System.getenv("DEBUG_RESILIENT_EXECUTOR").equals("1"));
    
    private val KILL_ITERATION:Long; 
    private val KILL_PLACE_ID:Long;
    
    private transient var runTime:Long = 0;
    private transient var restoreTime:Long = 0;
    private transient var appOnlyRestoreTime:Long = 0;
    private transient var restoreCount:Long = 0;
    //private transient var stepExecTime:Long = 0;
    private transient var failureDetectionTime:Long = 0;
    private transient var applicationInitializationTime:Long = 0;
    private transient var stepExecCount:Long = 0;
    
    private transient var restoreRequired:Boolean = false;
    private transient var restoreJustDone:Boolean = false;
    
    class PlaceTempData {
        var place0DebuggingTotalIter:Long = 0;
        var place0KillPlaceTime:Long = -1;
        ///checkpoint variables///
        var lastCheckpointIter:Long;
        val checkpointTimes:Rail[Long];
        var checkpointLastIndex:Long = -1;
        var commitCount:Long = 0;
        val snapshots:Rail[DistObjectSnapshot];
        ///step time logging ////
        val stepTimes:Rail[Long];
        var stepLastIndex:Long = -1;

        public def this(checkpointLastIndex:Long, snapshots:Rail[DistObjectSnapshot]){
            stepTimes = new Rail[Long](1000); //TODO use ArrayList
            checkpointTimes = new Rail[Long](100); // TODO: use ArrayList
            this.checkpointLastIndex = checkpointLastIndex; 
            this.snapshots = snapshots;
        }
    
        private def getConsistentSnapshot():DistObjectSnapshot{
            val idx = commitCount % 2;
            return snapshots(idx);
        }  
        
        public def getNextSnapshot():DistObjectSnapshot {
            val idx = (commitCount+1) % 2;
            return snapshots(idx);
        }

        /** Cancel a snapshot, in case of failure during checkpoint. */
        public def cancelOtherSnapshot() {
        	val idx = (commitCount+1) % 2;
            snapshots(idx).deleteAll_local();
        }
        
        public def commit() {
            commitCount++; // switch to the new snapshot
        }
        
        public def rollback(){
        	commitCount++; // switch to the new snapshot
        }
    }
    
    public def this(itersPerCheckpoint:Long, places:PlaceGroup) {
        this.places = places;
        this.itersPerCheckpoint = itersPerCheckpoint;
        if (itersPerCheckpoint > 0 && x10.xrx.Runtime.RESILIENT_MODE > 0) {
            isResilient = true;
            val killIterStr = System.getenv("RESILIENT_EXECUTOR_KILL_ITER");
            val killPlaceStr = System.getenv("RESILIENT_EXECUTOR_KILL_PLACE");
            KILL_ITERATION = (killIterStr != null)?Long.parseLong(killIterStr):-1;
            KILL_PLACE_ID = (killPlaceStr != null)?Long.parseLong(killPlaceStr):-1;  
        }
        else{
            KILL_ITERATION = -1;
            KILL_PLACE_ID = -1;
        }
        
    }

    public def run(app:LocalViewResilientIterativeAppOpt) {
        run(app, Timer.milliTime());
    }
    
    //the startRunTime parameter is added to allow the executor to consider 
    //any initlization time done by the application before starting the executor  
    public def run(app:LocalViewResilientIterativeAppOpt, startRunTime:Long) {
    	Console.OUT.println("LocalViewResilientExecutor: Application start time ["+startRunTime+"] ...");
        applicationInitializationTime = Timer.milliTime() - startRunTime;
        val root = here;
        val snapshots = (isResilient)?new Rail[DistObjectSnapshot](2, DistObjectSnapshot.make()):null;
        placeTempData = PlaceLocalHandle.make[PlaceTempData](places, ()=>new PlaceTempData(-1, snapshots));
        team = new Team(places);
        var globalIter:Long = 0;
        
        do{
            try {
            	restoreJustDone = false;
                if (restoreRequired) {
                    if (placeTempData().lastCheckpointIter > -1) {
                        if (VERBOSE) Console.OUT.println("Restoring to iter " + placeTempData().lastCheckpointIter);
                        restoreTime -= Timer.milliTime();
                        
                        val restorePGResult = PlaceGroupBuilder.createRestorePlaceGroup(places);
                        val newPG = restorePGResult.newGroup;
                        val addedPlaces = restorePGResult.newAddedPlaces;
                        
                        if (VERBOSE){
                            var str:String = "";
                            for (p in newPG)
                                str += p.id + ",";
                            Console.OUT.println("Restore places are: " + str);
                        } 
                        appOnlyRestoreTime -= Timer.milliTime();
                        
                        team = new Team(places);
                        //TODO: change restore to use DistObjectSnapshot class instead of ApplicationStore
                        val store = placeTempData().getConsistentSnapshot();
                        app.restore(newPG, team, store, placeTempData().lastCheckpointIter, addedPlaces);
                        appOnlyRestoreTime += Timer.milliTime();
                        
                        val lastIter = placeTempData().lastCheckpointIter;
                        //save place0 debugging data
                        val tmpPlace0LastCheckpointIndex = placeTempData().checkpointLastIndex;
                        
                        for (sparePlace in addedPlaces){
                            Console.OUT.println("LocalViewResilientExecutor: Adding place["+sparePlace+"] ...");           
                            PlaceLocalHandle.addPlace[PlaceTempData](placeTempData, sparePlace, ()=>new PlaceTempData(tmpPlace0LastCheckpointIndex, snapshots));
                        }
                        
                        places = newPG;
                        globalIter = lastIter;
                        
                        restoreRequired = false;
                        restoreJustDone = true;
                        restoreTime += Timer.milliTime();
                        restoreCount++;
                        Console.OUT.println("LocalViewResilientExecutor: All restore steps completed successfully ...");
                    } else {
                        throw new UnsupportedOperationException("process failure occurred but no valid checkpoint exists!");
                    }
                }
                
                //to be copied to all places
                val tmpRestoreJustDone = restoreJustDone;
                val tmpGlobalIter = globalIter;
                val placesCount = places.size();
                finish ateach(Dist.makeUnique(places)) {
                    var localIter:Long = tmpGlobalIter;
                    var localRestoreJustDone:Boolean = tmpRestoreJustDone;
                    
                    while ( !app.isFinished_local() ) {
                    	var stepStartTime:Long = -1; // (-1) is used to differenciate between checkpoint exceptions and step exceptions
                        try{
                        	// kill iteration?
                        	if (isResilient && KILL_ITERATION == localIter && here.id == KILL_PLACE_ID){
                        		at(Place(0)){
                        			placeTempData().place0KillPlaceTime = Timer.milliTime();
                                    Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
                        		}
                        		Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
                        		System.killHere();
                        	}
                        	
                        	//sync places & detect DPE
                        	team.barrier();
                        	
                        	//checkpoint iteration?
                        	if (!localRestoreJustDone) {
                                //take new checkpoint only if restore was not done in this iteration
                                if (isResilient && (localIter % itersPerCheckpoint) == 0) {
                                    if (VERBOSE) Console.OUT.println("checkpointing at iter " + localIter);
                                    checkpointProtocol_local(app, team, placeTempData(), root, placesCount);
                                    placeTempData().lastCheckpointIter = localIter;
                                }
                            } else {
                            	localRestoreJustDone = false;
                            }
                        	
                        	stepStartTime = Timer.milliTime();
                        	
                            app.step_local();
                            
                            placeTempData().stepTimes(++placeTempData().stepLastIndex) = Timer.milliTime()-stepStartTime;
                            
                            if (here.id == 0)
                                placeTempData().place0DebuggingTotalIter++;
                            
                            localIter++;
                            
                        } catch (ex:Exception) {
                        	if (stepStartTime != -1) { //step has started
                        	    placeTempData().stepTimes(++placeTempData().stepLastIndex) = Timer.milliTime()-stepStartTime;
                        	}
                            Console.OUT.println("[Hammer Log] Time DPE discovered is ["+Timer.milliTime()+"] ...");
                            if (isResilient && containsDPE(ex)){
                                if (placeTempData().place0KillPlaceTime != -1)
                                    failureDetectionTime = Timer.milliTime() - placeTempData().place0KillPlaceTime;
                                else
                                    failureDetectionTime = -1;
                            }
                            
                            throw ex;
                        }//step catch block
                    }//while !isFinished
                        
                }//finish ateach    
            }
            catch (iterEx:Exception) {
            	//exception from finish_ateach  or from restore
            	if (isResilient && containsDPE(iterEx))
            		restoreRequired = true;
            	else
            		throw iterEx;
            }
        }while(restoreRequired || !app.isFinished_local());
        
        val runTime = (Timer.milliTime() - startRunTime);
        
        //var stepExecTime:Long = -1;//TODO: fix this
        //Console.OUT.println("ResilientExecutor completed:checkpointTime:"+checkpointTime+":restoreTime:"+restoreTime+":stepsTime:"+stepExecTime+":AllTime:"+runTime+":checkpointCount:"+checkpointCount+":restoreCount:"+restoreCount+":totalIterations:"+placeTempData().place0DebuggingTotalIter+":applicationOnlyRestoreTime:"+appOnlyRestoreTime+":failureDetectionTime:"+failureDetectionTime+":applicationInitializationTime:"+applicationInitializationTime);
        //Console.OUT.println("DetailedCheckpointingTime:"+checkpointString);
        
        if (VERBOSE){
            var str:String = "";
            for (p in places)
                str += p.id + ",";
            Console.OUT.println("List of final survived places are: " + str);            
        }
    }
    
    private def processIterationException(ex:Exception) {
        if (ex instanceof DeadPlaceException) {
            ex.printStackTrace();
            if (!isResilient) {
                throw ex;
            }
        }
        else if (ex instanceof MultipleExceptions) {
            val mulExp = ex as MultipleExceptions;
            if (isResilient) {                
                val filtered = mulExp.filterExceptionsOfType[DeadPlaceException]();
                if (filtered != null) throw filtered;
                val deadPlaceExceptions = mulExp.getExceptionsOfType[DeadPlaceException]();
                for (dpe in deadPlaceExceptions) {
                    dpe.printStackTrace();
                }
            } else {
                throw mulExp;
            }
        }
        else
            throw ex;
    }
    
    private def containsDPE(ex:Exception):Boolean{
    	if (ex instanceof DeadPlaceException)
    		return true;
    	if (ex instanceof MultipleExceptions) {
            val mulExp = ex as MultipleExceptions;
            val deadPlaceExceptions = mulExp.getExceptionsOfType[DeadPlaceException]();
            if (deadPlaceExceptions == null)
            	return false;
            else
            	return true;
        }
    	
    	return false;
    }
    
    private def checkpointProtocol_local(app:LocalViewResilientIterativeAppOpt, team:Team, placeTmpData:PlaceTempData, root:Place, placesCount:Long){
        val startCheckpoint = Timer.milliTime();
        var checkpointSucceeded:Boolean = false;
        val excs = new GrowableRail[CheckedThrowable]();

        val store = placeTmpData.getNextSnapshot();
        var vote:Long = 1;
        try{
            //change store to use DistObjSnapsot
            app.checkpoint_local(store);
        }catch(ex:Exception){
            vote = 0;
            excs.add(ex);
        }
            
        //phase-1: voting
        var totalVotes:Long = 0;
        try{
            totalVotes = team.allreduce(vote, Team.ADD);
            //the semantics of Team.allReduce allows some places to succeed while others receive DPE
        }
        catch(vEx:Exception){
        	excs.add(vEx);
        }
            
        var phase1Succeeded:Boolean = false;
        if (totalVotes == placesCount){
            placeTmpData.commit();
            phase1Succeeded = true;
            //other places might have noticed a DPE, and did not commit
        }
            
        //phase-2: completion
        var phase2Succeeded:Boolean = false;
        if (isResilient){
            try{
                team.barrier();
                phase2Succeeded = true;
                //everyone is alive // they all at the same state (either committed or not)
            }catch(cEx:Exception){
            	excs.add(cEx);
                if (phase1Succeeded){
                    placeTmpData.rollback();
            	}
            }
        }
        else  
            phase2Succeeded = true; 
            
        placeTmpData.cancelOtherSnapshot();
            
        placeTempData().checkpointTimes(++placeTempData().checkpointLastIndex) = Timer.milliTime() - startCheckpoint;
        
        if (excs.size() > 0){
        	throw new MultipleExceptions(excs);
        }
    }
}
