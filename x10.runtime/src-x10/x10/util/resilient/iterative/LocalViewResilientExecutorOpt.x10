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


/*
 * TODO:
 * maximum retry for restore failures
 * use local view restore within the same fan-out of the steps and checkpoint
 * investigate team hanging with sockets again
 * when a palce dies, store.rebackup_local()
 * */
public class LocalViewResilientExecutorOpt {
    private var placeTempData:PlaceLocalHandle[PlaceTempData];
    private transient var places:PlaceGroup;
    private var team:Team;
    private val itersPerCheckpoint:Long;
    private var isResilient:Boolean = false;
    // if step_local() are implicitly synchronized, no need for a step barrier inside the executor
    private val implicitStepSynchronization:Boolean; 
    private val VERBOSE = (System.getenv("EXECUTOR_DEBUG") != null 
                        && System.getenv("EXECUTOR_DEBUG").equals("1"));
    
    //parameters for killing places at different times
    private val KILL_STEP = (System.getenv("EXECUTOR_KILL_STEP") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_STEP")):-1;
    private val KILL_STEP_PLACE = (System.getenv("EXECUTOR_KILL_STEP_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_STEP_PLACE")):-1;
    // index of the checkpoint first checkpoint (0), second checkpoint (1), ...etc
    private val KILL_CHECKVOTING_INDEX = (System.getenv("EXECUTOR_KILL_CHECKVOTING") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKVOTING")):-1;
    private val KILL_CHECKVOTING_PLACE = (System.getenv("EXECUTOR_KILL_CHECKVOTING_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKVOTING_PLACE")):-1;   
    private val KILL_CHECKCOMP_INDEX = (System.getenv("EXECUTOR_KILL_CHECKCOMP") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKCOMP")):-1;
    private val KILL_CHECKCOMP_PLACE = (System.getenv("EXECUTOR_KILL_CHECKCOMP_PLACE") != null)?Long.parseLong(System.getenv("EXECUTOR_KILL_CHECKCOMP_PLACE")):-1; 

    private transient var runTime:Long = 0;
    private transient var restoreTime:Long = 0;
    private transient var appOnlyRestoreTime:Long = 0;
    private transient var restoreCount:Long = 0;
    private transient var failureDetectionTime:Long = 0;
    private transient var applicationInitializationTime:Long = 0;
    
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
            Console.OUT.println("["+here+"] Consistent Checkpoint Index ["+idx+"] ...");
            return snapshots(idx);
        }  
        
        public def getNextSnapshot():DistObjectSnapshot {
            val idx = (commitCount+1) % 2;
            if (VERBOSE) Console.OUT.println("["+here+"] Temp Checkpoint Index ["+idx+"] ...");
            return snapshots(idx);
        }

        /** Cancel a snapshot, in case of failure during checkpoint. */
        public def cancelOtherSnapshot() {
        	val idx = (commitCount+1) % 2;
        	if (VERBOSE) Console.OUT.println("["+here+"] Deleting Checkpoint At Index ["+idx+"] ...");
            snapshots(idx).deleteAll_local();
        }
        
        public def commit() {
            commitCount++; // switch to the new snapshot
            if (VERBOSE) Console.OUT.println("["+here+"] Committed count ["+commitCount+"] ...");
        }
        
        public def rollback(){
        	commitCount++; // switch to the new snapshot
        	if (VERBOSE) Console.OUT.println("["+here+"] Rollbacked count ["+commitCount+"] ...");
        }
    }
    
    public def this(itersPerCheckpoint:Long, places:PlaceGroup, implicitStepSynchronization:Boolean) {
        this.places = places;
        this.itersPerCheckpoint = itersPerCheckpoint;
        this.implicitStepSynchronization = implicitStepSynchronization;
        if (itersPerCheckpoint > 0 && x10.xrx.Runtime.RESILIENT_MODE > 0) {
            isResilient = true;
            
            if (VERBOSE){
            	Console.OUT.println("EXECUTOR_KILL_STEP="+KILL_STEP);
            	Console.OUT.println("EXECUTOR_KILL_STEP_PLACE="+KILL_STEP_PLACE);
            	Console.OUT.println("EXECUTOR_KILL_CHECKVOTING="+KILL_CHECKVOTING_INDEX);
            	Console.OUT.println("EXECUTOR_KILL_CHECKVOTING_PLACE="+KILL_CHECKVOTING_PLACE);
            	Console.OUT.println("EXECUTOR_KILL_CHECKCOMP="+KILL_CHECKCOMP_INDEX);
            	Console.OUT.println("EXECUTOR_KILL_CHECKCOMP_PLACE="+KILL_CHECKCOMP_PLACE);
            }
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
                        
                        team = new Team(newPG);
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
                val tmpRestoreRequired = restoreRequired;
                val tmpGlobalIter = globalIter;
                val placesCount = places.size();
                finish ateach(Dist.makeUnique(places)) {
                    var localIter:Long = tmpGlobalIter;
                    var localRestoreJustDone:Boolean = tmpRestoreJustDone;
                    var localRestoreRequired:Boolean = tmpRestoreRequired;
                    
                    while ( !app.isFinished_local() /*|| localRestoreRequired*/) {
                    	var stepStartTime:Long = -1; // (-1) is used to differenciate between checkpoint exceptions and step exceptions
                        try{
                        	// kill iteration?
                        	if (isResilient && KILL_STEP == localIter && here.id == KILL_STEP_PLACE){
                        		at(Place(0)){
                        			placeTempData().place0KillPlaceTime = Timer.milliTime();
                                    Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
                        		}
                        		Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
                        		System.killHere();
                        	}
                        	
                        	//validate is restore required
                        	/*
                        	if (localRestoreRequired){
                        		restore_local(app, team, root, placesCount);
                        		localRestoreRequired = false;
                        		localRestoreJustDone = true;
                        	}
                        	*/
                        	
                        	if (!implicitStepSynchronization){
                        	    //to sync places & also to detect DPE
                        	    team.barrier();
                        	}
                        	
                        	//checkpoint iteration?
                        	if (!localRestoreJustDone) {
                                //take new checkpoint only if restore was not done in this iteration
                                if (isResilient && (localIter % itersPerCheckpoint) == 0) {
                                    if (VERBOSE) Console.OUT.println("["+here+"] checkpointing at iter " + localIter);
                                    checkpointProtocol_local(app, team, root, placesCount);
                                    placeTempData().lastCheckpointIter = localIter;
                                }
                            } else {
                            	localRestoreJustDone = false;
                            }
                        	
                        	stepStartTime = Timer.milliTime();
                            app.step_local();
                            placeTempData().stepTimes(++placeTempData().stepLastIndex) = Timer.milliTime()-stepStartTime;
                            
                            localIter++;
                            
                            if (here.id == 0)
                                placeTempData().place0DebuggingTotalIter++;
                            
                        } catch (ex:Exception) {
                        	if (stepStartTime != -1) { //failure happened during step, not during checkpoint
                        	    placeTempData().stepTimes(++placeTempData().stepLastIndex) = Timer.milliTime()-stepStartTime;
                        	}
                            throw ex;
                        }//step catch block
                    }//while !isFinished
                }//finish ateach    
            }
            catch (iterEx:Exception) {
            	iterEx.printStackTrace();
            	//exception from finish_ateach  or from restore
            	if (isResilient && containsDPE(iterEx)){
            		restoreRequired = true;
            		
            		Console.OUT.println("[Hammer Log] Time DPE discovered is ["+Timer.milliTime()+"] ...");
                    if (isResilient && containsDPE(iterEx)){
                        if (placeTempData().place0KillPlaceTime != -1)
                            failureDetectionTime = Timer.milliTime() - placeTempData().place0KillPlaceTime;
                        else
                            failureDetectionTime = -1;
                    }
            	}
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
    
    //Two phase commit protocol for ensuring consistent checkpointing.
    //Checkpointing will only occur in resilient mode
    //Limitation: we only assume the voting allreduce call to fail only with DPE, no other exceptions may occur
    private def checkpointProtocol_local(app:LocalViewResilientIterativeAppOpt, team:Team, root:Place, placesCount:Long){
        val startCheckpoint = Timer.milliTime();
        val excs = new GrowableRail[CheckedThrowable]();

        val store = placeTempData().getNextSnapshot();
        var vote:Long = 1;
        try{
            //change store to use DistObjSnapsot
            app.checkpoint_local(store);
        }catch(ex:Exception){
            vote = 0;
            excs.add(ex);
        }

        if (KILL_CHECKVOTING_INDEX == (placeTempData().checkpointLastIndex+1) && here.id == KILL_CHECKVOTING_PLACE){
    		at(Place(0)){
    			placeTempData().place0KillPlaceTime = Timer.milliTime();
                Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
    		}
    		Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
    		System.killHere();
    	}
        //phase-1: voting
        var totalVotes:Long = 0;
        try{
            totalVotes = team.allreduce(vote, Team.ADD);
            //the semantics of Team.allReduce allows some places to succeed while others may receive DPE
        }
        catch(vEx:Exception){
        	excs.add(vEx);
        }
            
        var phase1Succeeded:Boolean = false;
        if (totalVotes == placesCount){ // Limitation: this condition is assumed to be correct at each place in non-resilient mode
            placeTempData().commit();
            phase1Succeeded = true;
            //other places might have noticed a DPE, and did not commit
        }
        
        if (KILL_CHECKCOMP_INDEX == (placeTempData().checkpointLastIndex+1) && here.id == KILL_CHECKCOMP_PLACE){
    		at(Place(0)){
    			placeTempData().place0KillPlaceTime = Timer.milliTime();
                Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
    		}
    		Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
    		System.killHere();
    	}
        
        //phase-2: completion
        var phase2Succeeded:Boolean = false;
        try{
        	//TODO: barrier can only useful for detecting DPE exceptions, but they can not detect places failing due to other reasons
            team.barrier();
            phase2Succeeded = true;
            //everyone is alive // they all at the same state (either committed or not)
        }catch(cEx:Exception){
            //some places might have died, so rollback if you have committed
            excs.add(cEx);
            if (phase1Succeeded){
                placeTempData().rollback();
            }
        }
        //placeTempData().cancelOtherSnapshot();
        
        placeTempData().checkpointTimes(++placeTempData().checkpointLastIndex) = Timer.milliTime() - startCheckpoint;
        
        
        placeTempData().getConsistentSnapshot();
        
        
        if (excs.size() > 0){
        	throw new MultipleExceptions(excs);
        }
    }
}
