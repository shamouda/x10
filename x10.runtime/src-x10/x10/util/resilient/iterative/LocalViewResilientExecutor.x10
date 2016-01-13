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

public class LocalViewResilientExecutor {
    private var placeTempData:PlaceLocalHandle[PlaceTempData];
    private transient val store:ApplicationSnapshotStore;
    private transient var places:PlaceGroup;
    private val itersPerCheckpoint:Long;
    private var isResilient:Boolean = false;
    private val VERBOSE = (System.getenv("DEBUG_RESILIENT_EXECUTOR") != null 
                        && System.getenv("DEBUG_RESILIENT_EXECUTOR").equals("1"));
    
    private val KILL_ITERATION:Long; 
    private val KILL_PLACE_ID:Long;
    
    private transient var runTime:Long = 0;
    private transient var checkpointTime:Long = 0;
    private transient var checkpointString:String = "";
    private transient var checkpointCount:Long = 0;
    private transient var restoreTime:Long = 0;
    private transient var appOnlyRestoreTime:Long = 0;
    private transient var restoreCount:Long = 0;
    private transient var stepExecTime:Long = 0;
    private transient var failureDetectionTime:Long = 0;
    private transient var applicationInitializationTime:Long = 0;
    private transient var stepExecCount:Long = 0;
    //private var hammer:PlaceHammer = null;
    
    private transient var restoreRequired:Boolean = false;
    private transient var restoreJustDone:Boolean = false;
    private transient var lastCheckpointIter:Long = -1;
    
    class PlaceTempData {
        var globalIter:Long = 0;
        var place0DebuggingTotalIter:Long = 0;
        var place0TimePerIter:ArrayList[Long];      
        var place0TimeBeforeStep:Long;
        var place0KillPlaceTime:Long;
    
        public def this(iter:Long){
            globalIter = iter;
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
            
            /*val hammerConfigFile = System.getenv("X10_HAMMER_FILE");
            if (hammerConfigFile != null && !hammerConfigFile.equals("")){
                hammer = PlaceHammer.make(hammerConfigFile);
            }
            */
        }
        else{
            KILL_ITERATION = -1;
            KILL_PLACE_ID = -1;
        }
        store = (isResilient)? new ApplicationSnapshotStore(true, places):null;
    }

    public def run(app:LocalViewResilientIterativeApp) {
        run(app, Timer.milliTime());
    }
    
    //the startRunTime parameter is added to allow the executor to consider 
    //any initlization time done by the application before starting the executor  
    public def run(app:LocalViewResilientIterativeApp, startRunTime:Long) {
    	Console.OUT.println("LocalViewResilientExecutor: Application start time ["+startRunTime+"] ...");
		
        applicationInitializationTime = Timer.milliTime() - startRunTime;
        
        val root = here;
        placeTempData = PlaceLocalHandle.make[PlaceTempData](places, ()=>new PlaceTempData(0));
        placeTempData().place0TimePerIter = new ArrayList[Long]();
        /*
        if (isTimerHammerActive())
            hammer.startTimerHammer();
        */
        do{
            try {

                if (restoreRequired) {
                    if (lastCheckpointIter > -1) {
                        if (VERBOSE) Console.OUT.println("Restoring to iter " + lastCheckpointIter);
                        restoreTime -= Timer.milliTime();
                        
                        val restorePGResult = PlaceGroupBuilder.createRestorePlaceGroup(places);
                        val newPG = restorePGResult.newGroup;
                        
                        if (VERBOSE){
                            var str:String = "";
                            for (p in newPG)
                                str += p.id + ",";
                            Console.OUT.println("Restore places are: " + str);
                        } 

                        /*if (isIterativeHammerActive()){
                            val tmpIter = placeTempData().globalIter;
                            async hammer.checkKillRestore(tmpIter);
                        }*/
                        
                        appOnlyRestoreTime -= Timer.milliTime();
                        store.updatePlaces(newPG);
                        app.restore(newPG, store, lastCheckpointIter, restorePGResult.newAddedPlaces);
                        appOnlyRestoreTime += Timer.milliTime();
                        
                        
                        val lastIter = lastCheckpointIter;
                        //save place0 debugging data
                        val tmpPlace0LastIteration = placeTempData().place0DebuggingTotalIter;
                        val tmpPlace0TimePerIter = placeTempData().place0TimePerIter;
                        
                        Console.OUT.println("LocalViewResilientExecutor: destroying PLH ...");
                        PlaceLocalHandle.destroy(places, placeTempData, (Place)=>true);
                        placeTempData = PlaceLocalHandle.make[PlaceTempData](newPG, ()=>new PlaceTempData(lastIter));
                        Console.OUT.println("LocalViewResilientExecutor: PLH recreated successfully ...");
                        
                        //restore place0 debugging data
                        placeTempData().place0DebuggingTotalIter = tmpPlace0LastIteration;
                        placeTempData().place0TimePerIter = tmpPlace0TimePerIter;
                        
                        places = newPG;
                        restoreRequired = false;
                        restoreJustDone = true;
                        restoreTime += Timer.milliTime();
                        restoreCount++;
                        Console.OUT.println("LocalViewResilientExecutor: All restore steps completed successfully ...");
                    } else {
                        throw new UnsupportedOperationException("failure occurred at iter "
                            + placeTempData().globalIter + " but no valid checkpoint exists!");
                    }
                }

                if (isResilient && !restoreJustDone) {
                    val startCheckpoint = Timer.milliTime();
                                    
                    //take new checkpoint only if restore was not done in this iteration
                    if (VERBOSE) Console.OUT.println("checkpointing at iter " + placeTempData().globalIter);
                    try {
                    	
                        /*if (isIterativeHammerActive()) {
                            val tmpIter = placeTempData().globalIter;
                            async hammer.checkKillCheckpoint(tmpIter);
                        }
                        */
                        app.checkpoint(store);
                         
                        lastCheckpointIter = placeTempData().globalIter;
                        val checkpointingTime = Timer.milliTime() - startCheckpoint;
                        checkpointString += checkpointingTime + ",";
                        checkpointTime += checkpointingTime;
                        checkpointCount++;            
                    } catch (ex:Exception) {
                        store.cancelSnapshot();
                        throw ex;
                    }                    
                }
                else {
                    restoreJustDone = false;
                }
                
                stepExecTime -= Timer.milliTime();
                try{
                	Console.OUT.println("Starting a new round of steps, time consumed till now ["+(Timer.milliTime()-startRunTime)+"]...");
                    finish ateach(Dist.makeUnique(places)) {
                    	
                    	if (restoreCount > 0)
                    	    Console.OUT.println(here);
                    	
                    	var localIter:Long = 0;
                        while ( !app.isFinished_local() && 
                                (!isResilient || (isResilient && localIter < itersPerCheckpoint)) 
                               ) {
                        	
                        	
                        	val tmpIter = placeTempData().globalIter;
                        	
                        	if (KILL_ITERATION == tmpIter && here.id == KILL_PLACE_ID){
                        		at(Place(0)){
                        			placeTempData().place0KillPlaceTime = Timer.milliTime();
                                    Console.OUT.println("[Hammer Log] Time before killing is ["+placeTempData().place0KillPlaceTime+"] ...");
                        		}
                        		Console.OUT.println("[Hammer Log] Killing ["+here+"] ...");
                        		System.killHere();
                        	}
                        	
                        	/*
                            if (isIterativeHammerActive()){
                                val tmpIter = placeTempData().globalIter;
                                async hammer.checkKillStep_local(tmpIter);
                            }
                            */
                        
                            placeTempData().place0TimeBeforeStep = Timer.milliTime();
                            
                            app.step_local();
                            
                            if (here.id == 0)
                                placeTempData().place0TimePerIter.add( (Timer.milliTime()-placeTempData().place0TimeBeforeStep) );
                            
                            placeTempData().globalIter++;
                            
                            localIter++;
                            
                            placeTempData().place0DebuggingTotalIter++;
                        }
                    }
                    Console.OUT.println("Steps round completed successfully ...");
                    stepExecTime += Timer.milliTime();
                } catch (ex:Exception) {
                    Console.OUT.println("[Hammer Log] Time DPE discovered is ["+Timer.milliTime()+"] ...");
                    if (KILL_ITERATION != -1)
                        failureDetectionTime = Timer.milliTime() - placeTempData().place0KillPlaceTime;
                    else
                        failureDetectionTime = -1;
                    stepExecTime += Timer.milliTime() - failureDetectionTime;
                    placeTempData().place0TimePerIter.add( (Timer.milliTime()-placeTempData().place0TimeBeforeStep) );
                    throw ex;
                }
            
                
            }
            catch (iterEx:Exception) {
                processIterationException(iterEx);
                restoreRequired = true;
            }
        }while(restoreRequired || !app.isFinished_local());
        
        val runTime = (Timer.milliTime() - startRunTime);
        /*
        if (isTimerHammerActive())
            hammer.stopTimerHammer();
        */
        Console.OUT.println("ResilientExecutor completed:checkpointTime:"+checkpointTime+":restoreTime:"+restoreTime+":stepsTime:"+stepExecTime+":AllTime:"+runTime+":checkpointCount:"+checkpointCount+":restoreCount:"+restoreCount+":totalIterations:"+placeTempData().place0DebuggingTotalIter+":applicationOnlyRestoreTime:"+appOnlyRestoreTime+":failureDetectionTime:"+failureDetectionTime+":applicationInitializationTime:"+applicationInitializationTime);
        Console.OUT.println("DetailedCheckpointingTime:"+checkpointString);
        var timePerIterStr:String = "";
        for (x in placeTempData().place0TimePerIter)
            timePerIterStr += x + ",";
        Console.OUT.println("Place0TimePerIteration:" + timePerIterStr);
        
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
    /*
    private def isTimerHammerActive() = (hammer != null && hammer.isTimerHammer());
    private def isIterativeHammerActive() = (hammer != null && hammer.isIterativeHammer());
    */
}
