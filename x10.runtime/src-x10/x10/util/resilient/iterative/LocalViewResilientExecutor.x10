/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2014-2015.
 *  (C) Copyright Sara Salem Hamouda 2014-2015.
 */

package x10.util.resilient.iterative;

import x10.util.Timer;
import x10.util.Random;
import x10.regionarray.Dist;

public class LocalViewResilientExecutor {
    private var placeTempData:PlaceLocalHandle[PlaceTempData];
    private transient val store:ResilientStoreForApp;
    private transient var places:PlaceGroup;
    private val itersPerCheckpoint:Long;
    private var isResilient:Boolean = false;
    private val VERBOSE = false;
    
    private transient var runTime:Long = 0;
    private transient var checkpointTime:Long = 0;
    private transient var checkpointCount:Long = 0;
    private transient var restoreTime:Long = 0;
    private transient var restoreCount:Long = 0;
    private transient var stepExecTime:Long = 0;
    private transient var stepExecCount:Long = 0;
    private var hammer:PlaceHammer = null;
    
    private transient var restoreRequired:Boolean = false;
    private transient var restoreJustDone:Boolean = false;
    private transient var lastCheckpointIter:Long = -1;
    
    class PlaceTempData {
        var globalIter:Long = 0;
    }
    
    public def this(itersPerCheckpoint:Long, places:PlaceGroup) {
        this.places = places;
        this.itersPerCheckpoint = itersPerCheckpoint;
        if (itersPerCheckpoint > 0 && x10.xrx.Runtime.RESILIENT_MODE > 0) {
            isResilient = true;
            val hammerConfigFile = System.getenv("X10_GML_HAMMER_FILE");
            if (hammerConfigFile != null && !hammerConfigFile.equals("")){
                hammer = PlaceHammer.make(hammerConfigFile);
            }
        }
        store = (isResilient)? new ResilientStoreForApp(true, places):null;
    }

    public def run(app:LocalViewResilientIterativeApp) {
        val startRun = Timer.milliTime();
        
        val root = here;
        placeTempData = PlaceLocalHandle.make[PlaceTempData](places, ()=>new PlaceTempData());
        
        if (isTimerHammerActive())
            hammer.startTimerHammer();
        
        do{
            try {

                if (restoreRequired) {
                    if (lastCheckpointIter > -1) {
                        val startRestore = Timer.milliTime();
                        
                        val newPG = PlaceGroupBuilder.createRestorePlaceGroup(places);
                        if (VERBOSE) Console.OUT.println("restoring at iter " + lastCheckpointIter);

                        if (isIterativeHammerActive()){
                            val tmpIter = placeTempData().globalIter;
                            async hammer.checkKillRestore(tmpIter);
                        }

                        app.restore(newPG, store, lastCheckpointIter);

                        placeTempData = PlaceLocalHandle.make[PlaceTempData](newPG, ()=>new PlaceTempData());                        
                        placeTempData().globalIter = lastCheckpointIter;

                        places = newPG;
                        restoreRequired = false;
                        restoreJustDone = true;
                        restoreTime += (Timer.milliTime() - startRestore);
                        restoreCount++;
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
                        if (isIterativeHammerActive()) {
                            val tmpIter = placeTempData().globalIter;
                            async hammer.checkKillCheckpoint(tmpIter);
                        }            
                        app.checkpoint(store);
                         
                        lastCheckpointIter = placeTempData().globalIter;
                        checkpointTime += (Timer.milliTime() - startCheckpoint);
                        checkpointCount++;            
                    } catch (ex:Exception) {
                        store.cancelSnapshot();
                        throw ex;
                    }                    
                }
                else {
                    restoreJustDone = false;
                }
                
                val startStep = Timer.milliTime();
                finish ateach(Dist.makeUnique(places)) {                    
                    var localIter:Long = 0;
                    while ( !app.isFinished_local() && 
                            (!isResilient || (isResilient && localIter < itersPerCheckpoint)) 
                           ) {
                        if (isIterativeHammerActive()){
                            val tmpIter = placeTempData().globalIter;
                            async hammer.checkKillStep_local(tmpIter);
                        }
                        
                        app.step_local();
                        if (VERBOSE) Console.OUT.println("["+here+"] step completed globalIter["+placeTempData().globalIter+"] ...");
                        placeTempData().globalIter++;
                        localIter++;
                    }
                }
                stepExecTime += (Timer.milliTime() - startStep);
                stepExecCount++;
            }
            catch (iterEx:Exception) {                
                processIterationException(iterEx);
                restoreRequired = true;
            }
        }while(restoreRequired || !app.isFinished_local());
        
        val runTime = (Timer.milliTime() - startRun);
        if (isTimerHammerActive())
            hammer.stopTimerHammer();
        
        Console.OUT.println("ResilientExecutor completed:checkpointTime:"+checkpointTime+":restoreTime:"+restoreTime+":stepsTime:"+stepExecTime+":AllTime:"+runTime+":checkpointCount:"+checkpointCount+":restoreCount:"+restoreCount);
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
    
    private def isTimerHammerActive() = (hammer != null && hammer.isTimerHammer());
    private def isIterativeHammerActive() = (hammer != null && hammer.isIterativeHammer());
    
}
