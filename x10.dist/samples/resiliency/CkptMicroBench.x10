/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

public class CkptMicroBench {
    private static ITER = 10;
    private static CKPT_INTERVAL = 1;
    
    public static def main(args:Rail[String]){
    	teamWarmup();
    	
        val app = new DummyIterApp(ITER);
        val executorCentral = new SPMDResilientIterativeExecutor(CKPT_INTERVAL, 0, false);
        Console.OUT.println("...............................................");
        Console.OUT.println("... Starting SPMDResilientIterativeExecutor ...");
        Console.OUT.println("...............................................");
        executorCentral.run(app, Timer.milliTime());
        
        Console.OUT.println("....................................................");
        Console.OUT.println("... Starting SPMDAgreeResilientIterativeExecutor ...");
        Console.OUT.println("....................................................");
        val executorAgree = new SPMDAgreeResilientIterativeExecutor(CKPT_INTERVAL, 0, false);
        executorAgree.run(app, Timer.milliTime());
    }
    
    public static def teamWarmup(){
        val places = Place.places();
        val team = new Team(places);
        val startWarmupTime = Timer.milliTime();
        Console.OUT.println("Starting team warm up ...");
        finish for (place in places) at (place) async {
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
    }
}

class DummyIterApp(maxIter:Long) implements SPMDResilientIterativeApp {
	var i:Long = 0;

    public def isFinished_local() {
        return i == maxIter;
    }
    
    public def step_local() {
    	i++;
    }
    
    public def getCheckpointData_local() {
        return new HashMap[String,Cloneable]();
    }
    public def remake(changes:ChangeDescription, newTeam:Team) { }
    public def restore_local(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) { }   
}
