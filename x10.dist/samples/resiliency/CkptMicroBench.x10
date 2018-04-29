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
    private static ITERS = 10;
    
    public static def main(args:Rail[String]){
        val ckptCentral = new CheckpointingCentral();
        val ckptAgree = new CheckpointingAgree();
    }
}

class CheckpointingCentral implements SPMDResilientIterativeApp {
    public def isFinished_local() {
        return true;
    }
    
    public def step_local() {}
    
    public def getCheckpointData_local() {
        return new HashMap[String,Cloneable]();
    }
    
    public def remake(changes:ChangeDescription, newTeam:Team) {
        
    }
    
    public def restore_local(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) { }   
}

class CheckpointingAgree implements SPMDResilientIterativeApp {
    
    public def isFinished_local() {
        return true;
    }
    
    public def step_local() {}
    
    public def getCheckpointData_local() {
        return new HashMap[String,Cloneable]();
    }
    
    public def remake(changes:ChangeDescription, newTeam:Team) {
        
    }
    
    public def restore_local(restoreDataMap:HashMap[String,Cloneable], lastCheckpointIter:Long) { }   
}
