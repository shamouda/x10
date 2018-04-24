/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Sara Salem Hamouda 2017-2018.
 */
package x10.xrx;

import x10.util.HashSet;
import x10.util.HashMap;
import x10.xrx.freq.FinishRequest;

public abstract class FinishMasterState extends FinishResilient {
    abstract def exec(req:FinishRequest):MasterResponse;
    abstract def isImpactedByDeadPlaces(newDead:HashSet[Int]):Boolean;
    abstract def lock():void;
    abstract def unlock():void;
    abstract def getId():Id;
    abstract def dump():void;
    abstract def getBackupId():Int;
    public def addGCRequests(pendingGC:HashMap[Int, HashSet[Id]]) {
      //overridden in optimistic finish only  
    } 
}

