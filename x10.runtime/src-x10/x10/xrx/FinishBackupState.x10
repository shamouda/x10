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
package x10.xrx;

import x10.util.GrowableRail;
import x10.util.HashMap;

public abstract class FinishBackupState {
    abstract def markAsAdopted():void;
    abstract def exec(req:FinishRequest):BackupResponse;
    abstract def getNewMasterBlocking():FinishResilient.Id;
    
    abstract def getId():FinishResilient.Id;
    abstract def lock():void;
    abstract def unlock():void;
    abstract def getParentId():FinishResilient.Id;
    abstract def getPlaceOfMaster():Int;
    
    abstract def sync(_numActive:Long, _transit:HashMap[FinishResilient.Edge,Int],
            _sent:HashMap[FinishResilient.Edge,Int],
            _excs:GrowableRail[CheckedThrowable], _placeOfMaster:Int):void;
}

class BackupResponse {
    var excp:Exception;
}