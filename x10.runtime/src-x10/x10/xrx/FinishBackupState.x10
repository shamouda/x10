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

import x10.util.GrowableRail;
import x10.util.HashMap;

public abstract class FinishBackupState {
    abstract def exec(id:FinishResilient.Id, masterPlaceId:Int, reqType:Int, parentId:FinishResilient.Id, finSrc:Int, finKind:Int,
            map:HashMap[FinishResilient.Task,Int], 
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean, transitSubmitDPE:Boolean):Exception;
    abstract def getNewMasterBlocking():FinishResilient.Id;
    
    abstract def getId():FinishResilient.Id;
    abstract def lock():void;
    abstract def unlock():void;
    abstract def getParentId():FinishResilient.Id;
    abstract def getPlaceOfMaster():Int;
}

class BackupResponse {
    var excp:Exception;
}