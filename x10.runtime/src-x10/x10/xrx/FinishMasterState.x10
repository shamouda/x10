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
import x10.io.CustomSerialization;
import x10.io.Deserializer;
import x10.io.Serializer;
import x10.util.HashMap;

public abstract class FinishMasterState extends FinishResilient {
    abstract def exec(req:FinishRequest):MasterResponse;
    abstract def isImpactedByDeadPlaces(newDead:HashSet[Int]):Boolean;
    abstract def lock():void;
    abstract def unlock():void;
    abstract def getId():Id;
    abstract def dump():void;
}

class MasterResponse implements x10.io.Unserializable {
    var backupPlaceId:Int;
    var excp:Exception;
    var submit:Boolean = false;
    var transitSubmitDPE:Boolean = false;
    var backupChanged:Boolean = false;
    var parentId:FinishResilient.Id; /*used in ADD_CHILD only*/

    public def this() { }
    
    public def this(backupPlaceId:Int, excp:Exception, submit:Boolean, transitSubmitDPE:Boolean,
            backupChanged:Boolean, parentId:FinishResilient.Id) {
        this.backupPlaceId = backupPlaceId;
        this.excp = excp;
        this.submit = submit;
        this.transitSubmitDPE = transitSubmitDPE;
        this.backupChanged = backupChanged;
        this.parentId = parentId;
    }
}