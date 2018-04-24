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

public class MasterResponse {
    var backupPlaceId:Int;
    var errMasterMigrating:Boolean = false;
    var submit:Boolean = false;
    var transitSubmitDPE:Boolean = false;
    var backupChanged:Boolean = false;
    var parentIdHome:Int; /*used in ADD_CHILD only*/
    var parentIdSeq:Int;
    var gcReqs:HashSet[FinishResilient.Id];
}