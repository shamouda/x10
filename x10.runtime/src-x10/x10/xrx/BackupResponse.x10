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

public class BackupResponse {
    var errMasterDied:Boolean = false;
    var errMasterChanged:Boolean = false; 
    var newMasterPlace:Int = -1n; //valid only if errMasterChanged = true
}