/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.util.resilient.localstore;

public class TxResult {
    public val output:Any;
    public val commitStatus:Int; //SUCCESS or SUCCESS_RECOVER_STORE

    public def this(commitStatus:Int, output:Any) {
        this.output = output;
        this.commitStatus = commitStatus;
    }
    
}