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

import x10.util.Set;
import x10.util.concurrent.Future;

public class TxOpResult {
    var set:Set[String];
    var value:Any;
    var future:Future[Any];
    public def this(s:Set[String]) {
        set = s;
    }
    public def this(v:Any) {
        value = v;
    }
    public def this(f:Future[Any]) {
        future = f;
    }
}