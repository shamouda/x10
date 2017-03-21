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

import x10.util.ArrayList;
import x10.util.HashSet;

public class NestingTx (plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, depth:Long, parent:GlobalRef[NestingTx]) {
    private val members = new HashSet[Place]();
    private val futures = new ArrayList[Future[Any]]();
    
    public def addMember(target:Place) {
        if (!members.contains(target))
    	    members.add(target);
    }
    
    
}