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

import x10.util.*;
import x10.util.concurrent.Lock;
import x10.compiler.Inline;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicLong;
import x10.compiler.Ifdef;
import x10.util.resilient.localstore.Cloneable;

public class LocalStore {
    static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
        
    public var masterStore:MasterStore = null;
    public var virtualPlaceId:Long = -1; //-1 means a spare place
    
    /*resilient mode variables*/    
    public var slave:Place;
    public var slaveStore:SlaveStore = null;
    
    public def this(virtualPlaceId:Long, slave:Place) {
        this.virtualPlaceId = virtualPlaceId;
        masterStore = new MasterStore(new HashMap[String,Cloneable]());
        if (resilient) {
            slaveStore = new SlaveStore();
            this.slave = slave;
        }
    }

    /* used to initialize elastically added or spare places */
    public def this() { 
    }

    /*used when a spare place joins*/
    public def joinAsMaster (virtualPlaceId:Long, slave:Place, data:HashMap[String,Cloneable]) {
        assert(resilient);
        this.virtualPlaceId = virtualPlaceId;
        masterStore = new MasterStore(data);
        slaveStore = new SlaveStore();
        this.slave = slave;
    }
}