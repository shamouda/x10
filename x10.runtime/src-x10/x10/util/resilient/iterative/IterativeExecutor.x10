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

package x10.util.resilient.iterative;

import x10.util.Timer;
import x10.regionarray.Dist;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.Team;
import x10.util.RailUtils;
import x10.xrx.Runtime;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.store.PlaceLocalStore;

public abstract class IterativeExecutor(home:Place) {
    protected static val VERBOSE = (System.getenv("EXECUTOR_DEBUG") != null 
                                && System.getenv("EXECUTOR_DEBUG").equals("1"));

    protected val manager:GlobalRef[PlaceManager]{self.home == this.home};
    protected val resilientMap:PlaceLocalStore[Cloneable];
    protected var team:Team;
    protected val ckptInterval:Long;
    protected val isResilient:Boolean;
    
    // configuration parameters for killing places at different times
    protected var hammer:SimplePlaceHammer;

    public def this(ckptInterval:Long, sparePlaces:Long, supportShrinking:Boolean) {
        property(here);
        var thisTime:Long = 0;
        thisTime -= Timer.milliTime();
        isResilient = ckptInterval > 0 && x10.xrx.Runtime.RESILIENT_MODE > 0;
        this.ckptInterval = ckptInterval;
        val mgr = new PlaceManager(sparePlaces, supportShrinking);
        this.manager = GlobalRef[PlaceManager](mgr);
        
        var teamTime:Long = 0;
        teamTime -= Timer.milliTime();
        team = new Team(mgr.activePlaces());
        teamTime += Timer.milliTime();
        
        var storeTime:Long = 0;
        storeTime -= Timer.milliTime();
        if (isResilient) {
            this.resilientMap = PlaceLocalStore.make[Cloneable]("_map_", mgr.activePlaces());
            this.hammer = new SimplePlaceHammer();
            hammer.printPlan();
        }
        else {        	            
            this.resilientMap = null;
            this.hammer = null;
        }
        storeTime += Timer.milliTime();
        
        thisTime += Timer.milliTime();
        Console.OUT.println("IterativeExecutor created successfully:teamTime:"+teamTime+":storeTime:"+storeTime+":totalTime:"+thisTime);
    }

    
    public def setHammer(h:SimplePlaceHammer){here == home} {
        hammer = h;
    }

    public def activePlaces(){here == home} = manager().activePlaces();

    public def team(){here == home} = team;

    public def run(app:SPMDResilientIterativeApp){here == home} {
        run(app, Timer.milliTime());
    }
    
    public abstract def run(app:SPMDResilientIterativeApp, startRunTime:Long){here == home}:void ;
}

