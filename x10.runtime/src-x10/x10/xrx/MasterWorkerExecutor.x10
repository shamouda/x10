/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright Sara Salem Hamouda 2018-2018.
 */

package x10.xrx;
import x10.util.GrowableRail;
import x10.util.concurrent.SimpleLatch;
import x10.util.HashMap;
import x10.util.resilient.PlaceManager;
import x10.compiler.Uncounted;

public class MasterWorkerExecutor {
    public static val MW_DEBUG = System.getenv("MW_DEBUG") != null && System.getenv("MW_DEBUG").equals("1");
    private val master = GlobalRef[MasterWorkerExecutor](this);
    
    transient val app:MasterWorkerApp;
    transient var store:TxStore;
    transient var p0Cnt:Long;
    transient var p0Latch:SimpleLatch;
    transient var p0Excs:GrowableRail[CheckedThrowable] = null;
    transient val workerResults:HashMap[Long,Any] = new HashMap[Long,Any]();
    
    private def this(app:MasterWorkerApp) {
        this.app = app;
    }
    
    public static def make(activePlaces:PlaceGroup, app:MasterWorkerApp) {
        val asyncRecovery = true;
        val executor = new MasterWorkerExecutor(app);
        val master = executor.master;
        val callback = (vid:Long, newPlace:Place, store:TxStore)=>{
            at (newPlace) @Uncounted async {
                executor.runWorker(vid, store, app, master, true);
            }
        };
        val store = TxStore.make(activePlaces, asyncRecovery, callback);
        executor.store = store;
        return executor;
    }
    
    public static def makeRail(activePlaces:PlaceGroup, app:MasterWorkerApp, size:Long, init:(Long)=>Any) {
        val asyncRecovery = true;
        val executor = new MasterWorkerExecutor(app);
        val master = executor.master;
        val callback = (vid:Long, newPlace:Place, store:TxStore)=>{
            at (newPlace) @Uncounted async {
                executor.runWorker(vid, store, app, master, true);
            }
        };
        val store = TxStore.makeRail(activePlaces, asyncRecovery, callback, size, init);
        executor.store = store;
        return executor;
    }
    
    public def store() = store;
    
    private def print(msg:String) {
        if (MW_DEBUG) Console.OUT.println(msg);
    }
    
    public def run() {
        //avoid copying this
        val master = this.master;
        val store = this.store;
        val app = this.app;
        
        val activePlaces = store.fixAndGetActivePlaces();
        p0Cnt = activePlaces.size();
        p0Latch = new SimpleLatch();
        print(here + " MasterWorkerExecutor.run() p0Cnt="+p0Cnt);
        for (place in activePlaces) {
            val vid = activePlaces.indexOf(place);
            at (place) @Uncounted async {
                runWorker(vid, store, app, master, false);
            }
        }
        
        Runtime.increaseParallelism();
        p0Latch.await();
        Runtime.decreaseParallelism(1n);
        
        print(here + " MasterWorkerExecutor.await() completed p0Cnt="+p0Cnt);
        if (p0Excs != null)
            throw new MultipleExceptions(p0Excs);
    }
    
    private def runWorker(vid:Long, store:TxStore, app:MasterWorkerApp, master:GlobalRef[MasterWorkerExecutor], recovery:Boolean) {
        var ex:Exception = null;
        var result:Any = null;
        try {
            print(here + " MasterWorkerExecutor.startWorker(vid="+vid+")");
            result = app.execWorker(vid, store, recovery);
            print(here + " MasterWorkerExecutor.endWorker(vid="+vid+")");
            ex = null;
        } catch (workerEx:Exception) {
            print(here + " MasterWorkerExecutor.endWorker(vid="+vid+") exception thrown");
            workerEx.printStackTrace();
            result = null;
            ex = workerEx;
        }
        val e = ex;
        val r = result;
        at (master) @Uncounted async {
            master().notifyMaster(vid, r, e);
        }
    }
    
    public def workerResults() = workerResults;
    
    private def notifyMaster(vid:Long, result:Any, ex:CheckedThrowable) {
        print(here + " MasterWorkerExecutor.notifyMaster(vid="+vid+",result="+result+", ex="+ex+") ...");
        var lc:Long = -1;
        p0Latch.lock();
        if (ex != null) {
            if (p0Excs == null)
                p0Excs = new GrowableRail[CheckedThrowable]();
            p0Excs.add(ex);
        }
        lc = --p0Cnt;
        workerResults.put(vid, result);
        p0Latch.unlock();
        
        if (lc == 0)
            p0Latch.release();
    }
}