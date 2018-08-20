import x10.util.ArrayList;

import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.CloneableLong;
import x10.xrx.txstore.TxConfig;
import x10.xrx.TxStoreFatalException;
import x10.xrx.MasterWorkerApp;
import x10.xrx.MasterWorkerExecutor;
import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.xrx.TxLocking;
import x10.xrx.Runtime;
import x10.util.concurrent.Future;
import x10.xrx.Runtime;
import x10.util.HashMap;
import x10.util.Timer;
import x10.util.Option;
import x10.util.OptionsParser;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.HashSet;
import x10.compiler.Uncounted;
import x10.util.Team;

import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.GrowableRail;

public class ResilientTxBench(plh:PlaceLocalHandle[TxBenchState]) implements MasterWorkerApp {
    public static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public static def main(args:Rail[String]) {
        val opts = new OptionsParser(args, [
            Option("help","help","this information"),
            Option("v","verify","verify the result")
        ], [
            Option("r","keysRange","The range of possible keys (default 32K)"),
            Option("u","updatePercentage","percentage of update operation (default 0.0)"),
            Option("n","iterations","number of iterations of the benchmark (default 5)"),
            Option("p","txPlaces","number of places creating transactions (default (X10_PLACES-spare))"),
            Option("t","txThreadsPerPlace","number of threads creating transactions per place (default (X10_NTHREADS))"),
            Option("w","warmupTime","warm up time in milliseconds (default 5000 ms)"),
            Option("d","iterationDuration","Single iteration duration"),
            Option("h","txParticipants","number of transaction participants (default 2)"),
            Option("o","TxParticipantOperations","number of operations per transaction participant (default 2)"),
            Option("g","progress","interval of progress reporting per producer (default no progress reporting)"),
            Option("vp","victims","victim places to kill (comma separated)"),
            Option("vt","victimsTimes","times to kill victim places(comma separated)"),
            Option("s","spare","Spare places (default 0)"),
            Option("f","f","Transaction coordinator is also a participant (default 1) ")
        ]);
        
        val r = opts("r", 32*1024);
        val u = opts("u", 0.5F);
        val n = opts("n", 5);    
        val t = opts("t", Runtime.NTHREADS as Long);
        val w = opts("w", 5000);
        val d = opts("d", 5000);
        val h = opts("h", 2);
        val o = opts("o", 2);
        val g = opts("g", -1);
        val s = opts("s", 0);
        val vp = opts("vp", "");
        val vt = opts("vt", -1);
        val f = opts("f", 1) == 1;
        
        val mgr = new PlaceManager(s, false);
        val activePlaces = mgr.activePlaces();
        val p = opts("p", activePlaces.size());
        
        printRunConfigurations (r, u, n, p, t, w, d, h, o, g, s, f, vp, vt);
        
        assert (h <= activePlaces.size()) : "invalid value for parameter h, h should not exceed the number of active places" ;

        if (TxConfig.get().LOCK_FREE) {
            assert (p * t == 1): "lock free mode can only be used with only 1 producer thread";
        }
        
        val plh = PlaceLocalHandle.make[TxBenchState](Place.places(), ()=> new TxBenchState(here.id, r, u, n, p, t, w, d, h, o, g, s, f, vp, vt) );
        val app = new ResilientTxBench(plh);
        val executor = MasterWorkerExecutor.make(activePlaces, app);
        
        val startWarmup = Timer.milliTime();
        if (w == -1) {
            Console.OUT.println("no warmpup");
        } else {
            Console.OUT.println("warmup started");
            executor.run();
            app.resetStatistics(plh);
            Console.OUT.println("warmup completed, warmup elapsed time ["+(Timer.milliTime() - startWarmup)+"]  ms ");
        }
        
        try {
            for (iter in 1..n) {
                val startIter = Timer.milliTime();
                executor.run();
                Console.OUT.println("iteration:" + iter + " completed, iteration elapsedTime ["+(Timer.milliTime() - startIter)+"]  ms ");
                app.printThroughput(executor.store(), plh, iter);
                app.resetStatistics(plh);
            }
            Console.OUT.println("+++ TxBench Succeeded +++");
        }catch(ex:Exception) {
            Console.OUT.println("!!! TxBench Failed !!!");
            ex.printStackTrace();
        }
    }
    
    public def execWorker(vid:Long, store:TxStore, recovery:Boolean):Any {
        if (recovery) {
            val leftPlace = store.prevPlace();
            val me = here;
            finish at (leftPlace) async {
                val masterState = plh(); 
                if (masterState.rightPlaceDeathTimeNS == -1)
                    throw new TxBenchFailed(here + " assertion error, did not receive suicide note ...");
                val oldThroughput = masterState.rightTxBenchState;
                val recoveryTime = System.nanoTime() - masterState.rightPlaceDeathTimeNS;
                oldThroughput.shiftElapsedTime(recoveryTime);
                val iter = masterState.iteration;
                Console.OUT.println(here + " Calculated recovery time = " + (recoveryTime/ 1e9) + " seconds" );
                at (me) async {
                    plh().reinit(oldThroughput, iter); //state.recovered = true
                }
            }
        }
        
        val state = plh();
        state.iteration++;
        if (state.iteration > 0) killSelf(store, state, state.iteration);
        val t = state.t;
        finish for (thrd in 1..t) async {
            produce(store, plh, thrd-1);
        }
        return -1;
    }
    
    public def produce(store:TxStore, plh:PlaceLocalHandle[TxBenchState], producerId:Long) {
        val state = plh();
        val myVirtualPlaceId = state.virtualPlaceId;
        val h = state.h;
        val f = state.f;
        val r = state.r;
        val u = state.u;
        val o = state.o;
        val p = state.p;
        val t = state.t;
        val d = state.d;
        val g = state.g;
        
        state.started = true;
        if (state.recovered && producerId == 0) {
            Console.OUT.println(here + " Spare place started to produce transactions " + state.toString());
        }
        val rand = new Random((here.id+1) * t + producerId);
        val myThroughput = state.thrds(producerId);
        var timeNS:Long = myThroughput.elapsedTimeNS; // always 0 in non-resilient mode, non-zero for spare places in resilient mode

        //pre-allocate rails
        val virtualMembers = new Rail[Long](h);
        val keys = new Rail[Any] (h*o);
        val tmpKeys = new Rail[Long] (o);
        val values = new Rail[Long] (h*o);
        val readFlags = new Rail[Boolean] (h*o);
        
        while (timeNS < d*1e6) {
            val innerStr = nextTransactionMembers(rand, p, h, myVirtualPlaceId, virtualMembers, f);
            nextRandomOperations(rand, p, virtualMembers, r, u, o, keys, values, readFlags, tmpKeys);
            
            //time starts here
            val start = System.nanoTime();
            var includeTx:Boolean = true;
            if (TxConfig.get().LOCKING) {
                val distClosure = (tx:TxLocking) => {
                    for (var m:Long = 0; m < h; m++) {
                        val start = m*o;
                        val dest = virtualMembers(m);
                        tx.asyncAt(dest, () => {
                            for (var x:Long = 0; x < o; x++) {
                                val key = keys(start+x) as Long;
                                val read = readFlags(start+x);
                                val value = values(start+x);
                                read? tx.get(key): tx.put(key, new CloneableLong(value));
                            }
                        });
                    }
                };
                try {
                    store.executeLockingTx(virtualMembers, keys, readFlags, o, distClosure);
                } catch(expf:TxStoreFatalException) {
                    includeTx = false;
                    //expf.printStackTrace();
                }
            }
            else {
                val distClosure = (tx:Tx) => {
                    for (var m:Long = 0; m < h; m++) {
                        val start = m*o;
                        val dest = virtualMembers(m);
                        tx.asyncAt(dest, () => {
                            for (var x:Long = 0; x < o; x++) {
                                val key = keys(start+x) as Long;
                                val read = readFlags(start+x);
                                val value = values(start+x);
                                read? tx.get(key): tx.put(key, new CloneableLong(value));
                            }
                        });
                    }
                };
                
                val remainingTime =  (d*1e6 - timeNS) as Long;
                try {
                    store.executeTransaction(distClosure, -1, remainingTime);
                } catch(expf:TxStoreFatalException) {
                    includeTx = false;
                    //expf.printStackTrace();
                }
            }
            
            val elapsedNS = System.nanoTime() - start; 
            timeNS += elapsedNS;
            myThroughput.elapsedTimeNS = timeNS;
            
            if (includeTx) {
                myThroughput.txCount++;
                if (g != -1 && myThroughput.txCount%g == 0)
                    Console.OUT.println(here + " Progress " + myVirtualPlaceId + "x" + producerId + ":" + myThroughput.txCount );
            }
        }
        //Console.OUT.println(here + "==FinalProgress==> txCount["+myThroughput.txCount+"] elapsedTime["+(myThroughput.elapsedTimeNS/1e9)+" seconds]");
    }

    public def printThroughput(store:TxStore, plh:PlaceLocalHandle[TxBenchState], iteration:Long) {
        try {
            Console.OUT.println("========================================================================");
            Console.OUT.println("Collecting throughput information ..... .....");
            Console.OUT.println("========================================================================");
            
            val state = plh();
            val activePlcs = store.fixAndGetActivePlaces();
            val startReduce = System.nanoTime();
            if (state.p > 1) {
                val team = new Team(activePlcs);
                finish for (p in activePlcs) async at (p) {
                    val plcTh = plh();
                    val times = plcTh.mergeTimes();
                    val counts = plcTh.mergeCounts();
                    
                    plcTh.reduceSrc(0) = times;
                    plcTh.reduceSrc(1) = counts;
                    
                    team.allreduce(plcTh.reduceSrc, 0, plcTh.reduceDst, 0, 2, Team.ADD);
                    
                    if (!plcTh.started)
                        throw new TxBenchFailed(here + " never started ...");
                }
            }
            else {
                state.reduceDst(0) = state.mergeTimes();
                state.reduceDst(1) = state.mergeCounts();
            }
            val elapsedReduceNS = System.nanoTime() - startReduce;
            
            
            val allTimeNS = state.reduceDst(0);
            val cnt = state.reduceDst(1);
            val allOperations = cnt * state.h * state.o;
            
            val producers = state.p * state.t;
            val throughput = (allOperations as Double) / (allTimeNS/1e6) * producers;
            Console.OUT.println("Reduction completed in "+((elapsedReduceNS)/1e9)+" seconds   txCount["+cnt+"] OpCount["+allOperations+"]  timeNS["+allTimeNS+"]");
            Console.OUT.println("iteration:" + iteration + ":globalthroughput(op/MS):"+throughput);
            Console.OUT.println("========================================================================");
    
        } catch(ex:Exception) {
            throw new Exception("Failed while collecting throughput information ...");
        }
    }
    public static def resetStatistics(plh:PlaceLocalHandle[TxBenchState]) {
        Place.places().broadcastFlat(()=>{ plh().reset();}, (p:Place)=>true);
    }
    
    public static def nextTransactionMembers(rand:Random, activePlacesCount:Long, h:Long, myPlaceIndex:Long, result:Rail[Long], f:Boolean) {
        var selected:Long = 0;
        if (f){
            result(0) = myPlaceIndex;
            selected = 1 ;
        }
        
        while (selected < h) {
            val candidate = Math.abs(rand.nextLong()) % activePlacesCount;
            var repeated:Boolean = false;
            for (var i:Long = 0 ; i < selected; i++) {
                if (result(i) == candidate) {
                    repeated = true;
                    break;
                }
            }
            if (!repeated) {
                result(selected) = candidate;
                selected++;
            }
        }
        if (TxConfig.get().LOCKING)
            RailUtils.sort(result);
        
        var mstr:String = "";
        for (var i:Long = 0 ; i < result.size; i++) {
            mstr += result(i) + " ";
        }
        return mstr;
    }
    
    public static def nextRandomOperations(rand:Random, activePlacesCount:Long, virtualMembers:Rail[Long], r:Long, u:Float, o:Long,
            keys:Rail[Any], values:Rail[Long], readFlags:Rail[Boolean], tmpKeys:Rail[Long]) {
        val h = virtualMembers.size;
        val keysPerPlace = r / activePlacesCount;
        
        for (var i:Long = 0; i< h; i++) {
            val baseKey = virtualMembers(i) * keysPerPlace;
            val start = i*o;          
            
            for (var x:Long = 0; x < o; x++) {
                readFlags(start+x) = rand.nextFloat() > u;
                values(start+x) = Math.abs(rand.nextLong())%1000;
                var repeated:Boolean = false;
                do {
                    var k:Long = Math.abs(rand.nextLong()% keysPerPlace);
                    repeated = false;
                    for (var j:Long = 0; j < x; j++) {
                        if (tmpKeys(j) == k) {
                            repeated = true;
                            break;
                        }
                    }
                    if (!repeated) {
                        //tmpKeys is used to reduce string concat/compare operations
                        tmpKeys(x) = k;
                        
                    }
                }while (repeated);
            }
            
            if (TxConfig.get().LOCKING)
                RailUtils.sort(tmpKeys);
            
            for (var x:Long = 0; x < o; x++) {
                 keys(start+x) = baseKey + tmpKeys(x); //String.valueOf ( baseKey + tmpKeys(x) );
            }
        }
    }
    
    private def killSelf(store:TxStore, state:TxBenchState, iter:Long) {
        if (resilient && state.vp != null && iter != -1) {
            val arr = state.vp.split(",");
            if (arr.size >= iter) {
                val victim = Long.parseLong(arr(iter-1));
                if (here.id == victim) {
                    if (Runtime.NTHREADS == 1n) {
                        Runtime.increaseParallelism();
                    }
                    val killTime = state.vt * -1;
                    @Uncounted async {
                        Console.OUT.println("Hammer kill timer at "+here+" starting sleep for "+killTime+" secs");
                        val startedNS = System.nanoTime(); 
                        
                        val deadline = System.currentTimeMillis() + 1000 * killTime;
                        while (deadline > System.currentTimeMillis()) {
                            val sleepTime = deadline - System.currentTimeMillis();
                            System.sleep(sleepTime);
                        }
                        
                        val prev = store.plh().getMaster(here);
                        val myThroughput = plh();
                        myThroughput.setElapsedTime(System.nanoTime() - startedNS);
                        val me = here;
                        finish at (prev) async {
                            plh().rightTxBenchState = myThroughput;
                            plh().rightPlaceDeathTimeNS =  System.nanoTime();
                            Console.OUT.println(here + " Received suicide note from " + me + " througputValues: " + myThroughput);
                        }
                        
                        Console.OUT.println(here + " Hammer calling killHere" );
                        System.killHere();
                    }
                }
            }
        }
    }
    
    /***********************   Utils  *****************************/
    public static def printRunConfigurations(r:Long, u:Float, n:Long, p:Long, t:Long, w:Long, 
            d:Long, h:Long, o:Long, g:Long, s:Long, f:Boolean, vp:String, vt:Long) {
        Console.OUT.println("TxBench starting with the following parameters:");        
        Console.OUT.println("X10_NPLACES="  + Place.numPlaces());
        Console.OUT.println("X10_NTHREADS=" + Runtime.NTHREADS);
        Console.OUT.println("X10_NUM_IMMEDIATE_THREADS=" + System.getenv("X10_NUM_IMMEDIATE_THREADS"));
        Console.OUT.println("X10_RESILIENT_MODE=" + x10.xrx.Runtime.RESILIENT_MODE);
        Console.OUT.println("TM=" + System.getenv("TM"));
        Console.OUT.println("LOCK_FREE=" + System.getenv("LOCK_FREE"));
        Console.OUT.println("DISABLE_INCR_PARALLELISM=" + System.getenv("DISABLE_INCR_PARALLELISM"));
        Console.OUT.println("X10_EXIT_BY_SIGKILL=" + System.getenv("X10_EXIT_BY_SIGKILL"));
        Console.OUT.println("DISABLE_SLAVE=" + System.getenv("DISABLE_SLAVE"));
        Console.OUT.println("ENABLE_STAT=" + System.getenv("ENABLE_STAT"));
        Console.OUT.println("BUSY_LOCK=" + System.getenv("BUSY_LOCK"));
        
        Console.OUT.println("r=" + r);
        Console.OUT.println("u=" + u);
        Console.OUT.println("n=" + n);
        Console.OUT.println("p=" + p);
        Console.OUT.println("t=" + t);
        Console.OUT.println("w=" + w);
        Console.OUT.println("d=" + d);
        Console.OUT.println("h=" + h );
        Console.OUT.println("o=" + o);
        Console.OUT.println("g=" + g);
        Console.OUT.println("s=" + s);
        Console.OUT.println("f=" + f  + (f ? " !!! At least one place is local !!!! ": "h random places") );
        Console.OUT.println("vp=" + vp);
        Console.OUT.println("vt=" + vt);
    }
}

class ProducerThroughput {
    public var elapsedTimeNS:Long = 0;
    public var txCount:Long = 0;
    public val placeId:Long;
    public val threadId:Long;

    public def this (placeId:Long, threadId:Long, elapsedTimeNS:Long, txCount:Long) {
        this.elapsedTimeNS = elapsedTimeNS;
        this.txCount = txCount;
        this.placeId = placeId;
        this.threadId = threadId;
    }
    
    public def this(placeId:Long, threadId:Long) {
        this.placeId = placeId;
        this.threadId = threadId;
    }
    
    public def toString() {
        return placeId+"x"+threadId +": elapsedTime=" + elapsedTimeNS/1e9 + " seconds  txCount= " + txCount;
    }
}

class TxBenchFailed extends Exception {
    public def this(message:String) {
        super(message);
    }
}

class TxBenchState(r:Long, u:Float, n:Long, p:Long, t:Long, w:Long, d:Long,
        h:Long, o:Long, g:Long, s:Long, f:Boolean, vp:String, vt:Long) {
    var virtualPlaceId:Long = -1;

    var thrds:Rail[ProducerThroughput];
    var rightTxBenchState:TxBenchState;
    var rightPlaceDeathTimeNS:Long = -1;

    var started:Boolean = false;
    var recovered:Boolean = false;

    var reducedTime:Long;
    var reducedTxCount:Long;

    val reduceSrc = new Rail[Long](2);
    val reduceDst = new Rail[Long](2);
    
    var iteration:Long = -1; //for the warmup iteration
    var lc:AtomicInteger;
    
    public def this() {
        property(-1, -1F, -1, -1, -1, -1, -1, -1, -1, -1, -1, true, null, -1);
    }
    
    public def this(virtualId:Long, r:Long, u:Float, n:Long, p:Long, t:Long, w:Long, d:Long,
        h:Long, o:Long, g:Long, s:Long, f:Boolean, vp:String, vt:Long) {
        property(r, u, n, p, t, w, d, h, o, g, s, f, vp, vt);
        thrds = new Rail[ProducerThroughput](t);//, (i:Long)=> new ProducerThroughput( virtualPlaceId, i));
        for (i in 0..(t-1))
            thrds(i) = new ProducerThroughput( virtualPlaceId, i);
        this.virtualPlaceId = virtualId;
        lc = new AtomicInteger(t as Int);
        if (w > 0)
            iteration = -1;
        else
            iteration = 0;
    }
    
    public def reset() {
        thrds = new Rail[ProducerThroughput](t, (i:Long)=> new ProducerThroughput( virtualPlaceId, i));
        rightTxBenchState = null;
        rightPlaceDeathTimeNS = -1;

        started = false;
        recovered = false;

        reducedTime = 0;
        reducedTxCount = 0;
        
        reduceSrc(0) = 0; reduceSrc(1) = 0;
        reduceDst(0) = 0; reduceDst(1) = 0;
        
        lc = new AtomicInteger(t as Int);
    }
    
    public def reinit(other:TxBenchState, iter:Long) {
        virtualPlaceId = other.virtualPlaceId;
        thrds = other.thrds;
        recovered = true;
        reduceSrc(0) = 0; reduceSrc(1) = 0;
        reduceDst(0) = 0; reduceDst(1) = 0;
        iteration = iter;
    }
    
    public def toString() {
        var str:String = "TxBenchState[Place"+virtualPlaceId+"] ";
        for (thrd in thrds) {
            str += "{" + thrd + "} ";
        }
        return str;
    }
    
    public def shiftElapsedTime(timeNS:Long) {
        for (t in thrds) {
            t.elapsedTimeNS += timeNS;
        }
    }
    
    public def setElapsedTime(timeNS:Long) {
        for (t in thrds) {
            t.elapsedTimeNS = timeNS;
        }
    }
    
    public def mergeCounts():Long {
        var sumCount:Long = 0;
        for (t in thrds) {
            sumCount+= t.txCount;
        }
        return sumCount;
    }
    
    public def mergeTimes():Long {
        var sumTimes:Long = 0;
        for (t in thrds) {
            sumTimes += t.elapsedTimeNS;
        }
        return sumTimes;
    }
}

