import x10.util.ArrayList;

import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.LockingTx;
import x10.util.resilient.localstore.LocalTx;
import x10.util.resilient.localstore.AbstractTx;
import x10.util.resilient.localstore.tx.FatalTransactionException;

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
import x10.util.concurrent.Lock;

public class STMBench {
    private static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
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
            Option("opt","optimized","optimized runs use fixed pre-defined members list (default 0 (not optimized))"),
            Option("s","spare","Spare places (default 0)"),
            Option("flat","flat","Flat transaction (default 0)"),
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
        val vt = opts("vt", "");
        val optimized = opts("opt", 0) == 1;
        val flat = opts("flat", 0) == 1;
        val f = opts("f", 1) == 1;
        val victimsList = new VictimsList(vp, vt);
        
        val mgr = new PlaceManager(s, false);
        val activePlaces = mgr.activePlaces();
        val p = opts("p", activePlaces.size());
        printRunConfigurations (r, u, n, p, t, w, d, h, o, g, s, f, optimized, flat);
        
        assert (h <= activePlaces.size()) : "invalid value for parameter h, h should not exceed the number of active places" ;

        if (TxConfig.get().LOCK_FREE) {
            assert (p * t == 1): "lock free mode can only be used with only 1 producer thread";
        }
        
        val throughputPLH = PlaceLocalHandle.make[PlaceThroughput](Place.places(), ()=> new PlaceThroughput(here.id, t) );
                
        val immediateRecovery = true;
        val store = ResilientStore.make[Long](activePlaces, immediateRecovery);
        val map = store.makeMap();
        
        val startWarmup = Timer.milliTime();
        if (w == -1) {
            Console.OUT.println("no warmpup");
        }
        else {
            Console.OUT.println("warmup started");
            runIteration(map, p, w, r, u, t, h, o, g, f, null, optimized, flat, throughputPLH, null);
            resetStatistics(map, throughputPLH);
            Console.OUT.println("warmup completed, warmup elapsed time ["+(Timer.milliTime() - startWarmup)+"]  ms ");
        }
        
        try {
            for (iter in 1..n) {
                val startIter = Timer.milliTime();
                var victim:VictimsList = victimsList;
                if (victim.onePerIteration)
                    victim = victimsList.getVictim(iter-1);
                else
                    victim = victimsList;
                runIteration(map, p, d, r, u, t, h, o, g, f, victim, optimized, flat, throughputPLH, null);
                Console.OUT.println("iteration:" + iter + " completed, iteration elapsedTime ["+(Timer.milliTime() - startIter)+"]  ms ");
                
                printThroughput(map, p, iter, throughputPLH, d, t, h, o);
                resetStatistics(map, throughputPLH);
            }
            
            Console.OUT.println("+++ STMBench Succeeded +++");
        }catch(ex:Exception) {
            Console.OUT.println("!!! STMBench Failed !!!");
            ex.printStackTrace();
        }
    }
    
    public static def runIteration(map:ResilientNativeMap[Long], producersCount:Long, 
            d:Long, r:Long, u:Float, t:Long, h:Long, o:Long, g:Long, f:Boolean, victims:VictimsList, optimized:Boolean,
            flat:Boolean, throughput:PlaceLocalHandle[PlaceThroughput], recoveryThroughput:PlaceThroughput) {
        val activePlaces = map.getActivePlaces();
        try {
            
            finish for (var i:Long = 0; i < producersCount; i++) {
                startPlace(activePlaces(i), map, activePlaces.size(), producersCount, d, r, u, t, h, o, g, f, victims, optimized, flat, throughput, recoveryThroughput);
            }
            
        } catch (e:STMBenchFailed) {
            throw e;
        } catch (mulExp:MultipleExceptions) {
            val stmFailed = mulExp.getExceptionsOfType[STMBenchFailed]();
            if ((stmFailed != null && stmFailed.size != 0) || !resilient)
                throw mulExp;
            mulExp.printStackTrace();
        } catch(e:Exception) {
            if (!resilient)
                throw e;
            e.printStackTrace();
        }
    }

    private static def startPlace(pl:Place, map:ResilientNativeMap[Long], activePlacesCount:Long, producersCount:Long, 
            d:Long, r:Long, u:Float, t:Long, h:Long, o:Long, g:Long, f:Boolean, victims:VictimsList, optimized:Boolean,
            flat:Boolean, throughput:PlaceLocalHandle[PlaceThroughput], recoveryThroughput:PlaceThroughput) {
        
        at (pl) async {
            val myVirtualPlaceId = map.getVirtualPlaceId();
            if (recoveryThroughput != null)
                throughput().reinit(recoveryThroughput);
            
            for (thrd in 1..t) async {
                produce(map, activePlacesCount, myVirtualPlaceId, producersCount, thrd-1, d, r, u, t, h, o, g, f, victims, optimized, flat, throughput);
            }
            
            if (resilient && victims != null) {
                val killTime = victims.getKillTime(here);
                if (killTime != -1) {
                    @Uncounted async {
                        Console.OUT.println("Hammer kill timer at "+here+" starting sleep for "+killTime+" secs");
                        val startedNS = System.nanoTime(); 
                        
                        val deadline = System.currentTimeMillis() + 1000 * killTime;
                        while (deadline > System.currentTimeMillis()) {
                            val sleepTime = deadline - System.currentTimeMillis();
                            System.sleep(sleepTime);
                        }
                        
                        val prev = map.plh().getMaster(here);
                        val myThroughput = throughput();
                        myThroughput.setElapsedTime(System.nanoTime() - startedNS);
                        val me = here;
                        at (prev) {
                            throughput().rightPlaceThroughput = myThroughput;
                            throughput().rightPlaceDeathTimeNS =  System.nanoTime();
                            Console.OUT.println(here + " Received suicide note from " + me + " througputValues: " + myThroughput);
                        }
                        
                        Console.OUT.println(here + " Hammer calling killHere" );
                        System.killHere();
                    }
                }
            }
        }
    }
    
    public static def produce(map:ResilientNativeMap[Long], activePlacesCount:Long, myVirtualPlaceId:Long, producersCount:Long, producerId:Long, 
            d:Long, r:Long, u:Float, t:Long, h:Long, o:Long, g:Long, f:Boolean, victims:VictimsList, optimized:Boolean, flat:Boolean,
            throughput:PlaceLocalHandle[PlaceThroughput]) {
        throughput().started = true;
        if (throughput().recovered) {
            Console.OUT.println(here + " Spare place started to produce transactions " + throughput().toString());
        }
        val rand = new Random((here.id+1) * t + producerId);
        val myThroughput = throughput().thrds(producerId);
        var timeNS:Long = myThroughput.elapsedTimeNS; // always 0 in non-resilient mode, non-zero for spare places in resilient mode

        //pre-allocate rails
        val virtualMembers = new Rail[Long](h);
        val keys = new Rail[Long] (h*o);
        val tmpKeys = new Rail[Long] (o);
        val values = new Rail[Long] (h*o);
        val readFlags = new Rail[Boolean] (h*o);
        
        while (timeNS < d*1e6) {
            val innerStr = nextTransactionMembers(rand, activePlacesCount, h, myVirtualPlaceId, virtualMembers, f);
            nextRandomOperations(rand, activePlacesCount, virtualMembers, r, u, o, keys, values, readFlags, tmpKeys);
            
            val distClosure = (tx:AbstractTx[Long]) => {
                for (var m:Long = 0; m < h; m++) {
                    val start = m*o;
                    val dest = virtualMembers(m);
                    tx.asyncAt(dest, () => {
                        for (var x:Long = 0; x < o; x++) {
                            val key = keys(start+x);
                            val read = readFlags(start+x);
                            val value = values(start+x);
                            //read? Console.OUT.println("Tx["+tx.id+"] STMBench.read("+key+") ") : Console.OUT.println("Tx["+tx.id+"] STMBench.write("+key+") ") ;  
                            read? tx.get(key): tx.put(key, new CloneableLong(value));
                        }
                    });
                }
                return null;
            };
            
            val localClosure = (tx:AbstractTx[Long]) => {
                for (var x:Long = 0; x < o; x++) {
                    val key = keys(x);
                    val read = readFlags(x);
                    val value = values(x);
                    read? tx.get(key): tx.put(key, new CloneableLong(value));
                }
                return null;
            };
            
            //time starts here
            val start = System.nanoTime();
            var includeTx:Boolean = true;
            if (virtualMembers.size > 1 && TxConfig.get().STM ) { //STM distributed
                val remainingTime =  (d*1e6 - timeNS) as Long;
                try {
                    if (optimized)
                        map.executeTransaction(virtualMembers, distClosure, -1, remainingTime);
                    else if (flat)
                    	 map.executeFlatTransaction(null, distClosure, -1, remainingTime);
                    else
                        map.executeTransaction(null, distClosure, -1, remainingTime);
                }catch(expf:FatalTransactionException) {
                    includeTx = false;
                }
            }
            else if (virtualMembers.size > 1 && TxConfig.get().LOCKING ) { //locking distributed
                map.executeLockingTransaction(virtualMembers, keys, readFlags, o, distClosure);
            }
            else if (virtualMembers.size == 1 && producersCount == 1 && TxConfig.get().STM ) { // STM local
                //local transaction
                assert (virtualMembers(0) == here.id) : "local transactions are not supported at remote places in this benchmark" ;
                map.executeLocalTransaction(localClosure, -1, -1);
            }
            else if (virtualMembers.size == 1 && producersCount == 1 && TxConfig.get().LOCKING ) { //Locking local
                assert (virtualMembers(0) == here.id) : "local transactions are not supported at remote places in this benchmark" ;
                map.executeLockingTransaction(virtualMembers, keys, readFlags, o, localClosure);
            }
            else if (virtualMembers.size == 1 && producersCount == 1 && TxConfig.get().BASELINE ) { //baseline local
                map.executeBaselineTransaction(localClosure);
            }
            else if (virtualMembers.size > 1 && TxConfig.get().BASELINE ) { //baseline distributed
                map.executeBaselineTransaction(distClosure);
            }
            else
                assert (false) : "wrong or unsupported configurations, members= " + virtualMembers.size;
            
            val elapsedNS = System.nanoTime() - start; 
            timeNS += elapsedNS;
            myThroughput.elapsedTimeNS = timeNS;
            
            if (includeTx) {
                myThroughput.txCount++;
                if (g != -1 && myThroughput.txCount%g == 0)
                    Console.OUT.println(here + " Progress "+myVirtualPlaceId+"x"+producerId + ":" + myThroughput.txCount );
            }
            
            val slaveChange = map.nextPlaceChange();
            if (resilient && producerId == 0 && slaveChange.changed) {
                val nextPlace = slaveChange.newSlave;
                if (throughput().rightPlaceDeathTimeNS == -1)
                    throw new STMBenchFailed(here + " assertion error, did not receive suicide note ...");
                val oldThroughput = throughput().rightPlaceThroughput;
                val recoveryTime = System.nanoTime() - throughput().rightPlaceDeathTimeNS;
                oldThroughput.shiftElapsedTime(recoveryTime);
                Console.OUT.println(here + " Calculated recovery time = " + (recoveryTime/ 1e9) + " seconds" );
                startPlace(nextPlace, map, activePlacesCount, producersCount, d, r, u, t, h, o, g, f, victims, optimized, flat, throughput, oldThroughput);
            }
        }
        
        //Console.OUT.println(here + "==FinalProgress==> txCount["+myThroughput.txCount+"] elapsedTime["+(myThroughput.elapsedTimeNS/1e9)+" seconds]");
    }

    public static def printThroughput(map:ResilientNativeMap[Long], producersCount:Long, iteration:Long, plh:PlaceLocalHandle[PlaceThroughput], d:Long, t:Long, h:Long, o:Long ) {
        map.printTxStatistics();
        
        Console.OUT.println("========================================================================");
        Console.OUT.println("Collecting throughput information ..... .....");
        Console.OUT.println("========================================================================");
        
        val activePlcs = map.getActivePlaces();
        val startReduce = System.nanoTime();
        val lc = GlobalRef (new Lock());
        val timeCountRail = GlobalRef(new Rail[Long](2));
        
        if (producersCount > 1) {
            finish for (p in activePlcs) async at (p) {
                val plcTh = plh();
                if (!plcTh.started)
                    throw new STMBenchFailed(here + " never started ...");
                
                at (lc) async {
                    lc().lock();
                    timeCountRail()(0) += plcTh.mergeTimes();
                    timeCountRail()(1) += plcTh.mergeCounts();
                    lc().unlock();
                }
            }
        }
        else {
            timeCountRail()(0) = plh().mergeTimes();
            timeCountRail()(1) = plh().mergeCounts();
        }
        val elapsedReduceNS = System.nanoTime() - startReduce;
        
        val allOperations = timeCountRail()(1) * h * o;
        val allTimeNS = timeCountRail()(0);
        val producers = producersCount * t;
        val throughput = (allOperations as Double) / (allTimeNS/1e6) * producers;
        Console.OUT.println("Reduction completed in "+((elapsedReduceNS)/1e9)+" seconds   txCount["+plh().reducedTxCount+"] OpCount["+allOperations+"]  timeNS["+plh().reducedTime+"]");
        Console.OUT.println("iteration:" + iteration + ":globalthroughput(op/MS):"+throughput);
        Console.OUT.println("========================================================================");
    }
    
    public static def resetStatistics(map:ResilientNativeMap[Long], plh:PlaceLocalHandle[PlaceThroughput]) {
        map.resetTxStatistics();
        Place.places().broadcastFlat(()=>{plh().reset();}, (p:Place)=>true);
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
            keys:Rail[Long], values:Rail[Long], readFlags:Rail[Boolean], tmpKeys:Rail[Long]) {
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
    
    /*********************  Structs ********************/
    static class VictimsList {
        private val places:Rail[Long];
        private val seconds:Rail[Long];
        private var onePerIteration:Boolean;
    
        public def this(place:Long, time:Long) {
            onePerIteration = true;
            places = new Rail[Long](1);
            seconds = new Rail[Long](1);
            places(0) = place;
            seconds(0) = time;
        }

        public def getVictim(i:Long) {
            if (i >= places.size)
                return null;
            return new VictimsList(places(i), seconds(i));
        }
        
        public def this(vp:String, vt:String) {
            if (vp != null && !vp.equals("")) {
                assert (resilient) : "assertion error, set X10_RESILIENT_MODE to a non-zero value";
                val tmp = vp.split(",");
                places = new Rail[Long](tmp.size);
                for (var i:Long = 0; i < tmp.size; i++) {
                    places(i) = Long.parseLong(tmp(i));
                }
            }
            else
                places = null;
            
            if (vt != null && !vt.equals("")) {
                assert (resilient) : "assertion error, set X10_RESILIENT_MODE to a non-zero value";
                if (vt.equals("-1") && places != null) {
                    onePerIteration = true;
                    seconds = new Rail[Long](places.size);
                    for (var i:Long = 0 ; i< places.size; i++) {
                        seconds(i) = 1;
                    }
                }
                else {
                    onePerIteration = false;
                    val tmp = vt.split(",");
                    seconds = new Rail[Long](tmp.size);
                    for (var i:Long = 0; i < tmp.size; i++) {
                        seconds(i) = Long.parseLong(tmp(i));
                    }
                }
            }
            else
                seconds = null;
            
            if (places != null){
                assert (seconds != null) : "assertion error, -vt is missing" ;
                assert (places.size == seconds.size) : "wrong victims configurations" ;
            }
        }
        
        public def getKillTime(p:Place) {
            if (places == null)
                return -1;
            for (var i:Long = 0; i < places.size; i++){
                if (p.id == places(i))
                    return seconds(i);
            }
            return -1;
        }
        
        public def size() {
            if (places == null)
                return 0;
            else
                return places.size;
        }
    }
    
    /***********************   Utils  *****************************/
    public static def printRunConfigurations(r:Long, u:Float, n:Long, p:Long, t:Long, w:Long, 
            d:Long, h:Long, o:Long, g:Long, s:Long, f:Boolean, opt:Boolean, flat:Boolean) {
        Console.OUT.println("STMBench starting with the following parameters:");        
        Console.OUT.println("X10_NPLACES="  + Place.numPlaces());
        Console.OUT.println("X10_NTHREADS=" + Runtime.NTHREADS);
        Console.OUT.println("X10_NUM_IMMEDIATE_THREADS=" + System.getenv("X10_NUM_IMMEDIATE_THREADS"));
        Console.OUT.println("X10_RESILIENT_MODE=" + x10.xrx.Runtime.RESILIENT_MODE);
        Console.OUT.println("TM=" + System.getenv("TM"));
        Console.OUT.println("LOCK_FREE=" + System.getenv("LOCK_FREE"));
        Console.OUT.println("DISABLE_INCR_PARALLELISM=" + System.getenv("DISABLE_INCR_PARALLELISM"));
        Console.OUT.println("X10_EXIT_BY_SIGKILL=" + System.getenv("X10_EXIT_BY_SIGKILL"));
        Console.OUT.println("DISABLE_SLAVE=" + System.getenv("DISABLE_SLAVE"));
        Console.OUT.println("IMM_AT=" + System.getenv("IMM_AT"));
        Console.OUT.println("ENABLE_STAT=" + System.getenv("ENABLE_STAT"));
        
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
        Console.OUT.println("opt(static members)=" + opt);
        Console.OUT.println("flat=" + flat);
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


class PlaceThroughput(threads:Long) {
    public var thrds:Rail[ProducerThroughput];
    public var rightPlaceThroughput:PlaceThroughput;
    public var rightPlaceDeathTimeNS:Long = -1;
    public var virtualPlaceId:Long;

    public var started:Boolean = false;
    public var recovered:Boolean = false;

    public var reducedTime:Long;
    public var reducedTxCount:Long;

    public def this(virtualPlaceId:Long, threads:Long) {
        property(threads);
        this.virtualPlaceId = virtualPlaceId;
        thrds = new Rail[ProducerThroughput](threads, (i:Long)=> new ProducerThroughput( virtualPlaceId, i));
    }
    
    public def reset() {
        thrds = new Rail[ProducerThroughput](threads, (i:Long)=> new ProducerThroughput( virtualPlaceId, i));
        rightPlaceThroughput = null;
        rightPlaceDeathTimeNS = -1;

        started = false;
        recovered = false;

        reducedTime = 0;
        reducedTxCount = 0;
    }
    
    public def reinit(other:PlaceThroughput) {
        virtualPlaceId = other.virtualPlaceId;
        thrds = other.thrds;
        recovered = true;
    }
    
    public def toString() {
        var str:String = "PlaceThroughput[Place"+virtualPlaceId+"] ";
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


class STMBenchFailed extends Exception {
    public def this(message:String) {
        super(message);
    }
}