import x10.compiler.Pragma;
import x10.util.Random;
import x10.util.OptionsParser;
import x10.util.Option;
import x10.util.Team;
import x10.util.concurrent.Lock;

import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.TxConfig;
import x10.xrx.TxStoreFatalException;
import x10.util.resilient.localstore.Cloneable;
import x10.xrx.TxStoreConflictException;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.xrx.MasterWorkerApp;
import x10.xrx.MasterWorkerExecutor;
import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.xrx.TxLocking;
import x10.xrx.Runtime;
import x10.compiler.Uncounted;
import x10.util.concurrent.SimpleLatch;
import x10.util.concurrent.AtomicInteger;
import x10.util.GrowableRail;

/* 
Assumptions:
 - graph generated using the R-MAT algorithm
 - all edge costs are considered equal
To use locking, we needed to handle the following:
 - avoid locking the same key twice by remembering all acquired keys
 - maintain the list of acquired keys in the application to unlock them
*/
public final class ClusteringWithLocks(plh:PlaceLocalHandle[ClusteringState]) implements MasterWorkerApp {
    public static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public static struct Color implements Cloneable {
        public val placeId:Long;
        public val clusterId:Long;
    
        def this(p:Long, c:Long) {
            placeId = p;
            clusterId = c;
        }
        
        public def clone() = new Color(placeId, clusterId);
    };

    static class Result {
        val locked:HashMap[Long,HashSet[Int]] = new HashMap[Long,HashSet[Int]]();
        val lockedLast:HashMap[Long,HashSet[Int]] = new HashMap[Long,HashSet[Int]]();
        var lockedLastTotal:Long = 0;
        var total:Long = 0;
    
        public def merge() {
            for (e in lockedLast.entries()) {
                var set:HashSet[Int] = locked.getOrElse(e.getKey(), null);
                if (set == null)
                    set = new HashSet[Int]();
                set.addAll(e.getValue());
                locked.put(e.getKey(), set);
            }
            lockedLast.clear();
            lockedLastTotal = 0;    
        }
        
        public def print(txId:Long) {
            var str:String = "Tx["+txId+"] result -> ";
            for (e in locked.entries()) {
                str += " dst["+e.getKey()+"] values { " ;
                for (v in e.getValue()) {
                    str += v + ",";
                }
                str += " }";
            }
            Console.OUT.println(str);
        }
    }
    
    private def getAdjacentVertecesPlaces(v:Int, edgeStart:Int, edgeEnd:Int, verticesPerPlace:Long, 
            graph:Graph, result:GlobalRef[Result{self!=null}]{result.home==here}) {
        val map = new HashMap[Long,HashSet[Int]]();
        var dest:Long = v / verticesPerPlace;
        var set:HashSet[Int] = null;
        
        // Iterate over all its neighbors
        for(var wIndex:Int=edgeStart; wIndex<edgeEnd; ++wIndex) {
            // Get the target of the current edge.
            val w:Int = graph.getAdjacentVertexFromIndex(wIndex);
            dest = w / verticesPerPlace;
        
            //in the locking implementation, we need to ensure that 
            //we didn't lock it before
            val k = result().locked.getOrElse(dest, null);
            if (k == null || !k.contains(w)) {
                set = map.getOrElse(dest, null);
                if (set == null) {
                    set = new HashSet[Int]();
                    map.put(dest, set);
                }
                set.add(w);
            }
        }        
        return map;
    }
    
    private def printVertexPlaceMap(v:Int, map:HashMap[Long,HashSet[Int]], txId:Long) {
        var str:String = "Tx["+txId+"] vertexMap v=" + v + ":";
        val iter = map.keySet().iterator();
        while (iter.hasNext()) {
            val pl = iter.next();
            str += " place(" + pl + ") {";
            val set = map.getOrThrow(pl);
            for (l in set) {
                str += l + " ";
            }
            str += "}";
        }
        Console.OUT.println(str);
    } 
    
    //create a cluster rooted by vertic v. If v is taken, return immediately.
    //otherwise, look v and all its adjacent vertices
    //don't overwrite already taken keys
    private def processVertex(v:Int, placeId:Long, clusterId:Long, tx:TxLocking, 
            plh:PlaceLocalHandle[ClusteringState], result:GlobalRef[Result{self!=null}]{result.home==here}):Int {
        val state = plh();
        val graph = state.graph;
        val random = state.random;
        val verbose = state.verbose;
        // Get the start and the end points for the edge list for "v"
        val edgeStart:Int = graph.begin(v);
        val edgeEnd:Int = graph.end(v);
        val edgeCount:Int = edgeEnd-edgeStart ;
        val verticesPerPlace = state.verticesPerPlace;
        
        //get the adjacent vertices to v (excluding v)
        val map = getAdjacentVertecesPlaces(v, edgeStart, edgeEnd, verticesPerPlace, graph, result);
        if (verbose > 2n) {
            printVertexPlaceMap(v, map, tx.id);
        }
        
        //occupy the adjacent vertices that are not occupied
        finish {
            val iter = map.keySet().iterator();
            while (iter.hasNext()) {
                val dest = iter.next();
                val vertices = map.getOrThrow(dest);
                tx.asyncAt(dest, () => {
                    var success:Boolean = true;
                    val locked = new HashSet[Int]();
                    var failedV:Int = 0n;
                    for (s in vertices) {
                        if (!tx.tryLockWrite(s)) {
                            success = false;
                            failedV = s;
                            if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] failed to lock ["+s+"] ");
                            break;
                        }
                        val color = tx.get(s);
                        if (color == null) {
                            tx.put(s, new Color(placeId, clusterId));
                            locked.add(s);
                            if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] locked ["+s+"] ");
                        } else {
                            tx.unlockWrite(s);
                        }
                    }
                    if (!success) {
                        for (s in locked) {
                            tx.put(s, null);
                            tx.unlockWrite(s);
                        }
                        throw new TxStoreConflictException("["+here+"] failed to lock key["+failedV+"]", here);
                    } else {
                        val me = here.id;
                        at (result) async {
                            if (locked.size() > 0 ) {
                                atomic {
                                    result().lockedLast.put(me, locked);
                                    result().lockedLastTotal += locked.size();
                                    result().total += locked.size();
                                }
                            }
                        }
                    }
                });
            }
        }
        if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] size["+result().total+"] sizeLast["
                + result().lockedLastTotal+"] target[" + state.clusterSize 
                + "] edgeCount["+edgeCount+"] oldMapSize["+map.size()+"] ");

        return chooseNextVertex(state.clusterSize, state.random, state.verbose, result, tx.id);
    }
            
    private def chooseNextVertex(clusterSize:Long, random:Random, verbose:Int, 
            result:GlobalRef[Result{self!=null}]{result.home==here}, txId:Long):Int {
        var nextV:Int = -1n;
        if (result().total < clusterSize && result().lockedLastTotal > 0) {
            //pick the next vertex from the vertices locked last
            val plIndx = Math.abs(random.nextInt()) % result().lockedLast.size();
            var i:Long = 0;
            for (e in result().lockedLast.entries()) {
                if (i == plIndx) {
                    val tmpLocked = e.getValue();
                    val indx = Math.abs(random.nextInt()) % tmpLocked.size(); //we pick a random adjacent node
                    var m:Long = 0;
                    for (x in tmpLocked) {
                        if (m == indx) {
                            nextV = x;
                            break;
                        }
                        m++;
                    }
                    break;
                }
                i++;
            }
        } 
        result().merge();
        return nextV;
    }
    
    private def unlockObtainedKeys(result:Result, tx:TxLocking, success:Boolean, verbose:Int) {
        result.merge();
        if (verbose > 1n) Console.OUT.println(here + " Tx["+tx.id+"] unlock all start  success["+success+"]");
        if (verbose > 1n) result.print(tx.id);
        //unlock all obtained locks and try again
        finish {
            for (e in result.locked.entries()) {
                val dest = e.getKey(); //physical key
                val keys = e.getValue();
                var str:String = "";
                for (tmp in keys) {
                    str += tmp + "-";
                }
                if (verbose > 1n) Console.OUT.println(here + " Tx["+tx.id+"] dest["+dest+"] keys["+str+"]");
                if (Place(dest).id == here.id) {
                    for (k in keys) {
                        if (verbose > 1n) Console.OUT.println(here + " Tx["+tx.id+"] dest["+dest+"] key["+k+"]");
                        tx.unlockWrite(k);
                    }
                } else {
                    at (Place(dest)) async {
                        for (k in keys) {
                            if (verbose > 1n) Console.OUT.println(here + " Tx["+tx.id+"] dest["+dest+"] key["+k+"]");
                            tx.unlockWrite(k);
                        }
                    }
                }
            }
        }
    }
    
    private def createCluster(store:TxStore, root:Int, placeId:Long, clusterId:Long, 
            plh:PlaceLocalHandle[ClusteringState], verbose:Int):Long {
        var success:Boolean = false;
        var retryCount:Long = 0;
        while (!success) {
            val result = GlobalRef(new Result());
            val tx = store.makeLockingTx();
            try {
                //handle the root
                if (!tx.tryLockWrite(root))
                    throw new TxStoreConflictException("Failed to lock root ["+root+"]", here); // retry
                
                val color = tx.get(root);
                if (color == null) {
                    //if the root is not taken, take it
                    tx.put(root, new Color(placeId, clusterId));
                    val k = new HashSet[Int]();
                    k.add(root);
                    result().locked.put(here.id, k);
                    result().total = 1;
                    if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] locked root ["+root+"] ");
                } else {
                    tx.unlockWrite(root); 
                    return retryCount; //if the root is taken, end this iteration
                }
                var nextV:Int = root;
                while (nextV != -1n) {
                    nextV = processVertex(nextV, placeId, clusterId, tx, plh, result);
                }
                success = true;
                if (verbose > 1n) result().print(tx.id);
            } catch (ex:Exception) {
                success = false;
            }
            unlockObtainedKeys(result(), tx, success, verbose);
            retryCount++;
        }
        return retryCount;
    }
    
    private def execute(state:ClusteringState, placeId:Long, workerId:Long, start:Int, end:Int, 
            store:TxStore, plh:PlaceLocalHandle[ClusteringState], verbose:Int) {
        val time0 = System.currentTimeMillis();
        var totalFailedRetries:Long = 0;
        val all = end - start;
        Console.OUT.println(here + ":worker:"+workerId+":from:" + start + ":to:" + (end-1));
        // Iterate over each of the vertices in my portion.
        var c:Long = 1;
        for(var vertexIndex:Int=start; vertexIndex<end; ++vertexIndex, ++c) { 
            val s:Int = state.verticesToWorkOn(vertexIndex);
            val clusterId = vertexIndex;
            totalFailedRetries += createCluster(store, s, placeId, clusterId, plh, verbose);
            if (state.g > -1 && clusterId % state.g == 0) {
                printProgress(here + ":worker:"+workerId+":progress -> " + c + "/" + all , time0);
            }
        }
        Console.OUT.println(here + ":worker:"+workerId+":from:" + start + ":to:" + (end-1)+":totalRetries:"+totalFailedRetries);
        state.addRetries(totalFailedRetries);
    }
    
    private def printProgress(message:String, time0:Long) {
        val time = System.currentTimeMillis();
        val s = "        " + (time - time0);
        val s1 = s.substring(s.length() - 9n, s.length() - 3n);
        val s2 = s.substring(s.length() - 3n, s.length());
        Console.OUT.println(s1 + "." + s2 + ": " + message);
        return time;
    }
    
    public def execWorker(vid:Long, store:TxStore, recovery:Boolean):Any {
        val placeId = vid;
        val state = plh();
        val N = state.N;
        val max = state.places;
        val g = state.g;
        val verbose = state.verbose;
        val verticesPerPlace = state.verticesPerPlace;
        val verticesPerWorker = Math.ceil((state.verticesPerPlace as Double)/state.workersPerPlace) as Long;
        val startVertex = (N as Long*placeId/max) as Int;
        val endVertex = (N as Long*(placeId+1)/max) as Int;
        
        Console.OUT.println(here + " starting: " + startVertex + "-" + (endVertex-1));
        
        finish {
            for (workerId in 1..state.workersPerPlace) {
                val start = startVertex + (workerId-1) * verticesPerWorker;
                val end = start + verticesPerWorker;
                val end2 = end >= state.verticesToWorkOn.size ? state.verticesToWorkOn.size : end;
                async execute(state, placeId, workerId, start as Int, end2 as Int, store, plh, verbose);
            }
        }
        val totalRetries = state.totalRetries;
        Console.OUT.println(here + " ==> completed successfully ");
        return totalRetries;
    }
    
    /**
     * Dump the clusters.
     */
    private def printClusters(store:TxStore) {
        val activePlaces = store.fixAndGetActivePlaces();
        val places = activePlaces.size();
        Console.OUT.println("==============  Collecting clusters  ===============");
        val map = new HashMap[Long,ArrayList[Int]]();
        store.executeLockingTx(new Rail[Long](0),new Rail[Any](0), new Rail[Boolean](0), 0, (tx:TxLocking) => {
            for (var tmpP:Long = 0; tmpP < places; tmpP++) {
                val localMap = at (activePlaces(tmpP)) {
                    val setIter = tx.keySet().iterator();                    
                    val pMap = new HashMap[Long,ArrayList[Int]]();
                    while (setIter.hasNext()) {
                        val vertex = setIter.next();
                        val cl = tx.get(vertex);                        
                        if (cl != null) {
                            val clusterId = ((cl as Color).placeId+1) * 1000000 + (cl as Color).clusterId;
                            var tmpList:ArrayList[Int] = pMap.getOrElse(clusterId, null);
                            if (tmpList == null) {
                                tmpList = new ArrayList[Int]();
                                pMap.put (clusterId, tmpList);
                            }
                            tmpList.add(vertex as Int);
                        }
                    }
                    pMap
                };
                
                val iter = localMap.keySet().iterator();
                while (iter.hasNext()) {
                    val pl = iter.next();
                    var list:ArrayList[Int] = map.getOrElse(pl, null);
                    if (list == null) {
                        list = new ArrayList[Int]();
                        map.put (pl, list);
                    }
                    val localList = localMap.getOrThrow(pl);
                    list.addAll(localList);
                }
            }
        });
        
        val mergedMap = map;
        var pointsInClusters:Long = 0;
        val iter = mergedMap.keySet().iterator();
        while (iter.hasNext()) {
            val key = iter.next();
            val list = mergedMap.getOrThrow(key);
            var str:String = "";
            for (x in list) {
                str += x + " ";
                pointsInClusters++;
            }
            Console.OUT.println("Cluster " + key + " = { " + str + " }");
        }
        Console.OUT.println("Points in clusters = " + pointsInClusters);
    }
       
    /**
     * Reads in all the options and calculate clusters.
     */
    public static def main(args:Rail[String]):void {
        val cmdLineParams = new OptionsParser(args, new Rail[Option](0L), [
            Option("sp", "", "Spare places"),
            Option("cs", "", "Cluster size"),
            Option("s", "", "Seed for the random number"),
            Option("n", "", "Number of vertices = 2^n"),
            Option("a", "", "Probability a"),
            Option("b", "", "Probability b"),
            Option("c", "", "Probability c"),
            Option("d", "", "Probability d"),
            Option("g", "", "Progress"),
            Option("r", "", "Print resulting clusters"),
            Option("w", "", "Workers"),
            Option("vp","victims","victim places to kill (comma separated)"),
            Option("vt","victimsTimes","times to kill victim places(comma separated)"),
            Option("v", "", "Verbose")]);

        val spare:Long = cmdLineParams("-sp", 0);
        val clusterSize:Long = cmdLineParams("-cs", 10);
        val seed:Long = cmdLineParams("-s", 2);
        val n:Int = cmdLineParams("-n", 2n);
        val a:Double = cmdLineParams("-a", 0.55);
        val b:Double = cmdLineParams("-b", 0.1);
        val c:Double = cmdLineParams("-c", 0.1);
        val d:Double = cmdLineParams("-d", 0.25);
        val g:Long = cmdLineParams("-g", -1); // progress
        val r:Long = cmdLineParams("-r", 0);
        val verbose:Int = cmdLineParams("-v", 0n); // off by default
        val vp = cmdLineParams("vp", ""); //victim places
        val vt = cmdLineParams("vt", ""); //victim kill time or progress
        val workers:Int = cmdLineParams("-w", Runtime.NTHREADS); 
        
        Console.OUT.println("Running SSCA2 with the following parameters:");
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
        
        Console.OUT.println("clusterSize = " + clusterSize);
        Console.OUT.println("seed = " + seed);
        Console.OUT.println("workers = " + workers);
        Console.OUT.println("N = " + (1<<n));
        Console.OUT.println("a = " + a);
        Console.OUT.println("b = " + b);
        Console.OUT.println("c = " + c);
        Console.OUT.println("d = " + d);
        
        if (System.getenv("TM") == null || !System.getenv("TM").equals("locking")) {
            Console.OUT.println("!!!!ERROR: you must set TM=locking in this program!!!!");
            return;
        }
        
        var time:Long = System.nanoTime();
        
        val mgr = new PlaceManager(spare, false);
        val activePlaces = mgr.activePlaces();
        val places = activePlaces.size();
        Console.OUT.println("spare places = " + spare);
        Console.OUT.println("active places = " + places);
                
        val rmat = Rmat(seed, n, a, b, c, d);
        val graph = rmat.generate();
        if (verbose > 0) {
            Console.OUT.println(graph.toString());
        }
        graph.compress();
        val N = graph.numVertices();
        val verticesToWorkOn = new Rail[Int](N, (i:Long)=>i as Int);

        val plh = PlaceLocalHandle.make[ClusteringState](Place.places(), ()=>new ClusteringState(graph, verticesToWorkOn, places, workers, verbose, clusterSize, g, vp, vt));
        val app = new ClusteringWithLocks(plh);
        val executor = MasterWorkerExecutor.make(activePlaces, app);
        
        val distTime = (System.nanoTime()-time)/1e9;
        
        time = System.nanoTime();
        executor.run();
        time = System.nanoTime() - time;
        
        val results = executor.workerResults();
        var retries:Long = 0;
        for (entry in results.entries()) {
            retries += entry.getValue() as Long;
        }
        val procTime = time/1E9;
        val totalTime = distTime + procTime;
        val procPct = procTime*100.0/totalTime;
        if(verbose > 2 || r > 0) 
            app.printClusters(executor.store());

        Console.OUT.println("Places:" + places + ":N:" + plh().N + ":SetupInSeconds:" + distTime + ":ProcessingInSeconds:" + procTime + ":TotalInSeconds:" + totalTime + ":retries:"+retries+":(proc:" + procPct  + "%).");
    }
    
}

class ClusteringState(N:Int) {
    val graph:Graph;
    val verbose:Int;
    val verticesToWorkOn:Rail[Int];// = new Rail[Int](N, (i:Long)=>i as Int);
    val places:Long;
    val workersPerPlace:Long;
    val verticesPerPlace:Long;
    val random:Random;
    val clusterSize:Long;
    val g:Long;
    
    val vt:String;
    val vp:String;

    var totalRetries:Long = 0;
    
    public def this(graph:Graph, verticesToWorkOn:Rail[Int], places:Long, workersPerPlace:Long,
            verbose:Int, clusterSize:Long, g:Long, vp:String, vt:String) {
        property(graph.numVertices());
        this.graph = graph;
        this.places = places;
        this.workersPerPlace = workersPerPlace;
        this.verbose = verbose;
        this.verticesPerPlace = Math.ceil((graph.numVertices() as Double)/places) as Long;
        this.random = new Random(here.id);
        this.clusterSize = clusterSize;
        this.g = g;
        this.vp = vp;
        this.vt = vt;
        this.verticesToWorkOn = verticesToWorkOn;
    }
    
    public def addRetries(r:Long) {
        atomic { totalRetries += r; }
    }
}