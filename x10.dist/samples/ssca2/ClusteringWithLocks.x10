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
import x10.xrx.NonShrinkingApp;
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
public final class ClusteringWithLocks(plh:PlaceLocalHandle[ClusteringState]) implements NonShrinkingApp {  
    public static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    public static val asyncRecovery = true;
    
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
        if (verbose > 1n) {
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
                            atomic {
                                result().lockedLast.put(me, locked);
                                result().lockedLastTotal += locked.size();
                                result().total += locked.size();
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
        if (verbose > 1n) result().print(txId);
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
                at (Place(dest)) async {
                    for (k in keys) {
                        if (verbose > 1n) Console.OUT.println(here + " Tx["+tx.id+"] dest["+dest+"] key["+k+"]");
                        tx.unlockWrite(k);
                    }
                }
            }
        }
    }
    
    private def createCluster(store:TxStore, root:Int, placeId:Long, clusterId:Long, 
            plh:PlaceLocalHandle[ClusteringState], verbose:Int) {
        var success:Boolean = false;
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
                    //if the root is taken, end this iteration
                    return;
                }
                
                var nextV:Int = root;
                while (nextV != -1n) {
                    nextV = processVertex(nextV, placeId, clusterId, tx, plh, result);
                }
                success = true;
            } catch (ex:Exception) {
                success = false;
            }
            unlockObtainedKeys(result(), tx, success, verbose);
        }
    }
    
    private def execute(state:ClusteringState, placeId:Long, workerId:Long, start:Int, end:Int, 
            store:TxStore, plh:PlaceLocalHandle[ClusteringState], verbose:Int) {
        Console.OUT.println(here + " worder["+workerId+"] starting: " + start + "-" + (end-1));
        // Iterate over each of the vertices in my portion.
        var clusterId:Long = 1;
        for(var vertexIndex:Int=start; vertexIndex<end; ++vertexIndex, ++clusterId) { 
            val s:Int = state.verticesToWorkOn(vertexIndex);
            createCluster(store, s, placeId, clusterId, plh, verbose);
        }
    }
    
    /**
     * Dump the clusters.
     */
    public def startPlace(place:Place, store:TxStore, recovery:Boolean) {
        val plh = this.plh; //don't copy this
        
        at (place) @Uncounted async {
            val placeId = store.plh().getVirtualPlaceId();
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
                    async execute(state, placeId, workerId, start as Int, end as Int, store, plh, verbose);
                }
            }
            
            Console.OUT.println(here + " ==> completed successfully ");
            at (Place(0)) @Uncounted async {
                val p0state = plh();
                p0state.notifyTermination(null);
            }
        }
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
    
    public def run(store:TxStore, plh:PlaceLocalHandle[ClusteringState]) {
        val activePlaces = store.fixAndGetActivePlaces();
        try {
            val state = plh();
            state.p0Cnt = activePlaces.size();
            state.p0Latch = new SimpleLatch();
            
            for (var i:Long = 0; i < state.places; i++) {
                startPlace(activePlaces(i), store, false);
            }
            state.p0Latch.await();
            
            if (state.p0Excs != null)
                throw new MultipleExceptions(state.p0Excs);
            
        } catch (mulExp:MultipleExceptions) {
            /*val stmFailed = mulExp.getExceptionsOfType[TxBenchFailed]();
            if ((stmFailed != null && stmFailed.size != 0) || !resilient)
                throw mulExp;*/
            mulExp.printStackTrace();
        } catch(e:Exception) {
            if (!resilient)
                throw e;
            e.printStackTrace();
        }
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
            Option("p", "", "Permutation"),
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
        val g:Long = cmdLineParams("-g", -1);
        val r:Long = cmdLineParams("-r", 0);
        val permute:Int = cmdLineParams("-p", 1n); // on by default
        val verbose:Int = cmdLineParams("-v", 0n); // off by default
        val vp = cmdLineParams("vp", "");
        val vt = cmdLineParams("vt", "");
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
        Console.OUT.println("p = " + permute);
        
        if (System.getenv("TM") == null || !System.getenv("TM").equals("locking")) {
            Console.OUT.println("!!!!ERROR: you must set TM=locking in this program!!!!");
            return;
        }
        
        var time:Long = System.nanoTime();
        
        val mgr = new PlaceManager(spare, false);
        val activePlaces = mgr.activePlaces();
        val rmat = Rmat(seed, n, a, b, c, d);
        val places = activePlaces.size();
        
        val graph = rmat.generate();
        if (verbose > 0) {
            Console.OUT.println(graph.toString());
        }
        graph.compress();
        val N = graph.numVertices();
        val verticesToWorkOn = new Rail[Int](N, (i:Long)=>i as Int);
        if (permute > 0n)
            permuteVertices(N, verticesToWorkOn);
        
        val plh = PlaceLocalHandle.make[ClusteringState](Place.places(), ()=>new ClusteringState(graph, verticesToWorkOn, places, workers, verbose, clusterSize, g, vp, vt));
        
        val app = new ClusteringWithLocks(plh);
        val store = TxStore.make(activePlaces, asyncRecovery, app);
        Console.OUT.println("spare places = " + spare);
        Console.OUT.println("active places = " + activePlaces.size());
        
        val distTime = (System.nanoTime()-time)/1e9;
        
        time = System.nanoTime();
        app.run(store, plh);
        time = System.nanoTime() - time;
        
        val procTime = time/1E9;
        val totalTime = distTime + procTime;
        val procPct = procTime*100.0/totalTime;

        if(verbose > 2 || r > 0) 
            app.printClusters(store);

        Console.OUT.println("Places: " + places + "  N: " + plh().N + "  Setup: " + distTime + "s  Processing: " + procTime + "s  Total: " + totalTime + "s  (proc: " + procPct  +  "%).");
    }
    
    /**
     * A function to shuffle the vertices randomly to give better work dist.
     */
    private static def permuteVertices(N:Int, verticesToWorkOn:Rail[Int]) {
        val prng = new Random(1);

        for(var i:Int=0n; i<N; i++) {
            val indexToPick = prng.nextInt(N-i);
            val v = verticesToWorkOn(i);
            verticesToWorkOn(i) = verticesToWorkOn(i+indexToPick);
            verticesToWorkOn(i+indexToPick) = v;
        }
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
    
    var p0Cnt:Long;
    var p0Latch:SimpleLatch = null;
    var p0Excs:GrowableRail[CheckedThrowable] = null;
    
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
    
    public def notifyTermination(ex:CheckedThrowable) {
        var lc:Long = -1;
        p0Latch.lock();
        if (ex != null) {
            if (p0Excs == null)
                p0Excs = new GrowableRail[CheckedThrowable]();
        }
        lc = --p0Cnt;
        p0Latch.unlock();
        if (lc == 0)
            p0Latch.release();
    }
    
}