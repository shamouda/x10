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

//assumptions:
//graph generated using the R-MAT algorithm
//all edge costs are considered equal
public final class Clustering(plh:PlaceLocalHandle[ClusteringState]) implements NonShrinkingApp {
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
        val locked = new HashSet[Int]();
        val lockedLast = new HashSet[Int]();
    
        public def merge() {
            locked.addAll(lockedLast);
            lockedLast.clear();
        }
        
        public def print(txId:Long) {
            var str:String = "Tx["+txId+"] result -> ";
            for (e in locked) {
                str += e + "," ;
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
            if (!result().locked.contains(w)) {
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
    private def processVertex(v:Int, placeId:Long, clusterId:Long, tx:Tx,
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
        //val subTx = Tx.makeSubTx(tx);
        finish {
            //Runtime.registerFinishTx(subTx);
            val iter = map.keySet().iterator();
            while (iter.hasNext()) {
                val dest = iter.next();
                val vertices = map.getOrThrow(dest);
                tx.asyncAt(dest, () => {
  				    val locked = new HashSet[Int]();
                    for (s in vertices) {
                        val color = tx.get(s);
                        if (color == null) {
                            tx.put(s, new Color(placeId, clusterId));
	                        locked.add(s);
                        }
                    }
                    val me = here.id;
                    at (result) async {
                        atomic {
                            result().lockedLast.addAll(locked);
                        }
                    }
                });
            }
        }
        
        if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] size["+result().locked.size()+"] sizeLast["+result().lockedLast.size()+"] target[" + state.clusterSize 
                + "] edgeCount["+edgeCount+"] oldMapSize["+map.size()+"] ");

        return chooseNextVertex(state.clusterSize, state.random, state.verbose, result, tx.id);
    }
            
    private def chooseNextVertex(clusterSize:Long, random:Random, verbose:Int, 
            result:GlobalRef[Result{self!=null}]{result.home==here}, txId:Long):Int {
        var nextV:Int = -1n;
        val total = result().locked.size() + result().lockedLast.size();
        if (total < clusterSize && result().lockedLast.size() > 0) {
            //pick the next vertex from the vertices locked last
            val indx = Math.abs(random.nextInt()) % result().lockedLast.size();
            var i:Long = 0;
            for (x in result().lockedLast) {
                if (i == indx) {
                    nextV = x;
                    break;
                }
                i++;
            }
        }
        result().merge();
        if (verbose > 1n) result().print(txId);
        return nextV;
    }

    private def createCluster(store:TxStore, tx:Tx, root:Int, placeId:Long, clusterId:Long, 
            plh:PlaceLocalHandle[ClusteringState], verbose:Int) {
        val result = GlobalRef(new Result());
        val color = tx.get(root);
        if (color == null) {
            //if the root is not taken, take it
            tx.put(root, new Color(placeId, clusterId));
            result().locked.add(root);
            if (verbose > 1n) Console.OUT.println(here + " cluster["+clusterId+"] locked root ["+root+"] ");
        } else {
            //if the root is taken, end this iteration
            return;
        }
        var nextV:Int = root;
        while (nextV != -1n) {
            nextV = processVertex(nextV, placeId, clusterId, tx, plh, result);
        }
    }

    private def execute(store:TxStore, state:ClusteringState, placeId:Long, workerId:Long, start:Int, end:Int, 
            plh:PlaceLocalHandle[ClusteringState], verbose:Int) {
        Console.OUT.println(here + " worder["+workerId+"] starting: " + start + "-" + (end-1));
        // Iterate over each of the vertices in my portion.
        var c:Long = 1;
        for(var vertexIndex:Int=start; vertexIndex<end; ++vertexIndex, ++c) { 
            val s:Int = state.verticesToWorkOn(vertexIndex);
            val clusterId = c;
            val closure = (tx:Tx) => {
                createCluster(store, tx, s, placeId, clusterId, plh, verbose);
            };
            store.executeTransaction(closure);
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
            
            /** Scheduler a hammer activity to kill the place at the specified time **/
            if (resilient && state.vp != null && state.vt != null) {
                val arr = state.vp.split(",");
                var indx:Long = -1;
                for (var c:Long = 0; c < arr.size ; c++) {
                    if (Long.parseLong(arr(c)) == here.id) {
                        indx = c;
                        break;
                    }
                }
                if (indx != -1) {
                    val times = state.vt.split(",");
                    val killTime = Long.parseLong(times(indx));
                    @Uncounted async {
                        Console.OUT.println("Hammer kill timer at "+here+" starting sleep for "+killTime+" secs");
                        val startedNS = System.nanoTime(); 
                        
                        val deadline = System.currentTimeMillis() + 1000 * killTime;
                        while (deadline > System.currentTimeMillis()) {
                            val sleepTime = deadline - System.currentTimeMillis();
                            System.sleep(sleepTime);
                        }
                        Console.OUT.println(here + " Hammer calling killHere" );
                        System.killHere();
                    }
                }
            }
            
            finish {
                for (workerId in 1..state.workersPerPlace) {
                    val start = startVertex + (workerId-1) * verticesPerWorker;
                    val end = start + verticesPerWorker;
                    async execute(store, state, placeId, workerId, start as Int, end as Int, plh, verbose);
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
        
        if (!TxConfig.BUSY_LOCK) {
            Console.OUT.println("!!!!ERROR: you must set BUSY_LOCK=1 in this program!!!!");
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
        
        val app = new Clustering(plh);
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