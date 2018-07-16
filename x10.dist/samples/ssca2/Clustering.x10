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

    private def getAdjacentVertecesPlaces(v:Int, edgeStart:Int, edgeEnd:Int, verticesPerPlace:Long, graph:Graph) {
        val map = new HashMap[Long,HashSet[Int]]();
        var dest:Long = v / verticesPerPlace;
        var set:HashSet[Int] = null;// = new HashSet[Int]();
        //set.add(v);
        //map.put (dest, set);
        
        // Iterate over all its neighbors
        for(var wIndex:Int=edgeStart; wIndex<edgeEnd; ++wIndex) {
            // Get the target of the current edge.
            val w:Int = graph.getAdjacentVertexFromIndex(wIndex);
            dest = w / verticesPerPlace;
        
            set = map.getOrElse(dest, null);
            if (set == null) {
                set = new HashSet[Int]();
                map.put(dest, set);
            }
            set.add(w);
        }        
        return map;
    }
    
    private def printVertexPlaceMap(v:Int, map:HashMap[Long,HashSet[Int]]) {
        var str:String = "vertexMap v=" + v + ":";
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
    //otherwise, look v and all its adjacent vertices (what if they are already taken?  don't overwrite them)
    private def processVertex(v:Int, placeId:Long, clusterId:Long, tx:Tx, accum:Long, plh:PlaceLocalHandle[ClusteringState]):Long {
        //check the root of the cluster
        val color = tx.get(v);
        if (color == null) {
            //if the root is not taken, take it
            tx.put(v, new Color(placeId, clusterId));
        } else {
            //if the root is taken, end this iteration
            return 0;
        }
        
        val state = plh();
        val graph = state.graph;
        val random = state.random;
        val verbose = state.verbose;
        // Get the start and the end points for the edge list for "v"
        val edgeStart:Int = graph.begin(v);
        val edgeEnd:Int = graph.end(v);
        val edgeCount:Int = edgeEnd-edgeStart ;
        val verticesPerPlace = state.verticesPerPlace;
        
        var innerCount:Long = 0;
        var outerCount:Long = 0;
        
        //get the adjacent vertices to v (and exclude v)
        val map = getAdjacentVertecesPlaces(v, edgeStart, edgeEnd, verticesPerPlace, graph);
        if (verbose > 1n) {
            printVertexPlaceMap(v, map);
        }
        //occupy the adjacent vertices that are not occupied
        val iter = map.keySet().iterator();
        { 
            while (iter.hasNext()) {
                val dest = iter.next();
                val vertices = map.getOrThrow(dest);
                innerCount += vertices.size();
                
                //Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"].asyncAt("+dest+") started");
                if (verbose > 1n) Console.OUT.println(here + " tx["+tx.id+"].asyncAt("+dest+") started");
                tx.asyncAt(dest, () => {
                    val used = new HashSet[Int]();
                    for (s in vertices) {
                        if (used.contains(s))
                            continue;
                        //Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"] get("+s+") started <<");
                        val color = tx.get(s);
                        //Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"] get("+s+") ended >>");
                        if (color == null) {
                            try {
                          //      Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"] put("+s+") started <<");
                                tx.put(s, new Color(placeId, clusterId));
                                
                            } finally {
                           //     Console.OUT.println("Tx["+tx.id+"] " + TxConfig.txIdToString (tx.id)+ " here["+here+"] put("+s+") ended >>");
                            }
                        }
                        used.add(s);
                        //else if (! ((color as Color).placeId == placeId && (color as Color).clusterId == clusterId ) )
                        //    throw new ConflictException("Tx[" + tx.id + "] " + here + " vertex " + s + " allocated by place " + (color as Color).placeId, here);
                    }
                });
            }
        }
        
        outerCount = accum + innerCount;
        
        if (verbose > 1n) Console.OUT.println(here + " tx["+tx.id+"] edgeCount="+edgeCount + " outerCount["+outerCount+"] < clusterSize["+state.clusterSize+"]=" + (outerCount < state.clusterSize));
        if (edgeCount > 0 && outerCount < state.clusterSize) {
            val indx = Math.abs(random.nextInt()) % edgeCount; //we pick a random adjacent node
            val nextV:Int = graph.getAdjacentVertexFromIndex(edgeStart + indx);
            outerCount = processVertex(nextV, placeId, clusterId, tx, accum + innerCount, plh) ;
        }
        
        return outerCount;
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
            val par = state.par;
            val verbose = state.verbose;
            val verticesPerPlace = state.verticesPerPlace;
            val startVertex = (N as Long*placeId/max) as Int;
            val endVertex = (N as Long*(placeId+1)/max) as Int;
            
            
            Console.OUT.println(here + " " + startVertex + "-" + endVertex);
            
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
                // Iterate over each of the vertices in my portion.
                var c:Long = 1;
                val vCount = endVertex - startVertex;
                for(var vertexIndex:Int=startVertex; vertexIndex<endVertex; ++vertexIndex, ++c) { 
                    val s:Int = state.verticesToWorkOn(vertexIndex);
                    val dest = s / verticesPerPlace;
                    val clusterId = c;
                    val closure = (tx:Tx) => {
                        processVertex(s, placeId, clusterId, tx, 0, plh);
                    };
                    
                    if (par == 1n) { //process the vertices in parallel
                        async { 
                            try {
                                if (verbose > 0) Console.OUT.println(here + " vertex["+s+"] cluster started   ["+clusterId+"/"+vCount+"]");
                                store.executeTransaction(closure);
                                if (g > 0 && clusterId%g == 0) Console.OUT.println(here + " vertex["+s+"] cluster succeeded ["+clusterId+"/"+vCount+"]");
                            } catch (ex:Exception) {
                                if (verbose > 0) Console.OUT.println(here + " vertex["+s+"] cluster failed    ["+clusterId+"/"+vCount+"]   msg["+ex.getMessage()+"] ");
                            }
                        }
                    } else { //process the vertices in sequence
                        try {
                            if (verbose > 0) Console.OUT.println(here + " vertex["+s+"] cluster started   ["+clusterId+"/"+vCount+"]");
                            store.executeTransaction(closure);
                            if (g > 0 && clusterId%g == 0) Console.OUT.println(here + " vertex["+s+"] cluster succeeded ["+clusterId+"/"+vCount+"]");
                        } catch (ex:Exception) {
                            if (verbose > 0) Console.OUT.println(here + " vertex["+s+"] cluster failed    ["+clusterId+"/"+vCount+"]   msg["+ex.getMessage()+"] ");
                        }
                    }
                }
            }
            
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
            Option("par", "", "Parallel processing of vertices"),
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
        val par:Int = cmdLineParams("-par", 1n); // on by default
        
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
        Console.OUT.println("parallel processing of vertices = " + par);
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
        val plh = PlaceLocalHandle.make[ClusteringState](Place.places(), ()=>ClusteringState.make(rmat, par, permute, places, new Random(here.id), verbose, clusterSize, g, vp, vt));
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
}

class ClusteringState(N:Int) {
    val graph:Graph;
    val verbose:Int;
    val par:Int;
    val verticesToWorkOn = new Rail[Int](N, (i:Long)=>i as Int);
    val places:Long;
    val verticesPerPlace:Long;
    val random:Random;
    val clusterSize:Long;
    val g:Long;
    
    val vt:String;
    val vp:String;
    
    var p0Cnt:Long;
    var p0Latch:SimpleLatch = null;
    var p0Excs:GrowableRail[CheckedThrowable] = null;
    
    private def this(graph:Graph, par:Int, places:Long, random:Random, verbose:Int, clusterSize:Long, g:Long, vp:String, vt:String) {
        property(graph.numVertices());
        this.graph = graph;
        this.places = places;
        this.verbose = verbose;
        this.par = par;
        this.verticesPerPlace = Math.ceil((graph.numVertices() as Double)/places) as Long;
        this.random = random;
        this.clusterSize = clusterSize;
        this.g = g;
        this.vp = vp;
        this.vt = vt;
    }
    
    static def make(rmat:Rmat, par:Int, permute:Int, places:Long, random:Random, verbose:Int, 
            clusterSize:Long, g:Long, vp:String, vt:String) {
        val graph = rmat.generate();
        if (verbose > 0 && here.id == 0) {
            Console.OUT.println(graph.toString());
            Console.OUT.println("=========================");
        }
        graph.compress();
        val s = new ClusteringState(graph, par, places, random, verbose, clusterSize, g, vp, vt);
        if (permute > 0) s.permuteVertices();
        return s;
    }
    
    /**
     * A function to shuffle the vertices randomly to give better work dist.
     */
    private def permuteVertices() {
        val prng = new Random(1);

        for(var i:Int=0n; i<N; i++) {
            val indexToPick = prng.nextInt(N-i);
            val v = verticesToWorkOn(i);
            verticesToWorkOn(i) = verticesToWorkOn(i+indexToPick);
            verticesToWorkOn(i+indexToPick) = v;
        }
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