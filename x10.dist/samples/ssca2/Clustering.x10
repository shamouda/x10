import x10.compiler.Pragma;
import x10.util.Random;
import x10.util.OptionsParser;
import x10.util.Option;
import x10.util.Team;
import x10.util.concurrent.Lock;

import x10.util.resilient.PlaceManager;
import x10.xrx.txstore.TxConfig;
import x10.xrx.TxStoreFatalException;
import x10.util.resilient.localstore.Cloneable;
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

//assumptions:
//graph generated using the R-MAT algorithm
//all edge costs are considered equal
//test command: X10_RESILIENT_VERBOSE=0 TM_DEBUG=0 CONFLICT_SLEEP_MS=0 X10_NUM_IMMEDIATE_THREADS=1 TM=locking X10_RESILIENT_MODE=14 X10_NTHREADS=1 X10_NPLACES=5 ./a.out -w 1 -n 10 -vp 3,1 -sp 2 -vt 0.25,0.8 -vg 1
public final class Clustering(plh:PlaceLocalHandle[ClusteringState]) implements MasterWorkerApp {
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
        val locked = new HashSet[Int]();
        val lockedLast = new HashSet[Int]();
        val lc = new Lock();
        public def merge() {
            lc.lock();
            locked.addAll(lockedLast);
            lockedLast.clear();
            lc.unlock();
        }
        
        
        public def addToLockedLast(l:HashSet[Int]) {
            lc.lock();
            lockedLast.addAll(l);
            lc.unlock();
        }
        
        public def print(txId:Long, placeId:Long, clusterId:Long) {
            val cId = (placeId+1) * 1000000 + clusterId;
            var str:String = "Tx["+txId+"] cluster["+cId+"] result -> ";
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
            if (w == v)
                continue;
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
        if (verbose > 2n) {
            printVertexPlaceMap(v, map, tx.id);
        }
        //occupy the adjacent vertices that are not occupied
        finish {
            Runtime.registerFinishTx(tx, false);
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
                        result().addToLockedLast(locked);
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
            var nextV:Int = root;
            while (nextV != -1n) {
                nextV = processVertex(nextV, placeId, clusterId, tx, plh, result);
            }
            if (verbose > 1n) result().print(tx.id, placeId, clusterId);
        } //else: the root is taken, end this iteration 
    }

    private def execute(store:TxStore, state:ClusteringState, placeId:Long, workerId:Long, start:Int, end:Int, 
            plh:PlaceLocalHandle[ClusteringState], verbose:Int, killG:Double) {
        val time0 = System.currentTimeMillis();
        var totalConflicts:Long = 0;
        val all = end - start;
        if (killG != -1.0)
            Console.OUT.println(here + ":worker:"+workerId+":from:" + start + ":to:" + (end-1) + ":killG:"+killG);
        // Iterate over each of the vertices in my portion.
        var c:Long = 1;
        for(var vertexIndex:Int=start; vertexIndex<end; ++vertexIndex, ++c) { 
            val s = vertexIndex;
            val clusterId = vertexIndex as Long;
            val closure = (tx:Tx) => {
                createCluster(store, tx, s, placeId, clusterId, plh, verbose);
            };
            val conf = store.executeTransaction(closure);
            totalConflicts += conf;
            
            if (state.g > -1 && c % state.g == 0) {
                printProgress(here + ":worker:"+workerId+":progress -> " + c + "/" + all + " conflicts="+conf, time0);
            }
            if (killG > -1.0 && ((c as Double)/all) >= killG) {
            	Console.OUT.println(here + " Hammer calling killHere " + c + "/" + all );
                System.killHere();
            }
        }
        //Console.OUT.println(here + ":worker:"+workerId+":from:" + start + ":to:" + (end-1)+":totalConflicts:"+totalConflicts);
        state.addConflicts(totalConflicts);
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
        val verticesPerWorker = Math.ceil((state.verticesPerPlace as Double)/state.workersPerPlace) as Int;
        val startVertex = (N as Long*placeId/max) as Int;
        val endVertex = (N as Long*(placeId+1)/max) as Int;
        
        var killProgress:Double = 0.0;
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
                if (state.vg == 1n) {
                	killProgress = Double.parseDouble(times(indx));
                	Console.OUT.println("Hammer kill based on progress");
                } else {
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
        }
        
        finish {
            for (workerId in 1..state.workersPerPlace) {
                val start = (startVertex + (workerId-1) * verticesPerWorker) as Int;
                val end = start + verticesPerWorker as Int;
                val end2 = end >= state.N ? state.N : end;
                val killG = (workerId == 1 && killProgress != 0.0) ? killProgress : -1.0;
                async execute(store, state, placeId, workerId, start, end2, plh, verbose, killG);
            }
        }
        val totalConflicts = state.totalConflicts;
        state.totalConflicts = 0;
        //Console.OUT.println(here + " ==> completed successfully ");
        return totalConflicts;
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
            Option("i", "", "Iterations"),
            Option("vp","victims","victim places to kill (comma separated)"),
            Option("vt","victimsTimes","times to kill victim places(comma separated)"),
            Option("vg","killByProgress","whether we kill by time or progress: default 0 (by time)"),
            Option("v", "", "Verbose")]);

        val spare:Long = cmdLineParams("-sp", 0);
        val clusterSize:Long = cmdLineParams("-cs", 10);
        val seed:Long = cmdLineParams("-s", 2);
        val iters:Long = cmdLineParams("-i", 1);
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
        val vg = cmdLineParams("vg", 0n); //kill based on progress
        val workers:Int = cmdLineParams("-w", Runtime.NTHREADS); 
        
        Console.OUT.println("Running SSCA2 with the following parameters:");
        Console.OUT.println("X10_NPLACES="  + Place.numPlaces());
        Console.OUT.println("X10_NTHREADS=" + Runtime.NTHREADS);
        Console.OUT.println("X10_NUM_IMMEDIATE_THREADS=" + System.getenv("X10_NUM_IMMEDIATE_THREADS"));
        Console.OUT.println("X10_RESILIENT_MODE=" + x10.xrx.Runtime.RESILIENT_MODE);
        Console.OUT.println("TM=" + System.getenv("TM"));
        Console.OUT.println("LOCK_FREE=" + System.getenv("LOCK_FREE"));
        Console.OUT.println("X10_EXIT_BY_SIGKILL=" + System.getenv("X10_EXIT_BY_SIGKILL"));
        Console.OUT.println("DISABLE_SLAVE=" + System.getenv("DISABLE_SLAVE"));
        Console.OUT.println("ENABLE_STAT=" + System.getenv("ENABLE_STAT"));
        Console.OUT.println("BUSY_LOCK=" + System.getenv("BUSY_LOCK"));
        Console.OUT.println("CONFLICT_SLEEP_MS=" + System.getenv("CONFLICT_SLEEP_MS"));
        
        Console.OUT.println("clusterSize = " + clusterSize);
        Console.OUT.println("seed = " + seed);
        Console.OUT.println("workers = " + workers);
        Console.OUT.println("N = " + (1<<n));
        Console.OUT.println("a = " + a);
        Console.OUT.println("b = " + b);
        Console.OUT.println("c = " + c);
        Console.OUT.println("d = " + d);
        
        Console.OUT.println("Hammer parameters: vp="+vp+" vt="+vt+" vg="+ vg);
        if (System.getenv("TM") != null && System.getenv("TM").equals("locking")) {
            Console.OUT.println("!!!!ERROR: TM=locking is not accepted in this program!!!!");
            return;
        }
        val ar1 = vp.split(",");
        val ar2 = vt.split(",");
        if (ar1.size != ar2.size) {
            Console.OUT.println("!!!!ERROR: vp and vt must have the same number of elements!!!!");
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
        val plh = PlaceLocalHandle.make[ClusteringState](Place.places(), ()=>new ClusteringState(graph, places, workers, verbose, clusterSize, g, vp, vt, vg));
        val app = new Clustering(plh);
        var distTime:Double = 0;
        for (var i:Long = 0 ; i < iters; i++) {
            val executor = MasterWorkerExecutor.make(activePlaces, app);
            if (i == 0)
                distTime = (System.nanoTime()-time)/1e9;       
            time = System.nanoTime();
            executor.run();
            time = System.nanoTime() - time;        
            val results = executor.workerResults();
            var conflicts:Long = 0;
            for (entry in results.entries()) {
                conflicts += entry.getValue() as Long;
            }
            val procTime = time/1E9;
            val totalTime = distTime + procTime;
            val procPct = procTime*100.0/totalTime;
            if(verbose > 2 || r > 0) 
                app.printClusters(executor.store());
            var dead:String = "";
            for (var dx:Long = 0; dx < (places + spare); dx++) {
                if (Place(dx).isDead())
                    dead += "Place("+dx+") ";
            }
            Console.OUT.println("Iter:"+i+":Places:" + places + ":N:" + plh().N + ":SetupInSeconds:" + distTime + ":ProcessingInSeconds:" + procTime + ":TotalInSeconds:" + totalTime + ":conflicts:"+conflicts+":(proc:" + procPct  + "%):dead:"+dead);
        }
    }
}

class ClusteringState(N:Int) {
    val graph:Graph;
    val verbose:Int;
    val places:Long;
    val workersPerPlace:Long;
    val verticesPerPlace:Long;
    val random:Random;
    val clusterSize:Long;
    val g:Long;
    
    val vt:String;
    val vp:String;
    val vg:Int;

    var totalConflicts:Long = 0;
    val lock = new Lock();
    
    public def this(graph:Graph, places:Long, workersPerPlace:Long,
            verbose:Int, clusterSize:Long, g:Long, vp:String, vt:String, vg:Int) {
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
        this.vg = vg;
    }
    
    public def addConflicts(r:Long) {
        lock.lock();
        totalConflicts += r;
        lock.unlock();
    }
}