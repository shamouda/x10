import x10.util.ArrayList;

import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.LockingRequest;
import x10.util.resilient.localstore.LockingRequest.KeyInfo;
import x10.util.resilient.localstore.LockingTx;
import x10.util.resilient.localstore.LocalTx;

import x10.util.concurrent.Future;
import x10.xrx.Runtime;
import x10.util.HashMap;
import x10.util.Timer;
import x10.util.Option;
import x10.util.OptionsParser;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.HashSet;


public class STMBench {
	private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
	
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
            Option("o","TxParticipantOperations","number of operations per transaction participant"),
            Option("g","progress","interval of progress reporting per producer (default 10 transactions)"),
            Option("s","spare","Spare places (default 0)")
        ]);
		
		val r = opts("r", 32*1024);
		val u = opts("u", 0.0F);
		val n = opts("n", 5);	
		val t = opts("t", Runtime.NTHREADS);
		val w = opts("w", 5000);
		val d = opts("d", 5000);
		val h = opts("h", 2);
		val o = opts("o", 2);
		val g = opts("g", 10);
		val s = opts("s", 0);
			
		val mgr = new PlaceManager(s, false);
		val activePlaces = mgr.activePlaces();
		val p = opts("p", activePlaces.size());
		val producerPlaces = getTxProducers(activePlaces, p);		
		printRunConfigurations (new STMBenchParameters(r, u, n, p, t, w, d, h, o, g, s));
		
		assert (h <= activePlaces.size()) : "invalid value for parameter h, h should not exceed the number of active places" ;

		if (TxConfig.getInstance().LOCKING_MODE == TxConfig.LOCKING_MODE_FREE) {
			assert (p * t == 1): "lock free mode can only be used with only 1 producer thread";
		}
		
		val store = ResilientStore.make(activePlaces);
		val map = store.makeMap("map");
		
		val startWarmup = Timer.milliTime();
		Console.OUT.println("warmup started");
		runIteration(map, activePlaces, producerPlaces, w, r, u, t, h, o, g);
		Console.OUT.println("warmup completed, warmp elapsed time ["+(Timer.milliTime() - startWarmup)+"]  ms ");
		
		for (iter in 1..n) {
			val throughputList = runIteration(map, activePlaces, producerPlaces, d, r, u, t, h, o, g);
			printThroughput(iter, throughputList, h, o);
		}
    }
	
	public static def runIteration(map:ResilientNativeMap, activePlaces:PlaceGroup, producers:PlaceGroup, d:Long, r:Long, u:Float, t:Long, h:Long, o:Long, g:Long) {
		val root = here;
		val statGR = GlobalRef(new ArrayList[ProducerThroughput]());
		finish for (pl in producers) at (pl) async {
			for (thrd in 1..t) async {
				
				val throughput = produce(map, activePlaces, producers, thrd, d, r, u, t, h, o, g);
				
				at (root) async {
					atomic statGR().add(throughput);
				}
			}
		}
		map.printTxStatistics();
		map.resetTxStatistics();
		return statGR();
	}

	public static def produce(map:ResilientNativeMap, activePlaces:PlaceGroup, producers:PlaceGroup, producerId:Long, d:Long, r:Long, u:Float, t:Long, h:Long, o:Long, g:Long) {
		val startMS = Timer.milliTime();
		var txCount:Long = 0;
		val activePlacesCount = activePlaces.size();
		val rand = new Random((here.id+1) * producerId);
		var flag:Boolean = true;
		
		while (Timer.milliTime() - startMS < d) {
			val members = nextTransactionMembers(rand, activePlaces, h);
			val membersOperations = nextRandomeOperations(rand, activePlacesCount, members, r, u, o);
			
			/*var str:String = "members = ";
			for (ee in members) {
				str += ee + " ";
			}
			Console.OUT.println(str);*/
			if (members.size() > 1 && !TxConfig.getInstance().TM.equals("locking")) {
				if (flag) {
				    Console.OUT.println("globalSTM ");
				    flag = false;
				}
				map.executeTransaction(members, (tx:Tx) => {
					val futuresList = new ArrayList[Future[Any]]();
					
					for (var m:Long = 0; m < members.size(); m++) {
						val operations = membersOperations.get(m);
						//Console.OUT.println(operations);
						val f1 = tx.asyncAt(operations.dest, () => {
							
							for (var x:Long = 0; x < o; x++) {
								val key = operations.keys(x).key;
								val read = operations.keys(x).rw;
								val value = operations.values(x);
								
								if (read) {
									tx.get(key);
									//Console.OUT.println(here.id + "x" + producerId + ":read:"+key);
								}
								else {
									tx.put(key, new CloneableLong(value));
									//Console.OUT.println(here.id + "x" + producerId + ":write:"+key);
								}
							}
		                });
						futuresList.add(f1);
					}
	                val startWait = Timer.milliTime();
	                for (f in futuresList)
	                	f.force();
	                tx.setWaitElapsedTime(Timer.milliTime() - startWait);
	                return null;
	            });
			}
			else if (members.size() == 1 && producers.size() == 1 && !TxConfig.getInstance().TM.equals("locking")) {
			    if (flag) {
			        Console.OUT.println("localSTM ");
			        flag = false;
			    }
				//local transaction
				assert (members(0) == here) : "local transactions are not supported at remote places for this benchmark" ;
				map.executeLocalTransaction((tx:LocalTx) => {
					val operations = membersOperations.get(0);
					//Console.OUT.println(operations);
					for (var x:Long = 0; x < o; x++) {
						val key = operations.keys(x).key;
						val read = operations.keys(x).rw;
						val value = operations.values(x);
						
						if (read) {
							tx.get(key);
							//Console.OUT.println(here.id + "x" + producerId + ":read:"+key);
						}
						else {
							tx.put(key, new CloneableLong(value));
							//Console.OUT.println(here.id + "x" + producerId + ":write:"+key);
						}
					}
	                return null;
	            });
			}
			else if (members.size() > 1 && TxConfig.getInstance().TM.equals("locking")) { //locking
			    if (flag) {
			        Console.OUT.println("globalLocking ");
			        flag = false;
			    }
				val lockRequests = new ArrayList[LockingRequest]();
				for (memReq in membersOperations) {
					lockRequests.add(new LockingRequest(memReq.dest, memReq.keys));
				}
				
				map.executeLockingTransaction(members, lockRequests, (tx:LockingTx) => {
					val futuresList = new ArrayList[Future[Any]]();
					
					for (var m:Long = 0; m < members.size(); m++) {
						val operations = membersOperations.get(m);
						//Console.OUT.println(operations);
						val f1 = tx.asyncAt(operations.dest, () => {
							for (var x:Long = 0; x < o; x++) {
								val key = operations.keys(x).key;
								val read = operations.keys(x).rw;
								val value = operations.values(x);
								
								if (read) {
									tx.get(key);
									//Console.OUT.println(here.id + "x" + producerId + ":read:"+key);
								}
								else {
									tx.put(key, new CloneableLong(value));
									//Console.OUT.println(here.id + "x" + producerId + ":write:"+key);
								}
							}
		                });
						futuresList.add(f1);
					}
	                val startWait = Timer.milliTime();
	                for (f in futuresList)
	                	f.force();
	                tx.setWaitElapsedTime(Timer.milliTime() - startWait);
	                return null;
				});
			}
			else if (members.size() == 1 && producers.size() == 1 && TxConfig.getInstance().TM.equals("locking")) { //locking
				if (flag) {
				    Console.OUT.println("localLocking ");
				    flag = false;
				}
				val lockRequests = new ArrayList[LockingRequest]();
				for (memReq in membersOperations) {
					lockRequests.add(new LockingRequest(memReq.dest, memReq.keys));
				}
				
				map.executeLockingTransaction(members, lockRequests, (tx:LockingTx) => {
					val operations = membersOperations.get(0);
					//Console.OUT.println(operations);
					for (var x:Long = 0; x < o; x++) {
						val key = operations.keys(x).key;
						val read = operations.keys(x).rw;
						val value = operations.values(x);
						
						if (read) {
							tx.get(key);
							//Console.OUT.println(here.id + "x" + producerId + ":read:"+key);
						}
						else {
							tx.put(key, new CloneableLong(value));
							//Console.OUT.println(here.id + "x" + producerId + ":write:"+key);
						}
					}
	                return null;
				});
			}
			else
				assert (false) : "wrong or unsupported configurations, members= " + members.size();
				
			if (g != -1 && txCount%g == 0)
				Console.OUT.println("progress "+here.id+"x"+producerId + ":" + txCount );
			txCount++;
		}
		
		val endMS = Timer.milliTime();
		return new ProducerThroughput(here.id, producerId, endMS - startMS, txCount);
	}

	public static def printThroughput(iteration:Long, throughputList:ArrayList[ProducerThroughput], h:Long, o:Long) {
	    var allOperations:Long = 0;
	    var allTime:Long = 0;
	    val producers = throughputList.size();
	    for ( producer in throughputList) {
	    	allOperations += producer.txCount * h * o;
	    	allTime += producer.elapsedTimeMS;
	    	val localThroughput = producer.txCount * h * o / producer.elapsedTimeMS;
	    	Console.OUT.println("iteration:" + iteration +":producer:"+producer.placeId+"x"+producer.threadId+ ":throughput:"+localThroughput);
	    }
	    val throughput = (allOperations as Double) / allTime * producers;
	    Console.OUT.println("iteration:" + iteration + ":throughput:"+throughput);
	}
	
	
	
	public static def nextTransactionMembers(rand:Random, activePlaces:PlaceGroup, h:Long) {
		val activePlacesCount = activePlaces.size();
		val selectedPlaces = new HashSet[Long]();
		while (selectedPlaces.size() < h) {
			selectedPlaces.add(Math.abs(rand.nextLong()) % activePlacesCount);
		}
		val rail = new Rail[Long](h);
		val iter = selectedPlaces.iterator();
		for (var c:Long = 0; c < h; c++) {
			rail(c) = iter.next();
		}
		RailUtils.sort(rail);
		return new SparsePlaceGroup(new Rail[Place](h, (i:Long) => activePlaces(rail(i))));
	}
	
	public static def nextRandomeOperations(rand:Random, activePlacesCount:Long, members:PlaceGroup, r:Long, u:Float, o:Long) {
		val list = new ArrayList[MemberOperations]();
		
		val keysPerPlace = r / activePlacesCount;
		
		for (pl in members) {
			val keys = new Rail[KeyInfo](o);
			val values = new Rail[Long](o);
			
			val baseKey = pl.id * keysPerPlace;
			val uniqueKeys = new HashSet[String]();
			for (var x:Long = 0; x < o; x++) {
				val rw = rand.nextFloat() >= u;
				var k:String = "key" + (baseKey + (Math.abs(rand.nextLong()% keysPerPlace)));
				while (uniqueKeys.contains(k))
					k = "key" + (baseKey + (Math.abs(rand.nextLong()% keysPerPlace)));
				uniqueKeys.add(k);
				keys(x) = new KeyInfo(k, rw); 
				values(x) = Math.abs(rand.nextLong())%1000;
			}
			list.add(new MemberOperations(pl, keys, values));
		}
		
		return list;
	}
	
	/*********************  Structs ********************/
	public static struct MemberOperations {
		val dest:Place;
	    val keys:Rail[KeyInfo];
	    val values:Rail[Long];
	    public def this(dest:Place, keys:Rail[KeyInfo], values:Rail[Long]) {
	    	this.dest = dest;
	    	this.keys = keys;
	    	this.values = values;
	    }
	    public def toString() {
	    	var str:String = "memberOperations:" + dest + ":";
	        for (k in keys)
	        	str += "("+k.key+","+k.rw+")" ;
	    	return str;
	    }
	}
	
	public static struct STMBenchParameters {
    	public val r:Long;  //keysRange
    	public val u:Float; //updatePercentage
    	public val n:Long;	//iterations
    	public val p:Long;  //txPlaces
    	public val t:Long;  //txThreadsPerPlace
    	public val w:Long;  //warmupTime
    	public val d:Long;  //iterationDuration
    	public val h:Long;  //txParticipants
    	public val o:Long;  //TxParticipantOperations
		public val g:Long;  //progress
    	public val s:Long;  //spare

        def this(r:Long, u:Float, n:Long, p:Long, t:Long, w:Long, 
        		d:Long, h:Long, o:Long, g:Long, s:Long) {
            this.r = r;
            this.u = u;
            this.n = n;
            this.p = p;
            this.t = t;
            this.w = w;
            this.d = d;
            this.h = h;
            this.o = o;
            this.g = g;
            this.s = s;
        }
    };
	
    public static struct ProducerThroughput {
    	val placeId:Long;
    	val threadId:Long;
    	val elapsedTimeMS:Long;
    	val txCount:Long;
        public def this (placeId:Long, threadId:Long, elapsedTimeMS:Long, txCount:Long) {
        	this.elapsedTimeMS = elapsedTimeMS;
        	this.txCount = txCount;
        	this.placeId = placeId;
        	this.threadId = threadId;
        }
    };
    
	/***********************   Utils  *****************************/
	public static def getTxProducers(activePlaces:PlaceGroup, p:Long){
		if (p == activePlaces.size())
			return activePlaces;
		return new SparsePlaceGroup(new Rail[Place](p, (i:Long) => activePlaces(i)));
	}
	
	public static def printRunConfigurations(param:STMBenchParameters) {
		Console.OUT.println("STMBench starting with the following parameters:");
		Console.OUT.println("X10_NPLACES=" + System.getenv("X10_NPLACES"));
		Console.OUT.println("X10_NTHREADS=" + Runtime.NTHREADS);
		Console.OUT.println("TM=" + System.getenv("TM"));
		Console.OUT.println("LOCK_FREE=" + System.getenv("LOCK_FREE"));
		
		Console.OUT.println("r=" + param.r);
		Console.OUT.println("u=" + param.u);
		Console.OUT.println("n=" + param.n);
		Console.OUT.println("p=" + param.p);
		Console.OUT.println("t=" + param.t);
		Console.OUT.println("w=" + param.w);
		Console.OUT.println("d=" + param.d);
		Console.OUT.println("h=" + param.h);
		Console.OUT.println("o=" + param.o);
		Console.OUT.println("g=" + param.g);
		Console.OUT.println("s=" + param.s);
	}
}