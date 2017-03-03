import x10.util.ArrayList;
import x10.util.resilient.PlaceManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.concurrent.Future;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.util.HashMap;
import x10.util.resilient.localstore.CloneableLong;
import x10.util.Timer;
import x10.util.Option;
import x10.util.OptionsParser;
import x10.xrx.Runtime;

public class STMBench {
	private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
	
	public static def main(args:Rail[String]) {
		val opts = new OptionsParser(args, [
            Option("h","help","this information"),
            Option("v","verify","verify the result")
		], [
			Option("i","initialKeys","The number of initially added keys (default = 16K)"),
			Option("r","keysRange","The range of possible keys (default 32K)"),
			Option("u","updatePercentage","percentage of update operation (default 0.0)"),
			Option("n","iterations","number of iterations of the benchmark (default 5)"),
            Option("p","txPlaces","number of places creating transactions (default (X10_PLACES-spare))"),
            Option("w","warmup","warm up time in milliseconds (default 5000 ms)"),
            Option("d","iterationDuration","Single iteration duration"),
            Option("h","txParticipants","number of transaction participants (default 2)"),
            Option("o","TxParticipantOperations","number of operations per transaction participant"),
            Option("s","spare","Spare places (default 0)")
        ]);
		
		val i = opts("i", 16*1024);
		val r = opts("r", 32*1024);
		val u = opts("u", 0.0F);
		val n = opts("n", 5);		
		val w = opts("w", 5000);
		val d = opts("d", 5000);
		val h = opts("h", 2);
		val o = opts("o", 1);
		val s = opts("s", 0);
		
		val mgr = new PlaceManager(s, false);
		val p = opts("p", mgr.activePlaces().size());
		val store = ResilientStore.make(mgr.activePlaces());
		val map = store.makeMap("map");
		initMap(map, i);
		
		val producers = getTxProducers(mgr.activePlaces(), p);
		val threads = 
		val algorithm = 
	
    }
	
	public static def initMap(map:ResilientNativeMap, i:Long) {
		
	}
	
	public static def getTxProducers(activePlaces:PlaceGroup, p:Long){
		if (p == activePlaces.size())
			return activePlaces;
		return new SparsePlaceGroup(new Rail[Place](p, (i:Long) => activePlaces(i)));
	}
}