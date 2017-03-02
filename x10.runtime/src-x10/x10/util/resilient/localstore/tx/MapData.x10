package x10.util.resilient.localstore.tx;

import x10.util.concurrent.Lock;

import com.sun.javafx.css.StyleCacheEntry.Key;

import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;

/*
 * MapData may be accessed by different transactions at the same time.
 * We use a lock per MapBucket to synchronize access to the shared metadata hashmap.
 **/
public class MapData(name:String) {
	val BUCKETS = System.getenv("BUCKETS") == null? 1024 : Long.parseLong(System.getenv("BUCKETS"));
	val bucketsLocks:Rail[Lock];
	val buckets:Rail[MapBucket];
	
    public def this(name:String) {
        property(name);
        buckets = new Rail[MapBucket](BUCKETS, (i:Long) => new MapBucket(i));
        bucketsLocks = new Rail[Lock](BUCKETS, (i:Long) => new Lock());
    }
    
    public def this(name:String, serBuckets:Rail[MapBucket]) {
        property(name);
        buckets = new Rail[MapBucket](BUCKETS, (i:Long) => new MapBucket(i, serBuckets(i)));
        bucketsLocks = new Rail[Lock](BUCKETS, (i:Long) => new Lock());
    }

    
    public def getSerializableBuckets() {
    	val serBuckets = new Rail[MapBucket](BUCKETS);
    	for (var i:Int = 0; i < BUCKETS; i++) {
    		bucketsLocks(i).lock();
    		serBuckets(i) = buckets(i).getSerializableBucket();
    		bucketsLocks(i).unlock();
    	}
        return serBuckets;
    }
    
    public def getMemoryUnit(k:String):MemoryUnit {
    	val indx=getBucketIndex(k);
    	var m:MemoryUnit = null;
        try {
        	bucketsLocks(indx).lock();
        	m =  
        } finally {
        	bucketsLocks(indx).unlock();
        }
        return m;
    }
    
    
    public def keySet() {
        throw new Exception("TODO implement MapData.keySet() ...");
    }
    
    public def toString() {
    	throw new Exception("TODO implement MapData.toString() ...");
    }
    
    private def getBucketIndex(key:String) = key.hashCode() % BUCKETS ;

}