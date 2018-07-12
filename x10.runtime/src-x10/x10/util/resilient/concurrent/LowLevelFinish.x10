package x10.util.resilient.concurrent;

import x10.util.concurrent.Condition;
import x10.util.concurrent.Lock;
import x10.io.Unserializable;
import x10.util.ArrayList;
import x10.xrx.Runtime;
import x10.util.concurrent.AtomicInteger;

/**
 * Low level resilient SPMD finish 
 */
public class LowLevelFinish implements Unserializable {
    private val gr = GlobalRef[LowLevelFinish](this);
    private val places:Rail[Int];
    private val cond:Condition;
    private var count:Int;
    private val ilock = new Lock();
    
    private var vote:Boolean = true;
    private var failure:Boolean = false;
    
    private static val all = new ArrayList[LowLevelFinish]();
    private static val glock = new Lock();
    
    private def this(p:Rail[Int]) {
    	cond = new Condition();
    	places = new Rail[Int](p);
    	count = p.size as Int;
    }
    
    public static def make(p:Rail[Int]){
        val instance = new LowLevelFinish(p);
        glock.lock();
        all.add(instance);
        glock.unlock();
        return instance;
    }
    
    public def getGr() = gr;
    
    public def notifyTermination(place:Int) {
        notifyTermination(place, true);
    }
    
    public def notifyTermination(place:Int, placeVote:Boolean) {
    	try {
    		ilock.lock();
    		for (var i:Long = 0; i < places.size; i++) {
    			if (places(i) == place) {
    			    count--;
    	            vote = vote & placeVote;
    			    places(i) = -1000n;
    				break;
    			}
    		}
    		if (count == 0n) {
    			cond.release();
    			forget();
    		}
    	} finally {
    		ilock.unlock();
    	}
    }
    
    public def notifyFailure() {
    	try {
    		ilock.lock();
    		for (var i:Long = 0; i < places.size; i++) {
    			if (places(i) != -1000n && Place(places(i)).isDead() ) {
    				count--;
    				places(i) = -1000n;
    				failure = true;
    			}
    		}
    		if (count == 0n) {
    			cond.release();
    			forget();
    		}
    	} finally {
    		ilock.unlock();
    	}
    }
    
    public def failed() = failure;
    
    public def yesVote() = vote;

    public def forget() {
        glock.lock();
        (gr as GlobalRef[LowLevelFinish]{self.home == here}).forget();
        all.remove(this);
        glock.unlock();
    }
    
    public def await() {
        try {
        	Runtime.increaseParallelism();
            cond.await();
        } finally {
        	Runtime.decreaseParallelism(1n);
        }
    }
    
    public def run (immediateAsyncClosure:(gr:GlobalRef[LowLevelFinish])=>void) {
        immediateAsyncClosure(gr); //this closure MUST release the condition 
        await();
    }
    
    public static def notifyPlaceDeath() {
        val tmp = new ArrayList[LowLevelFinish]();
        glock.lock();
        for (inst in all) {
            tmp.add(inst);
        }
        glock.unlock();
        
        for (inst in tmp) {
            inst.notifyFailure();    
        }
        
    }
}
