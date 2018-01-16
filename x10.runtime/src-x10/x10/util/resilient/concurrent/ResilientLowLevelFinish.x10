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
public class ResilientLowLevelFinish implements Unserializable {
    private val gr = GlobalRef[ResilientLowLevelFinish](this);
    private val places:Rail[Int];
    private val status:Rail[Int];
    private val cond:Condition;
    private var count:Int;
    private val ilock = new Lock();
    
    private static val all = new ArrayList[ResilientLowLevelFinish]();
    private static val glock = new Lock();
    
    private def this(p:Rail[Int]) {
    	cond = new Condition();
    	places = p;
    	count = p.size as Int;
    	status = new Rail[Int](p.size, 0n);
    }
    
    public static def make(p:Rail[Int]){
        val instance = new ResilientLowLevelFinish(p);
        glock.lock();
        all.add(instance);
        glock.unlock();
        return instance;
    }
    
    public def getGr() = gr;
    
    public def notifyTermination(place:Int) {
    	try {
    		ilock.lock();
    		count--;
    		for (var i:Long = 0; i < places.size; i++) {
    			if (places(i) == place) {
    				status(i) = 1n;
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
    			if (status(i) == 0n && (places(i) == -1n || Place(places(i)).isDead()) ) {
    				count--;
    				status(i) = -1n;
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
    
    public def failed() {
    	for (var i:Long = 0; i < status.size; i++) {
    		if (status(i) == -1n)
    			return true;
    	}
    	return false;
    }
    
    public def forget() {
        glock.lock();
        (gr as GlobalRef[ResilientLowLevelFinish]{self.home == here}).forget();
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
    
    public def run (immediateAsyncClosure:(gr:GlobalRef[ResilientLowLevelFinish])=>void) {
        immediateAsyncClosure(gr); //this closure MUST release the condition 
        await();
    }
    
    public static def notifyPlaceDeath() {
        glock.lock();
        for (inst in all) {
            inst.notifyFailure();
        }
        glock.unlock();
    }
}
