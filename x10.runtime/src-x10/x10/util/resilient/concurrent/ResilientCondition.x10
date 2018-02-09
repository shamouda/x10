package x10.util.resilient.concurrent;

import x10.util.concurrent.Condition;
import x10.util.concurrent.Lock;
import x10.io.Unserializable;
import x10.util.ArrayList;
import x10.xrx.Runtime;

/**
 * A wrapper for x10.util.concurrent.Condition 
 * that releases the condition when the releasing place dies.
 */
public class ResilientCondition implements Unserializable {
    private transient var place:Place;
    public val gr = new GlobalRef[Condition](new Condition());

    private static val all = new ArrayList[ResilientCondition]();
    private static val lock = new Lock();
    
    private def this(p:Place) {
        place = p;
    }
    
    public static def make(p:Place){
        val instance = new ResilientCondition(p);
        lock.lock();
        all.add(instance);
        lock.unlock();
        return instance;
    }
    
    public def failed() {
        return place.isDead();
    }
    
    public def forget() {
        lock.lock();
        (gr as GlobalRef[Condition]{self.home == here}).forget();
        all.remove(this);
        lock.unlock();
    }
    
    public def await() {
        if (place.isDead()) {
            forceRelease();
            return;
        }
        
        try {
        	if (Runtime.NUM_IMMEDIATE_THREADS == 0n) Runtime.increaseParallelism();
            (gr as GlobalRef[Condition]{self.home == here})().await();
        } finally {
        	if (Runtime.NUM_IMMEDIATE_THREADS == 0n) Runtime.decreaseParallelism(1n);
        }
    }
    
    public def forceRelease() {
        (gr as GlobalRef[Condition]{self.home == here})().release();
    }
    
    public def run (immediateAsyncClosure:(gr:GlobalRef[Condition])=>void) {
        immediateAsyncClosure(gr); //this closure MUST release the condition 
        await();
    }
    
    public static def notifyPlaceDeath() {
        lock.lock();
        for (inst in all) {
            if (inst.failed())
                inst.forceRelease();
        }
        lock.unlock();
    }
}