package x10.core.concurrent;

import java.util.concurrent.Semaphore;

@SuppressWarnings("serial")
public class NativeSemaphore  {
	private Semaphore sem;
    
    public NativeSemaphore(System[] $dummy) {
    }
    
    public final NativeSemaphore x10$core$concurrent$NativeSemaphore$$init$S(int permits) {
        sem = new Semaphore(permits);
        return this;
    }
    
    public void acquire() {
       try {
           sem.acquire();
       }catch (java.lang.InterruptedException e) {
           throw new x10.xrx.InterruptedException();
       }
    }   

    public boolean tryAcquire$O() {
    	return sem.tryAcquire();
    }

    public void release() {
        sem.release();
    }
    
    public int availablePermits$O() {
    	return sem.availablePermits();
    }
}
