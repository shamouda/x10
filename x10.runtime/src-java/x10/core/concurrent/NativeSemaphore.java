package x10.core.concurrent;

@SuppressWarnings("serial")
public class NativeSemaphore extends java.util.concurrent.Semaphore {
	
    public NativeSemaphore(System[] $dummy) { 
    	super(-1); 
    }
    
    // constructor just for allocation
    public NativeSemaphore(int permits) {
        super(permits);
    }
    
    public final NativeSemaphore x10$core$concurrent$NativeSemaphore$$init$S(int permits) {
        return this;
    }
    
    public void acquire() {
       try {
    	   super.acquire();
       }catch (java.lang.InterruptedException e) {
           throw new x10.xrx.InterruptedException();
       }
    }   

    public boolean tryAcquire$O() {
    	return super.tryAcquire();
    }

    public void release() {
    	super.release();
    }
    
    public int availablePermits$O() {
    	return super.availablePermits();
    }
}
