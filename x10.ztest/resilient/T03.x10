import x10.util.concurrent.AtomicInteger;
//x10c++ T03.x10 -o t03.o
//X10_RESILIENT_VERBOSE=0 X10_RESILIENT_MODE=14 X10_NPLACES=4 ./t03.o

/*testing that the master nominates a new backup when it is dead and the new backup
 * doesn't deny the masters requests*/
public class T03 {
    static OUTER_ITERS = 100;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)
    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }   
   
    public static def main (args:Rail[String]) {
        val gr = new GlobalRef[AtomicInteger](new AtomicInteger(0n));
        
        try {
            
            finish {
                
                at (Place(1)) async {
                    
                    think(1);
                    
                    finish {
                        at (Place(3)) async {
                            
                            think(1);
                            
                            val me = here;
                            
                            at (gr) async { 
                                val c = gr().incrementAndGet();
                                
                                Console.OUT.println(me + " incr to " + c);
                            }
                        }
                    }

                    at (Place(2)) async {
                        System.killHere();
                    }
                    
                    System.threadSleep(100);
                    
                    Console.OUT.println(" place 2 is dead = " + Place(2).isDead());
                    
                    finish {
                        at (Place(3)) async {
                            
                            think(1);
                            
                            val me = here;
                            
                            at (gr) async { 
                                val c = gr().incrementAndGet();
                                
                                Console.OUT.println(me + " incr to " + c);
                            }
                        }
                    }
                    
                    
                }
            }
        } catch (e:MultipleExceptions) {
            Console.OUT.println("multiple exceptions");
            e.printStackTrace();
        } catch (e:Exception) {
            Console.OUT.println("single exception");
            e.printStackTrace();
        }
        
        if (gr().get() == 2n)
            Console.OUT.println("++ Test succeeded ++");
        else
            Console.OUT.println("!! Test failed !! ");
    }
}

