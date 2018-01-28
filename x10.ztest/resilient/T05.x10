import x10.util.concurrent.AtomicInteger;
//x10c++ T05.x10 -o t05.o
//X10_RESILIENT_VERBOSE=0 X10_RESILIENT_MODE=14 X10_NPLACES=4 ./t05.o
public class T05 {
    static OUTER_ITERS = 100;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)
    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }   
   
    public static def main (args:Rail[String]) {
        //for (i in 0..0) 
        val i = 1;
    	{
            val gr = new GlobalRef[AtomicInteger](new AtomicInteger(0n));
            try {
                finish {
                    at (Place(1)) async {
                        think(1);
                        val me = here;
                        at (gr) async { 
                            val c = gr().incrementAndGet();
                            Console.OUT.println(me + " incr to " + c);
                        }
                    }
                    
                    at (Place(2)) async {
                        think(1);
                        val me = here;
                        at (gr) async {
                            val c = gr().incrementAndGet();
                            Console.OUT.println(me + " incr to " + c);
                        }
                    }
                    
                    at (Place(3)) async {
                        think(1);
                        val me = here;
                        at (gr) async {
                            val c = gr().incrementAndGet();
                            Console.OUT.println(me + " incr to " + c);
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
            
            if (gr().get() == 3n)
                Console.OUT.println("++ Test["+i+"] succeeded ++");
            else
                Console.OUT.println("!! Test["+i+"] failed !! ");
        }
    }
}

