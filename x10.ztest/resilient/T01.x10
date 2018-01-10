import x10.util.concurrent.AtomicInteger;

public class T01 {
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
                    val me = here;
                    at (gr) { 
                        val c = gr().incrementAndGet();
                        Console.OUT.println(me + " incr to " + c);
                    }
                }
                
                at (Place(2)) async {
                    think(1);
                    System.killHere();
                }
                
                at (Place(3)) async {
                    think(1);
                    val me = here;
                    at (gr) {
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
        
        if (gr().get() == 2n)
            Console.OUT.println("++ Test succeeded ++");
        else
            Console.OUT.println("!! Test failed !! ");
    }
}

