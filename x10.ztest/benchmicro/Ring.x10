public class Ring {
    static OUTER_ITERS = 100;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)
    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
    
    private static def ring(thinkTime:long, destination:Place):void {
        think(thinkTime);
        if (destination == here) return;
        val nextHop = Place.places().next(here);
        at (nextHop) ring(thinkTime, destination);
    }
    
    public static def main (args:Rail[String]) {
        val t = 0;
        val endPlace = Place.places().prev(here);
        ring(t, endPlace);
    }
}