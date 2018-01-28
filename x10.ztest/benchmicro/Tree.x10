public class Tree {
    static OUTER_ITERS = 100;
    static INNER_ITERS = 100;
    static MIN_NANOS = (10*1e9) as long; // require each test to run for at least 10 seconds (reduce jitter)
    public static def think(think:Long) {
        if (think == 0) return;
        val start = System.nanoTime();
        do {} while (System.nanoTime() - start < think);
    }
    
    private static def downTree(thinkTime:long):void {
        think(thinkTime);
        val parent = here.id;
        val child1 = parent*2 + 1;
        val child2 = child1 + 1;
        if (parent == 1 && Place.numPlaces() == 2) {
            // special case to invert tree so scaling calcuation is reasonable.
            finish at (Place(0)) async think(thinkTime);
        }
        if (child1 < Place.numPlaces() || child2 < Place.numPlaces()) {
            Console.OUT.println(here.id + "=> child1:" + child1 + " child2:" + child2 + "  started");
            finish {
                if (child1 < Place.numPlaces()) at (Place(child1)) async downTree(thinkTime);
                if (child2 < Place.numPlaces()) at (Place(child2)) async downTree(thinkTime);
            }
            Console.OUT.println(here.id + "=> child1:" + child1 + " child2:" + child2 + "  ended");
        }
    }
    
    public static def main (args:Rail[String]) {
        val t = 0;
        downTree(t);
    }
}
