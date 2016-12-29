import x10.util.Random;
import x10.compiler.Uncounted;

public class DummyFinishUncountAsync {
    public static def main(args:Rail[String]) {
        if (args.size != 1) {
            Console.OUT.println("missing parameter");
            return;
        }        
        val repeat = Long.parseLong(args(0));
        Console.OUT.println("Starting DummyFinishUncountAsync benchmark - repeat="+repeat);
        val start = System.nanoTime();
        finish for (p in Place.places()) at (p) async {
            val rand = new Random(System.nanoTime());
            for (i in 1..repeat) {
                val s = selectRandomPlaces(here, rand);
                val p1 = s.p1;
                val p2 = s.p2;               
                at (p1) @Uncounted async { }
                at (p2) @Uncounted async { }
            }    
        }
        val end = System.nanoTime();
        val totalTime = (end-start)/1e9;
        Console.OUT.println("ProcessingTime:"+ totalTime + " seconds");
    }
    
    private static def selectRandomPlaces(src:Place, rand:Random):SelectedPlaces {
        val p1 = Math.abs(rand.nextLong()% Place.numPlaces());
        var p2:Long = p1;
        while (p1 == p2) {
            p2 = Math.abs(rand.nextLong()% Place.numPlaces());
        }
        return new SelectedPlaces(Place(p1), Place(p2));
    }
}

class SelectedPlaces(p1:Place, p2:Place) {
    
}

class TmpResult {
    var result:Long = -1;
}