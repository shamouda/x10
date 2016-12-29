import x10.util.Random;
import x10.compiler.Uncounted;

public class DummyFinish {
    public static def main(args:Rail[String]) {
        if (args.size != 1) {
            Console.OUT.println("missing parameter");
            return;
        }
        val repeat = Long.parseLong(args(0));
        val start = System.nanoTime();
        finish for (p in Place.places()) at (p) async {
            val rand = new Random(System.nanoTime());
            for (i in 1..repeat) {
                val s = selectRandomPlaces(here, rand);
                val p1 = s.p1;
                val p2 = s.p2;
                /*
                val p1val = new TmpResult();
                val p2val = new TmpResult();
                
                val p1GR = new GlobalRef[TmpResult](p1val);
                val p2GR = new GlobalRef[TmpResult](p2val);
                */
                at (p1) @Uncounted async {
                    /*at (p1GR) {
                        atomic p1GR().result = 1;
                    }*/
                }
                
                at (p2) @Uncounted async {
                    /*at (p2GR) {
                        atomic p2GR().result = 1;
                    }*/
                }
                
                //when(p1val.result != -1 && p2val.result != -1);
            }    
        }
        val end = System.nanoTime();
        val totalTime = (end-start)/1e9;
        Console.OUT.println("Total Time:"+ totalTime + " seconds");
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