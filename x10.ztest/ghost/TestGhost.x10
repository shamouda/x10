public class TestGhost {
    private static val ITERATIONS:Long = System.getenv("GHOST_ITER") == null ? 100 : Long.parseLong(System.getenv("GHOST_ITER"));
    
    public static def main (args:Rail[String]) {
        val home = Place(0);
        val places = Place.places();
        val numPlaces = Place.numPlaces();
        val size = 1024;
        
        /*will be sent to all places*/
        val broadcastRail = new Rail[Double](size, 1024.0);
        
        /*a rail at each place to receive data from place 0*/
        val plh = PlaceLocalHandle.make[Rail[Double]](places, () => new Rail[Double](size, 0));
        
        /*place0: global pointers to rails at other places*/
        val dummy = GlobalRail[Double](new Rail[Double](0));
        val remoteRecvBuffers:Rail[GlobalRail[Double]]{self!=null};
        remoteRecvBuffers = new Rail[GlobalRail[Double]](numPlaces, dummy);
        
        val p0GR = GlobalRef[Rail[GlobalRail[Double]]](remoteRecvBuffers);
        /*place0 obtaining the correct global pointers from remote places*/
        finish {
            for (place in places) {
                at (place) async {
                    val realRail = plh();
                    val gr = GlobalRail[Double](realRail);
                    val indx = here.id;
                    at (p0GR) async {
                        p0GR()(indx)  = gr;
                        Console.OUT.println(here + " received global rail ref from Place("+indx+")");
                    }
                }
            }
        }
        Console.OUT.println(here + " finished obtaining global rails");

        val src = broadcastRail;
        for (iter in 1..ITERATIONS) {
            finish {
                for (place in places) {
                    val dst = remoteRecvBuffers(place.id);
                    Rail.asyncCopy(src, 0, dst, 0, size);
                    //Console.OUT.println(here + " issued asyncCopy to " + place);
                }
            }
            Console.OUT.println(here + " completed iter "+iter+" ...");
        }
        
        Console.OUT.println(here + " finished sending data, validating ...");
        
        finish {
            for (place in places) at (place) async {
                val local = plh();
                for (i in 0..(size-1)) {
                    if (local(i) != 1024.0) {
                        Console.OUT.println(here + " FATAL wrong value found: " + local(i));
                    }
                }
            }
        }
        Console.OUT.println(here + " finished sending data, validation completed");
    }
}