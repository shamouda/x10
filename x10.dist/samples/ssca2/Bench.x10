import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.util.resilient.PlaceManager;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.CloneableLong;

public class Bench {
    public static def main(args:Rail[String]):void {
        val mgr = new PlaceManager(0, false);
        val activePlaces = mgr.activePlaces();
        val store = TxStore.make(activePlaces, false, null);
        val places= activePlaces.size();
        Console.OUT.println("Starting Bench with "+places+" places");
        Console.OUT.println("X10_RESILIENT_MODE=" + x10.xrx.Runtime.RESILIENT_MODE);
        val OUTER = 30;
        val INNER = 10;
        val TX_PLACES = places / 3;
        val time0 = System.nanoTime();
        for (var i:Long = 0; i < OUTER; i++) {
            val t1 = System.nanoTime();
            val tx = store.makeTx();
            finish {
                Runtime.registerFinishTx(tx, true);
                for (var j:Long = 0; j < INNER; j++) {
                    finish {
                        Runtime.registerFinishTx(tx, false);
                        for (var k:Long = 0; k < TX_PLACES; k++) {
                            val dst = ((j * TX_PLACES) + k) % places;
                            val key = "p"+i+j+k;
                            at (Place(dst)) async {
                                tx.put(key, new CloneableLong(1));
                            }
                        }
                    }
                    //Console.OUT.println("iter["+i+"]: finish"+j+" completed");
                }
            }
            val t2 = System.nanoTime();
            Console.OUT.println("iter["+i+"]:" + (t2-t1)/1E9+" seconds");
        }
        val time1 = System.nanoTime();
        Console.OUT.println("Average: "+(time1-time0)/1E9/OUTER+" seconds");
    }
}