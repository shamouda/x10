import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.util.resilient.PlaceManager;
import x10.xrx.Runtime;

public class BenchRail {
    public static def main(args:Rail[String]):void {
        val mgr = new PlaceManager(0, false);
        val activePlaces = mgr.activePlaces();
        val places= activePlaces.size();
        Console.OUT.println("Starting BenchRail with "+places+" places");
        Console.OUT.println("X10_RESILIENT_MODE=" + x10.xrx.Runtime.RESILIENT_MODE);
        val sizePerPlace = 3;
        val size = sizePerPlace*places;
        val store = TxStore.makeRail(activePlaces, false, null, sizePerPlace, (i:Long)=> here.id);
        val tx = store.makeTx();
        Console.OUT.println("BenchRail starting ...");
        finish {
            Runtime.registerFinishTx(tx, true);
            for (var i:Long = 0; i < size; i++) {
                val dest = i / sizePerPlace;
                val index = i;
                finish {
                    Runtime.registerFinishTx(tx, false);
                    at (Place(dest)) async {
                        val v = tx.getRail(index) as Long;
                        Console.OUT.println(here + " Rail["+index+"] = " + v);
                        tx.putRail(index, v + 1);
                    }
                }
            }
        }
        Console.OUT.println("BenchRail phase 1 completed ...");
        val tx2 = store.makeTx();
        finish {
            Runtime.registerFinishTx(tx2, true);
            for (var i:Long = 0; i < size; i++) {
                finish {
                    Runtime.registerFinishTx(tx2, false);
                    val dest = i / sizePerPlace;
                    val index = i;
                    at (Place(dest)) async {
                        val v = tx2.getRail(index);
                        Console.OUT.println(here + " Rail["+index+"] = " + v);
                    }
                }
            }
        }
        Console.OUT.println("BenchRail completed ...");
    }
}