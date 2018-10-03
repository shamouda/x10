import x10.xrx.TxStore;
import x10.xrx.Tx;
import x10.util.resilient.PlaceManager;
import x10.xrx.Runtime;
import x10.util.resilient.localstore.CloneableLong;

public class Test {
    static class Result {
        var count:Long = 0;
    }
    
    public static def main(args:Rail[String]):void {
        val mgr = new PlaceManager(0, false);
        val activePlaces = mgr.activePlaces();
        val store = TxStore.make(activePlaces, false, null);
        
        val gr = new GlobalRef[Result](new Result());
        
        val tx = store.makeTx();
        finish {
            Runtime.registerFinishTx(tx, true);
            at (Place(1)) async {
                tx.put("p1", new CloneableLong(1));
                
                finish 
                {
                    Runtime.registerFinishTx(tx, false);
                    at (Place(2)) async {
                        tx.put("p2", new CloneableLong(2));
                        
                        at (gr) async {
                            atomic { gr().count ++; }
                        }
                    }
                    
                    at (Place(3)) async {
                        tx.put("p3", new CloneableLong(3));
                        
                        at (gr) async {
                            atomic { gr().count ++; }
                        }
                    }
                }
                
                at (gr) async {
                    atomic { gr().count ++; }
                }
            }
        }
        Console.OUT.println("Found count = " + gr().count + " expected 3");
    }
}