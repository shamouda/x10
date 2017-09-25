package x10.util.resilient.localstore.recovery;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.tx.logging.TxDesc;
import x10.util.resilient.localstore.*;

public class CentralizedRecoveryHelper {
    
    /*******************  Centralized Recovery starting at Place(0)  ****************************/
    public static def recover[K](plh:PlaceLocalHandle[LocalStore[K]], changes:ChangeDescription) {K haszero} {
    	val recoveryStart = System.nanoTime();
        var i:Long = 0;
        finish for (deadPlace in changes.removedPlaces) {
            val masterOfDeadSlave = changes.oldActivePlaces.prev(deadPlace);
            val spare = changes.addedPlaces.get(i++);
            Console.OUT.println("recovering " + deadPlace + "  through its master " + masterOfDeadSlave);
            at (masterOfDeadSlave) async {
                plh().recoverSlave(deadPlace, spare);
            }
        }
        val newActivePlaces = changes.newActivePlaces;
        Place.places().broadcastFlat(()=> {
        	plh().activePlaces = newActivePlaces;
        });
        val recoveryEnd = System.nanoTime();
        Console.OUT.printf("CentralizedRecoveryHelper.recover completed, recoveryTime %f seconds\n", (recoveryEnd-recoveryStart)/1e9);
    }
    
}