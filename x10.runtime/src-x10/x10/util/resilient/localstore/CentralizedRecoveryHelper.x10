package x10.util.resilient.localstore;

import x10.util.ArrayList;
import x10.util.concurrent.Lock;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.resilient.localstore.tx.TxDesc;

public class CentralizedRecoveryHelper {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    /*******************  Centralized Recovery At Place(0)  ****************************/
    public static def recover(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription):void {
        checkIfBothMasterAndSlaveDied(changes);
        
        recoverTransactions(plh, changes);
        
        recoverMasters(plh, changes);
        
        recoverSlaves(plh, changes);
    }
    
    private static def checkIfBothMasterAndSlaveDied(changes:ChangeDescription) {
        for (dp in changes.removedPlaces) {
            val slave = changes.oldActivePlaces.next(dp);
            if (changes.removedPlaces.contains(slave)) {
                val virtualId = changes.oldActivePlaces.indexOf(dp);
                throw new Exception("Fatal: both master and slave were lost for virtual place["+virtualId+"] ");
            }
        }
    }

    private static def recoverTransactions(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription) {
        Console.OUT.println("recoverTransactions started");
        finish {
            val oldActivePlaces = changes.oldActivePlaces;
            for (deadPlace in changes.removedPlaces) {
                val slave = oldActivePlaces.next(deadPlace);
                Console.OUT.println("recoverTransactions deadPlace["+deadPlace+"] moving to its slave["+slave+"] ");
                at (slave) async {
                    val txDescMap = plh().slaveStore.getSlaveMasterState();
                    if (txDescMap != null) {
                        val set = txDescMap.keySet();
                        val iter = set.iterator();
                        while (iter.hasNext()) {
                            val txId = iter.next();
                            if (txId.contains("_TxDesc_")) {
                                val obj = txDescMap.get(txId);
                                if (obj != null) {
                                    val txDesc = obj as TxDesc;
                                    val map = new ResilientNativeMap(txDesc.mapName, plh);
                                    if (TM_DEBUG) Console.OUT.println(here + " recovering txdesc " + txDesc);
                                    val tx = map.restartGlobalTransaction(txDesc);
                                    if (txDesc.status == TxDesc.COMMITTING) {
                                        if (TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] commit it");
                                        tx.commit(true); //ignore phase one
                                    }
                                    else if (txDesc.status == TxDesc.STARTED) {
                                        if (TM_DEBUG) Console.OUT.println(here + " recovering Tx["+tx.id+"] abort it");
                                        tx.abort();
                                    }
                                }
                            }
                        }
                    }
                    
                    if (TxConfig.getInstance().TM_REP.equals("lazy"))
                        applySlaveTransactions(plh, changes);
                }
            }
        }
        Console.OUT.println("recoverTransactions completed");
    }
    
    private static def recoverMasters(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription) {
        finish {
            for (newMaster in changes.addedPlaces) {
                val active = changes.newActivePlaces;
                val slave = changes.newActivePlaces.next(newMaster);
                Console.OUT.println("recovering masters: newMaster["+newMaster+"] slave["+slave+"] ");
                at (slave) async {                    
                    val map = plh().slaveStore.getSlaveMasterState();
                    at (newMaster) async {
                        plh().joinAsMaster(active, map);
                    }
                }
            }
        }
    }

    private static def recoverSlaves(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription) {
        finish {
            for (newSlave in changes.addedPlaces) {
                val master = changes.newActivePlaces.prev(newSlave);
                Console.OUT.println("recovering slaves: master["+master+"] newSlave["+newSlave+"] ");
                at (master) async {
                    val masterState = plh().masterStore.getState().getKeyValueMap();
                    at (newSlave) {
                        plh().slaveStore.addMasterPlace(masterState);
                    }
                    plh().slave = newSlave;
                }
            }
        }
    }
    
    private static def applySlaveTransactions(plh:PlaceLocalHandle[LocalStore], changes:ChangeDescription) {
        val committed = GlobalRef(new ArrayList[Long]());
        val committedLock = GlobalRef(new Lock());
        val root = here;
        
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            val placeTxsMap = plh().slaveStore.clusterTransactions();
            finish {
                val iter = placeTxsMap.keySet().iterator();
                while (iter.hasNext()) {
                    val placeIndex = iter.next();
                    val txList = placeTxsMap.getOrThrow(placeIndex);
                    var pl:Place = changes.oldActivePlaces(placeIndex);
                    var master:Boolean = true;
                    if (pl.isDead()){
                        pl = changes.oldActivePlaces.next(pl);
                        master = false;
                    }
                    val isMaster = master;
                    at (pl) async {
                        var committedList:ArrayList[Long]; 
                        if (isMaster)
                            committedList = plh().masterStore.filterCommitted(txList);
                        else
                            committedList = plh().slaveStore.filterCommitted(txList);
                        
                        val cList = committedList;
                        at (root) {
                            committedLock().lock();
                            committed().addAll(cList);
                            committedLock().unlock();
                        }
                    }
                }
            }
        }
        
        val orderedTx = plh().slaveStore.getPendingTransactions();
        if (TxConfig.getInstance().VALIDATION_REQUIRED) {
            val commitTxOrdered = new ArrayList[Long]();
            for (val tx in orderedTx){
                if (committed().contains(tx))
                    commitTxOrdered.add(tx);
            }
            plh().slaveStore.commitAll(commitTxOrdered);
        }
        else
            plh().slaveStore.commitAll(orderedTx);
    }
}