package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.TxMembers;
import x10.util.HashSet;

public abstract class CommitHandler {
	public transient var phase1ElapsedTime:Long = 0;
    public transient var phase2ElapsedTime:Long = 0;
	public transient var txLoggingElapsedTime:Long = 0;

    public abstract def abort(recovery:Boolean):void;
    public abstract def commit(commitRecovery:Boolean):Int;
    
    protected plh:PlaceLocalHandle[LocalStore];
    protected id:Long;
    protected mapName:String;
    protected members:TxMembers;
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers) {
    	this.plh = plh;
    	this.id = id;
    	this.mapName = mapName;
    	this.members = members;
    }
    
    /***************************************************************************************/
    protected def finalizeSlaves(commit:Boolean, deadMasters:ArrayList[Place], 
            plh:PlaceLocalHandle[LocalStore], id:Long, members:TxMembers) {
        
        //ask slaves to commit (their master died, 
        //and we need to resolve the slave's pending transactions)
        finish for (p in deadMasters) {
            assert(members.contains(p));
            val virtualPlace = members.getVirtualPlaceId(p);
            val slave = plh().getSlave(virtualPlace);
            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moving to slave["+slave+"] to " + ( commit? "commit": "abort" ));
            at (slave) async {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " finalizeSlaves here["+here+"] moved to slave["+here+"] to " + ( commit? "commit": "abort" ));
                if (commit)
                    plh().slaveStore.commit(id);
                else
                    plh().slaveStore.abort(id);
            }
        }
    }
    
    protected def getDeadPlaces(mulExp:MultipleExceptions, members:TxMembers) {
        val list = new ArrayList[Place]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                if (members.contains(dpe.place))
                    list.add(dpe.place);
            }
        }
        return list;
    }

    public static def getDeadAndConflictingPlaces(ex:Exception) {
        val list = new ArrayList[Place]();
        if (ex != null) {
            if (ex instanceof DeadPlaceException) {
                list.add((ex as DeadPlaceException).place);
            }
            else if (ex instanceof ConflictException) {
                list.add((ex as ConflictException).place);
            }
            else if (ex instanceof MultipleExceptions) {
                val mulExp = ex as MultipleExceptions;
                val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
                if (deadExList != null) {
                    for (dpe in deadExList) {
                        list.add(dpe.place);
                    }
                }
                val confExList = mulExp.getExceptionsOfType[ConflictException]();
                if (confExList != null) {
                    for (ce in confExList) {
                        list.add(ce.place);
                    }
                }
            }
        }
        return list;
    }
    
    protected def executeFlat(closure:(PlaceLocalHandle[LocalStore],Long)=>void, deleteTxDesc:Boolean) {
        for (p in members.pg()) {
            async at (p) {
                closure(plh, id);
            }
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
    }
    
    protected def executeRecursively(closure:(PlaceLocalHandle[LocalStore],Long)=>void, parents:HashSet[Long], deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursively started ...");
        closure(plh, id);
        var childCount:Long = 0;
        val childrenVirtual = plh().txDescManager.getVirtualMembers(id);
        if (childrenVirtual != null) {
            parents.add(here.id);
            childCount = childrenVirtual.size;
            val childrenPhysical = plh().getTxMembers( childrenVirtual , true);
            for (p in childrenPhysical.pg()) {
                if (!parents.contains(p.id)) {
                    async at (p) {
                        executeRecursively(closure, parents, deleteTxDesc);
                    }
                }
            }
        }
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursively ended children ["+childCount+"] ...");
    }
    
    protected def executeFlatSlaves(deadMasters:ArrayList[Place], closure:(PlaceLocalHandle[LocalStore],Long)=>void, deleteTxDesc:Boolean) {
        for (p in deadMasters) {
            val virtualPlace = members.getVirtualPlaceId(p);
            val slave = plh().getSlave(virtualPlace);
            async at (slave) {
                closure(plh, id);
            }
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
    }
    
}