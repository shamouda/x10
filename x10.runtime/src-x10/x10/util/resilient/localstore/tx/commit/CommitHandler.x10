package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.TxMembers;
import x10.util.HashSet;
import x10.compiler.Pinned;
import x10.util.resilient.localstore.tx.logging.TxDescManager;

public abstract class CommitHandler[K] {K haszero} {
	public transient var phase1ElapsedTime:Long = 0;
    public transient var phase2ElapsedTime:Long = 0;
	public transient var txLoggingElapsedTime:Long = 0;

    public abstract def abort(recovery:Boolean):void;
    public abstract def commit(commitRecovery:Boolean):Int;
    
    protected plh:PlaceLocalHandle[LocalStore[K]];
    protected id:Long;
    protected mapName:String;
    protected members:TxMembers;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, mapName:String, members:TxMembers) {
    	this.plh = plh;
    	this.id = id;
    	this.mapName = mapName;
    	this.members = members;
    }
    
    protected def executeFlat(closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat started ...");
                
        for (p in members.pg()) {
            at (p) async {
                closure(plh, id);
            }
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat ended children ["+members.pg().size()+"] ...");
    }
    
    protected def executeRecursively(closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, parents:HashSet[Long], deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively started ...");
        closure(plh, id);
        var childCount:Long = 0;
        val childrenVirtual = plh().txDescManager.getVirtualMembers(id, TxDescManager.FROM_MASTER);
        try {
            if (childrenVirtual != null) {
                val myVirtualPlaceId = plh().getVirtualPlaceId();
                parents.add(myVirtualPlaceId);
                childCount = childrenVirtual.size;
                val txMembers = plh().getTxMembers( childrenVirtual , true);
                val virtual = txMembers.virtual;
                val physical = txMembers.places;
                for (var i:Long = 0; i < virtual.size ; i++) {
                    if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursively virtual(" + i + "/"+childCount+")="+virtual(i)+" containedInParent="+parents.contains(virtual(i))+" ...");
                    if (!parents.contains(virtual(i))) {
                        val p = physical(i);
                        if (p.isDead())
                            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursively target="+p+" DEAD ...");
                        
                        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " going to place ["+p+"]  ...");
                        at (p) async {
                            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " arrived to ["+p+"]  ...");
                            executeRecursively(closure, parents, deleteTxDesc);
                        }
                    }
                }
            }
        }finally {
            if (deleteTxDesc && childrenVirtual != null) {
                plh().txDescManager.delete(id, true);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively ended children ["+childCount+"] ...");
    }
    
}