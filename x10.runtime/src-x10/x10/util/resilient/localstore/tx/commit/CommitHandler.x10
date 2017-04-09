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
    
    protected def executeFlat(closure:(PlaceLocalHandle[LocalStore],Long)=>void, deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat started ...");
                
        for (p in members.pg()) {
            async at (p) {
                closure(plh, id);
            }
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat ended children ["+members.pg().size()+"] ...");
    }
    
    protected def executeRecursively(closure:(PlaceLocalHandle[LocalStore],Long)=>void, parents:HashSet[Long], deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively started ...");
        closure(plh, id);
        var childCount:Long = 0;
        val childrenVirtual = plh().txDescManager.getVirtualMembers(id, true);
        try {
            if (childrenVirtual != null) {
                parents.add(here.id);
                childCount = childrenVirtual.size;
                val childrenPhysical = plh().getTxMembers( childrenVirtual , true);
                for (p in childrenPhysical.pg()) {
                    if (!parents.contains(p.id)) {
                        if (p.isDead())
                            if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " " + here + " executeRecursively target="+p+" DEAD ...");                    
                        
                        async at (p) {
                            executeRecursively(closure, parents, deleteTxDesc);
                        }
                    }
                }
            }
        }finally {
            if (deleteTxDesc) {
                plh().txDescManager.delete(id, true);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively ended children ["+childCount+"] ...");
    }
    
    

    
    
}