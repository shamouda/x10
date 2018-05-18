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
import x10.xrx.Runtime;

public abstract class CommitHandler[K] {K haszero} {
    public transient var phase1ElapsedTime:Long = 0;
    public transient var phase2ElapsedTime:Long = 0;
    public transient var txLoggingElapsedTime:Long = 0;

    public abstract def abort(recovery:Boolean):void;
    public abstract def commit(commitRecovery:Boolean):Int;
    
    protected plh:PlaceLocalHandle[LocalStore[K]];
    protected id:Long;
    protected var members:TxMembers;
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long, members:TxMembers) {
        this.plh = plh;
        this.id = id;
        this.members = members;
    }
    
    public def this(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        this.plh = plh;
        this.id = id;
        this.members = null;
    }
    
    protected def finishFlat(closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat started ...");
        
        val places:Rail[Long];
        if (members != null){
            places = members.places;
        } else {
            val childrenVirtual = plh().txDescManager.getVirtualMembers(id, TxDescManager.FROM_MASTER);
            members = plh().getTxMembersIncludingDead(childrenVirtual);
            places = members.places;
        }
        
        finish for (p in places) at (Place(p)) async {
            closure(plh, id);
        }
        
        if (deleteTxDesc) {
            plh().txDescManager.delete(id, true);
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeFlat ended ...");
    }
    
    protected def executeRecursively(closure:(PlaceLocalHandle[LocalStore[K]],Long)=>void, parents:HashSet[Long], deleteTxDesc:Boolean) {
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively started , calling closure(); ...");
        closure(plh, id);
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively started , calling closure(); completed ...");
        var childCount:Long = 0;
        val childrenVirtual = plh().txDescManager.getVirtualMembers(id, TxDescManager.FROM_MASTER);
        try {
            if (childrenVirtual != null) {
                val myVirtualPlaceId = plh().getVirtualPlaceId();
                parents.add(myVirtualPlaceId);
                childCount = childrenVirtual.size();
                val txMembers = plh().getTxMembersIncludingDead( childrenVirtual );
                val virtual = txMembers.virtual;
                val physical = txMembers.places;
                for (var i:Long = 0; i < virtual.size ; i++) {
                    if (!parents.contains(virtual(i))) {
                        val p = Place(physical(i));
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
        } finally {
            if (deleteTxDesc && childrenVirtual != null) {
                plh().txDescManager.delete(id, true);
            }
        }
        if (TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here[" + here + "] executeRecursively ended children ["+childCount+"] ...");
    }
    
    protected def validate_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().validate(id);
    }
    
    protected def commit_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().commit(id);
    }
    
    protected def abort_local(plh:PlaceLocalHandle[LocalStore[K]], id:Long) {
        plh().getMasterStore().abort(id);
    }
    
    public def clean() {
        
    }
}