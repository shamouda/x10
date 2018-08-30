/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.xrx;

import x10.util.Set;
import x10.util.HashSet;
import x10.util.concurrent.Future;
import x10.xrx.txstore.TxLocalStore;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.concurrent.LowLevelFinish;
import x10.compiler.Immediate;
import x10.compiler.Uncounted;
import x10.xrx.txstore.TxConfig;
import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.GrowableRail;
import x10.xrx.TxStoreConflictException;
import x10.xrx.TxStoreFatalException;

public class Tx(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {   
    static resilient = Runtime.RESILIENT_MODE > 0;
    
    protected transient var finishObj:Releasable = null;
    protected transient var members:Set[Int] = null;
    protected transient var readOnly:Boolean = false;      // transient is initialized as false by default    
    protected transient var lock:Lock = null;
    protected transient var count:Int = 0n;
    protected transient var vote:Boolean = false;          // transient is initialized as false by default
    protected transient var gr:GlobalRef[Tx];
    protected transient var excs:GrowableRail[CheckedThrowable];    //fatal exception or Conflict exception
    
    private transient var gcGR:GlobalRef[FinishState];
    
    protected def this(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
        property(plh, id);
    }
    
    public static def make(plh:PlaceLocalHandle[TxLocalStore[Any]], id:Long) {
        if (resilient)
            if (TxConfig.DISABLE_SLAVE)
                return new TxResilientNoSlaves(plh, id);
            else
                return new TxResilient(plh, id);
        else
            return new Tx(plh, id);
    }
    
    public static def clone(old:Tx) {
        if (old instanceof TxResilientNoSlaves) {
            return new TxResilientNoSlaves(old.plh, old.id);
        } else if (old instanceof TxResilient) {
            return new TxResilient(old.plh, old.id);
        } else if (old instanceof Tx) {
            return new Tx(old.plh, old.id);
        } else 
            return null;
    }
    /**
     * Initializing the transaction object.
     * Must be called at the place responsible for 2PC
     * **/
    public def initialize(fgr:GlobalRef[FinishState]) {
        gcGR = fgr;
        lock = new Lock();
        gr = GlobalRef[Tx](this);
        vote = true;
        readOnly = true;
    }
    
    /**
     * Used in resilient mode only
     * */
    public def initialize(dummy:FinishResilient.Id, backupId:Int) { }
    public def initializeNewMaster(dummy:FinishResilient.Id, _mem:Set[Int], _excs:GrowableRail[CheckedThrowable], _ro:Boolean, _backupId:Int) {}
    public def getBackupClone():Tx { return null; }
    
    protected def addExceptionUnsafe(t:CheckedThrowable) {
        if (excs == null) excs = new GrowableRail[CheckedThrowable]();
        excs.add(t);
    }
    
    protected def getAddMemberPrintMsg(m:Int, ro:Boolean, tag:String) {
        return "Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] "+tag+"["+m+"] readOnly["+ro+"] ...";
    }
    
    /***************** Members *****************/
    public def addMember(m:Int, ro:Boolean, tag:Int){
        if (TxConfig.TM_DEBUG) 
            Console.OUT.println(getAddMemberPrintMsg(m, ro, "add member"));
        if (lock != null) {
            lock.lock();
            if (members == null)
                members = new HashSet[Int]();
            members.add(m);
            readOnly = readOnly & ro;
            lock.unlock();
        } else {
            Console.OUT.println(here + "Tx["+id+"] " + TxConfig.txIdToString (id)+ " obj["+this+"] WARNING Tx.addMember lock is null add member["+m+"] tag["+tag+"]");
        }
    }
    
    public def addSubMembers(subMembers:Set[Int], subReadOnly:Boolean, tag:Int) {
        if (subMembers == null)
            return;
        lock.lock();
        if (members == null)
            members = new HashSet[Int]();
        for (s in subMembers) {
            if (TxConfig.TM_DEBUG) 
                Console.OUT.println(getAddMemberPrintMsg(s, subReadOnly, "tag["+tag+"] add sub member"));
            members.add(s);
        }
        readOnly = readOnly & subReadOnly;
        lock.unlock();
    }
    
    public def addMembers(otherTx:Tx) {
        lock.lock();
        if (otherTx.members != null) {
            this.members.addAll(otherTx.members);
        }
        lock.unlock();
    }
    
    public def contains(place:Int) {
        if (members == null)
            return false;
        return members.contains(place);
    }
    
    public def getMembers() = members;
    
    public def isReadOnly() = readOnly;
    
    public def isEmpty() = members == null || members.size() == 0;
    
    /***************** Get ********************/
    public def get(key:Any):Cloneable {
        Runtime.activity().tx = true;
        return plh().getMasterStore().get(id, key);
    }
    
    /***************** PUT ********************/
    public def put(key:Any, value:Cloneable):Cloneable {
        Runtime.activity().tx = true;
        Runtime.activity().txReadOnly = false;
        return plh().getMasterStore().put(id, key, value);
    }
    
    /***************** Delete *****************/
    public def delete(key:Any):Cloneable {
        Runtime.activity().tx = true;
        Runtime.activity().txReadOnly = false;
        return plh().getMasterStore().delete(id, key);
    }
    
    /***************** KeySet *****************/
    public def keySet():Set[Any] {
        Runtime.activity().tx = true;
        return plh().getMasterStore().keySet(id); 
    }
    
    public def asyncAt(virtualPlace:Long, closure:()=>void) {
        val pl = plh().getPlace(virtualPlace);
        if (pl.id == here.id)
            async closure();
        else
            at (pl) async closure();
    }
    
    /********** Finalizing a transaction **********/
    public def finalize(finObj:Releasable, abort:Boolean) {
        if (TxConfig.TM_DEBUG)
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] finalize abort="+abort+" ...");
        this.finishObj = finObj;
        nonResilient2PC(abort);
    }
    
    public def finalizeLocal(finObj:Releasable, abort:Boolean) {
        if (TxConfig.TM_DEBUG)
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] finalizeLocal abort="+abort+" ...");
        this.finishObj = finObj;
        nonResilientLocal(abort);
    }
    
    public def finalizeWithBackup(finObj:Releasable, abort:Boolean, backupId:Int, isRecovered:Boolean) {
        finalize(finObj, abort);
    }
    
    private def nonResilientLocal(abort:Boolean) {
        var vote:Boolean = true;
        if (!abort) {
            try {
                plh().getMasterStore().validate(id);
                plh().getMasterStore().commit(id);
            } catch (e:Exception) {
                vote = false;
                addExceptionUnsafe(new TxStoreConflictException());
            }
        }
        if (abort || !vote) {
            plh().getMasterStore().abort(id);
        }
        release();
    }
    
    private def nonResilient2PC(abort:Boolean) {
        count = members.size() as Int;
        
        if (abort) {
            commitOrAbort(false);
        } else {
            if (TxConfig.VALIDATION_REQUIRED)
                prepare();
            else
                commitOrAbort(true);
        }
    }
    
    protected def prepare() {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val plh = this.plh;
        
        for (p in members) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] prepare["+p+"] totalMembers["+members.size()+"] ...");
            at (Place(p)) @Immediate("prep_request") async {
                //validate may block, so we cannot do it in this immediate thread
                Runtime.submitUncounted(()=> {
                    var vote:Boolean = true;
                    try {
                        plh().getMasterStore().validate(id);
                    } catch (e:Exception) {
                        vote = false;
                    }
                    if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] prepare["+p+"] vote["+vote+"] ...");
                    val v = vote;
                    at (gr) @Immediate("prep_response") async {
                        gr().notifyPrepare(v);
                    }
                });
            }
        }
    }
    
    protected def commitOrAbort(isCommit:Boolean) {
        //don't copy this
        val gr = this.gr;
        val id = this.id;
        val gcGR = this.gcGR;
        val plh = this.plh;
        
        for (p in members) {
            if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] commitOrAbort["+p+"] totalMembers["+members.size()+"] ...");
            at (Place(p)) @Immediate("comm_request") async {
                //gc
                Runtime.finishStates.remove(gcGR);
                if (isCommit)
                    plh().getMasterStore().commit(id);
                else
                    plh().getMasterStore().abort(id);
                at (gr) @Immediate("comm_response") async {
                    gr().notifyAbortOrCommit();
                }
            }
        }        
    }
    
    protected def notifyPrepare(v:Boolean) {
        var prep:Boolean = false;
    
        lock.lock();
        count--;
        
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] notifyPrepare vote["+v+"] count="+count+"...");
        vote = vote & v;
        if (count == 0n) {
            prep = true;
            count = members.size() as Int;
            if (!vote)
                addExceptionUnsafe(new TxStoreConflictException());
        }
        lock.unlock();
        
        if (prep) {
            commitOrAbort(vote);
        }
    }
    
    protected def notifyAbortOrCommit() {
        var rel:Boolean = false;
        lock.lock();
        count--;
        if (TxConfig.TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] obj["+this+"] notifyAbortOrCommit count="+count+"...");
        if (count == 0n) {
            rel = true;
        }
        lock.unlock();
        
        if (rel) {
            release();
        }
    }
    
    protected def release() {
        if (TxConfig.TM_DEBUG) 
            Console.OUT.println("Tx["+id+"] " + TxConfig.txIdToString (id)+ " here["+here+"] transaction releasing finish ...");
        finishObj.releaseFinish(excs);
        (gr as GlobalRef[Tx]{self.home == here}).forget();
    }
    
}
