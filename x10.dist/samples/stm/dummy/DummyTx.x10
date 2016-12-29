import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.Set;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.compiler.Pinned;
import x10.util.GrowableRail;
import x10.util.Timer;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.*;
import x10.compiler.Uncounted;
import x10.compiler.Immediate;


public class DummyTx (id:Long, mapName:String, members:PlaceGroup) {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");

    private val root = GlobalRef[DummyTx](this);

    public transient val startTime = System.nanoTime();
    public transient var commitTime:Long = -1;
    public transient var abortTime:Long = -1;
    
    private transient val excs:GrowableRail[CheckedThrowable]; 
    
    /* resilient mode variables */
    private transient var aborted:Boolean = false;
    private transient var txDescMap:ResilientNativeMap;
    private transient var activePlaces:PlaceGroup;
    
    /* Constants */
    public static val SUCCESS = 0n;
    public static val SUCCESS_RECOVER_STORE = 1n;

    private static val GET_LOCAL = 0n;
    private static val GET_REMOTE = 1n;
    private static val PUT_LOCAL = 2n;
    private static val PUT_REMOTE = 3n;
    private static val DELETE_LOCAL = 4n;
    private static val DELETE_REMOTE = 5n;
    private static val KEYSET_LOCAL = 6n;
    private static val KEYSET_REMOTE = 7n;
    private static val AT_VOID = 8n;
    private static val AT_RETURN = 9n;
    private static val ASYNC_GET = 10n;
    private static val ASYNC_PUT = 11n;
    private static val ASYNC_DELETE = 12n;
    private static val ASYNC_KEYSET = 13n;
    private static val ASYNC_AT_VOID = 14n;
    private static val ASYNC_AT_RETURN = 15n;
    
    public def this(id:Long, mapName:String, members:PlaceGroup) {
        property(id, mapName, members);
        excs = new GrowableRail[CheckedThrowable]();
        var membersStr:String = "";
        for (p in members)
            membersStr += p +" ";
        if (TM_DEBUG) Console.OUT.println("TX["+id+"] started   members["+membersStr+"]");
    }
    
    /***************** Get ********************/
    public def get(key:String):Cloneable {
        return execute(GET_LOCAL, here, key, null, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def getRemote(dest:Place, key:String):Cloneable {
        return execute(GET_REMOTE, dest, key, null, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncGetRemote(dest:Place, key:String):TxFuture {
        return execute(ASYNC_GET, dest, key, null, null, null,  id, mapName, members, root).future;
    }
    
    private def getLocal(key:String,  id:Long, mapName:String):Cloneable {
        return new BankAccount(10);
    }
    
    /***************** PUT ********************/
    public def put(key:String, value:Cloneable):Cloneable {
        return execute(PUT_LOCAL, here, key, value, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def putRemote(dest:Place, key:String, value:Cloneable):Cloneable {
        return execute(PUT_REMOTE, dest, key, value, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncPutRemote(dest:Place, key:String, value:Cloneable):TxFuture {
        return execute(ASYNC_PUT, dest, key, value, null, null,  id, mapName, members, root).future;
    }
    
    private def putLocal(key:String, value:Cloneable,  id:Long, mapName:String):Cloneable {
        return new BankAccount(10);
    }
    
    /***************** Delete ********************/
    public def delete(key:String):Cloneable {
        return execute(DELETE_LOCAL, here, key, null, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def deleteRemote(dest:Place, key:String):Cloneable {
        return execute(DELETE_REMOTE, dest, key, null, null, null,  id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncDeleteRemote(dest:Place, key:String):TxFuture {
        return execute(ASYNC_DELETE, dest, key, null, null, null,  id, mapName, members, root).future;
    }
    
    private def deleteLocal(key:String,  id:Long, mapName:String):Cloneable {
        return new BankAccount(10);
    }
    
    /***************** KeySet ********************/
    public def keySet():Set[String] {
        return execute(KEYSET_LOCAL, here, null, null, null, null,  id, mapName, members, root).set; 
    }
    
    public def keySetRemote(dest:Place):Set[String] {
        return execute(KEYSET_REMOTE, dest, null, null, null, null,  id, mapName, members, root).set; 
    }
    
    public def asyncKeySetRemote(dest:Place):TxFuture {
        return execute(ASYNC_KEYSET, dest, null, null, null, null,  id, mapName, members, root).future; 
    }
    
    private def keySetLocal( id:Long, mapName:String):Set[String] {
        return null;
    }
    
    /***************** At ********************/
    public def syncAt(dest:Place, closure:()=>void) {
        execute(AT_VOID, dest, null, null, closure, null,  id, mapName, members, root);
    }
    
    public def syncAt(dest:Place, closure:()=>Any):Cloneable {
        return execute(AT_RETURN, dest, null, null, null, closure,  id, mapName, members, root).value as Cloneable;
    }
    
    public def asyncAt(dest:Place, closure:()=>void):TxFuture {
        return execute(ASYNC_AT_VOID, dest, null, null, closure, null,  id, mapName, members, root).future;
    }
    
    public def asyncAt(dest:Place, closure:()=>Any):TxFuture {
        return execute(ASYNC_AT_RETURN, dest, null, null, null, closure,  id, mapName, members, root).future;
    }
    
    /***************** Execution of All Operations ********************/
    private def execute(op:Int, dest:Place, key:String, value:Cloneable, closure_void:()=>void, closure_return:()=>Any, 
             id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[DummyTx]):TxOpResult {
        assert (members.contains(dest));
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Start Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
        val startExec = System.nanoTime();
        try {
            if (op == GET_LOCAL) {
                return new TxOpResult(getLocal(key,  id, mapName));
            }
            else if (op == GET_REMOTE) {
                return new TxOpResult (at (dest) getLocal(key,  id, mapName));
            }
            else if (op == PUT_LOCAL) {
                return new TxOpResult(putLocal(key, value,  id, mapName)); 
            }
            else if (op == PUT_REMOTE) {
                return new TxOpResult(at (dest) putLocal(key, value,  id, mapName));
            }
            else if (op == DELETE_LOCAL) {
                return new TxOpResult(deleteLocal(key,  id, mapName)); 
            }
            else if (op == DELETE_REMOTE) {
                return new TxOpResult(at (dest) deleteLocal(key,  id, mapName));
            }
            else if (op == KEYSET_LOCAL) {
                return new TxOpResult(keySetLocal( id, mapName));
            }
            else if (op == KEYSET_REMOTE) {
                return new TxOpResult(at (dest) keySetLocal( id, mapName));
            }
            else if (op == AT_VOID) {
                at (dest) closure_void();
                return null;
            }
            else if (op == AT_RETURN) {
                return new TxOpResult(at (dest) closure_return());
            }
            else {  /*Remote Async Operations*/
                val fid = 10;
                val future = new TxFuture(id, fid, dest);
                val gr = new GlobalRef(future);
                
                try {
                    if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Op["+opDesc(op)+"] going to move to dest["+dest+"] key["+key+"] ...");
                    at (dest) @Uncounted async {
                        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Op["+opDesc(op)+"] moved here["+here+"] key["+key+"] ...");
                        var result:Any = null;
                        var exp:Exception = null;
                        try {
                            if (op == ASYNC_GET) {
                                result = getLocal(key,  id, mapName);
                            }
                            else if (op == ASYNC_PUT) {
                                result = putLocal(key, value,  id, mapName);
                            }
                            else if (op == ASYNC_DELETE) {
                                result = deleteLocal(key,  id, mapName);
                            }
                            else if (op == ASYNC_KEYSET) {
                                result = keySetLocal( id, mapName);
                            }
                            else if (op == ASYNC_AT_VOID) {
                                closure_void();
                            }
                            else if (op == ASYNC_AT_RETURN) {
                                result = closure_return();
                            }
                        }catch(e:Exception) {
                            exp = e;
                        }
                        
                        val x = result;
                        val y = exp;
                        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] Op["+opDesc(op)+"] finished here["+here+"] key["+key+"] going to fill future at["+gr.home+"]...");
                        at (gr) @Immediate ("fill_tx_future") async {
                        
                        }
                    }
                } catch (m:Exception) {
                    future.fill(null, m);
                }
                return new TxOpResult(future);
            }
        }catch (ex:Exception) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"]  Failed Op["+opDesc(op)+"] here["+here+"] dest["+dest+"] key["+key+"] value["+value+"] ...");
            try {
                val startIntAbort = System.nanoTime();
                internalAbort(ex, root);
                val endIntAbort = System.nanoTime();
                if(TM_DEBUG) Console.OUT.println("Tx["+id+"] internal abort Op["+opDesc(op)+"] time ["+((endIntAbort - startIntAbort)/1e6)+"] ms");
            }catch(abortEx:Exception) {}
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] here[" + here + "] after internal Abort throwing exception ["+ex.getMessage()+"] ...");
            throw ex;
        } finally {
            val endExec = System.nanoTime();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] execute Op["+opDesc(op)+"] time: [" + ((endExec-startExec)/1e6) + "] ms");
        }
        
    }
    /*********************** Abort ************************/
    
    private def internalAbort(ex:Exception, root:GlobalRef[DummyTx]) {
        val abortedPlaces = new ArrayList[Place]();
        if (ex instanceof ConflictException) {
            abortedPlaces.add( (ex as ConflictException).place);
        }
        finish at (root) async {
            root().abort(abortedPlaces);
        }
    }
    
    @Pinned public def abort() {
        abort(new ArrayList[Place]());
    }
    
    @Pinned private def abort(abortedPlaces:ArrayList[Place]) {
        atomic {
            if (!aborted) {
                aborted = true;
            }
            else
                return;
        }
        
        try {
            //ask masters to abort (a master will abort slave first, then abort itself)
            finalize(false, abortedPlaces,  id, mapName, members, root);
        }
        catch(ex:MultipleExceptions) {
            
        }
        
        if (abortTime == -1)
            abortTime = System.nanoTime();
    }

    
    /***********************   Two Phase Commit Protocol ************************/
    @Pinned public def commit():Int {
        //Commit must be done from the same place who started the transaction
        assert(here.id == root.home.id);
        try {
            val startWhen = System.nanoTime();
            commitPhaseOne( id, mapName, members, root); // failures are fatal
            val endWhen = System.nanoTime();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne time [" + ((endWhen-startWhen)/1e6) + "] ms");
        }
        catch(ex:MultipleExceptions) {
            //ex.printStackTrace();
            val list = getDeadAndConflictingPlaces(ex);
            abort(list);
            if(TM_DEBUG) Console.OUT.println(here + "==> (A) throwing " + ex.getMessage());
            throw ex;
        }
        /*Transaction MUST Commit (if master is dead, commit at slave)*/
        
        val startWhen = System.nanoTime();
        commitPhaseTwo( id, mapName, members, root);
        val endWhen = System.nanoTime();
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseTwo time [" + ((endWhen-startWhen)/1e6) + "] ms");
            
        commitTime = System.nanoTime();
        
        if (excs.size() > 0)
            return SUCCESS_RECOVER_STORE;
        else
            return SUCCESS;
    }
    
    private def commitPhaseOne( id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[DummyTx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne ...");
        
        finish for (p in members) {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseOne going to move to ["+p+"] ...");
            at (p) async {
                
            }
        }
    }
    
    private def commitPhaseTwo( id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[DummyTx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"] commitPhaseTwo ...");
        try {
            //ask masters and slaves to commit
            finalize(true, null, id, mapName, members, root);
        }
        catch(ex:MultipleExceptions) {
             throw ex;
        }
    }
    
    //used for both commit and abort
    private def finalize(commit:Boolean, abortedPlaces:ArrayList[Place], 
             id:Long, mapName:String, members:PlaceGroup, root:GlobalRef[DummyTx]) {
        if(TM_DEBUG) Console.OUT.println("Tx["+id+"]  here["+here+"] " + ( commit? " Commit Started ": " Abort Started " ) + " ...");
        
        //if one of the masters die, let the exception be thrown to the caller, but hide dying slves
        finish for (p in members) {
            /*skip aborted places*/
            if (!commit && abortedPlaces.contains(p))
                continue;
            
            at (p) async {
               
            }
        }
    }
       
    public static def opDesc(op:Int) {
        switch(op) {
            case GET_LOCAL: return "GET_LOCAL";
            case GET_REMOTE: return "GET_REMOTE";
            case PUT_LOCAL: return "PUT_LOCAL";
            case PUT_REMOTE: return "PUT_REMOTE";
            case DELETE_LOCAL: return "DELETE_LOCAL";
            case DELETE_REMOTE: return "DELETE_REMOTE";
            case KEYSET_LOCAL: return "KEYSET_LOCAL";
            case KEYSET_REMOTE: return "KEYSET_REMOTE";
            case AT_VOID: return "AT_VOID";
            case AT_RETURN: return "AT_RETURN";
            case ASYNC_GET: return "ASYNC_GET";
            case ASYNC_PUT: return "ASYNC_PUT";
            case ASYNC_DELETE: return "ASYNC_DELETE";
            case ASYNC_KEYSET: return "ASYNC_KEYSET";
            case ASYNC_AT_VOID: return "ASYNC_AT_VOID";
            case ASYNC_AT_RETURN: return "ASYNC_AT_RETURN";
        }
        return "";
    }
    
    private static def getDeadAndConflictingPlaces(mulExp:MultipleExceptions) {
        val list = new ArrayList[Place]();
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
        return list;
    }
    
    private static def getDeadPlaces(mulExp:MultipleExceptions) {
        val list = new ArrayList[Place]();
        val deadExList = mulExp.getExceptionsOfType[DeadPlaceException]();
        if (deadExList != null) {
            for (dpe in deadExList) {
                list.add(dpe.place);
            }
        }
        return list;
    }
    
    public def getConsumedTime() {
        if (commitTime != -1)
            return commitTime - startTime;
        else if (abortTime != -1)
            return abortTime - startTime;
        else
            return -1;
    }
    
    public def getStatusAsString() {
        var str:String = "";
        if (commitTime != -1)
            str += "Id:"+id+":Status:Committed:TimeNanoSeconds:"+( commitTime - startTime );
        else if (abortTime != -1)
            str += "Id:"+id+":Status:Aborted:TimeNanoSeconds:"+( abortTime - startTime );
        else
            str += "Id:"+id+":Status:Running";
        return str;
    }
}

class TxOpResult {
    var set:Set[String];
    var value:Any;
    var future:TxFuture;
    public def this(s:Set[String]) {
        set = s;
    }
    public def this(v:Any) {
        value = v;
    }
    public def this(f:TxFuture) {
        future = f;
    }
}