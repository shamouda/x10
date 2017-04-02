package x10.util.resilient.localstore.tx.commit;

import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.resilient.localstore.tx.*;
import x10.util.resilient.localstore.LocalStore;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.TxMembers;

public abstract class CommitHandler {
	public transient var phase1ElapsedTime:Long = 0;
    public transient var phase2ElapsedTime:Long = 0;
	public transient var txLoggingElapsedTime:Long = 0;

    public abstract def abort(abortedPlaces:ArrayList[Place]):void;
    public abstract def commit(skipPhaseOne:Boolean):Int;
    
    protected plh:PlaceLocalHandle[LocalStore];
    protected id:Long;
    protected mapName:String;
    protected members:TxMembers;
    protected txDescMap:ResilientNativeMap;
    
    public def this(plh:PlaceLocalHandle[LocalStore], id:Long, mapName:String, members:TxMembers, txDescMap:ResilientNativeMap) {
    	this.plh = plh;
    	this.id = id;
    	this.mapName = mapName;
    	this.members = members;
    	this.txDescMap = txDescMap;
    }
    
    protected def updateTxDesc(status:Long){
        val start = Timer.milliTime();
        val localTx = txDescMap.startLocalTransaction();
        try {
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] started ...");
            val desc = localTx.get("tx"+id) as TxDesc;
            //while recovering a transaction, the current place will not have the TxDesc stored, NPE can be thrown
            if (desc != null) {
                desc.status = status;
                localTx.put("tx"+id, desc);
            }
            val ignoreDeadSlave = true;
            localTx.commit(ignoreDeadSlave);
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] completed ...");
    
        } catch(ex:Exception) {
            if(TxConfig.get().TM_DEBUG) {
                Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] failed exception["+ex.getMessage()+"] ...");
                ex.printStackTrace();
            }
            throw ex;
        }
        finally {
            txLoggingElapsedTime += Timer.milliTime() - start;
        }
    }
    
    protected def deleteTxDesc(){
        val start = Timer.milliTime();
        try {
            val localTx = txDescMap.startLocalTransaction();
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] started ...");
            localTx.put("tx"+id, null);
            val ignoreDeadSlave = true;
            localTx.commit(ignoreDeadSlave);
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] completed ...");
        }catch(ex:Exception) {
            if(TxConfig.get().TM_DEBUG) {
                Console.OUT.println("Warning: ignoring exception during deleteTxDesc: " + ex.getMessage());
                ex.printStackTrace();
            }
        }
        finally {
            txLoggingElapsedTime += Timer.milliTime() - start;
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
}