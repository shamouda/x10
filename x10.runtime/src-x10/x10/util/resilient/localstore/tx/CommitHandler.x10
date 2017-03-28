package x10.util.resilient.localstore.tx;

import x10.util.ArrayList;

public abstract class CommitHandler {
	protected transient var phase1ElapsedTime:Long = 0;
    protected transient var phase2ElapsedTime:Long = 0;

    public abstract def abort(abortedPlaces:ArrayList[Place]):void;
    public abstract def commit(skipPhaseOne:Boolean):void;
    protected static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    protected def updateTxDesc(status:Long){
        val start = Timer.milliTime();
        val localTx = txDescMap.startLocalTransaction();
        try {
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] started ...");
            val desc = localTx.get("tx"+id) as TxDesc;
            //while recovering a transaction, the current place will not have the TxDesc stored, NPE can be thrown
            if (desc != null) {
                desc.status = status;
                localTx.put("tx"+id, desc);
            }
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " updateTxDesc localTx["+localTx.id+"] completed ...");
    
        } catch(ex:Exception) {
            if(TM_DEBUG) {
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
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] started ...");
            localTx.put("tx"+id, null);
            localTx.commit();
            if(TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " deleteTxDesc localTx["+localTx.id+"] completed ...");
        }catch(ex:Exception) {
            Console.OUT.println("Warning: ignoring exception during deleteTxDesc: " + ex.getMessage());
            ex.printStackTrace();
        }
        finally {
            txLoggingElapsedTime += Timer.milliTime() - start;
        }
    }
    
    
    protected def getDeadPlaces(mulExp:MultipleExceptions, members:PlaceGroup) {
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

}