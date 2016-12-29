import x10.util.resilient.localstore.CloneableLong;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.tx.ConflictException;

public class STMResilientAppUtils {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    
    public static def processException(txId:Long, ex:Exception, trial:Long) {
        var newTrial:Long = 0;
        if (ex instanceof  ConflictException) {
            newTrial = trial +1 ;
            if (TM_DEBUG) {
                Console.OUT.println("Tx["+txId+"] ApplicationException["+ex.getMessage()+"] CE retry ...");
                ex.printStackTrace();
            }
        }
        else if (ex instanceof MultipleExceptions) {
            val deadExList = (ex as MultipleExceptions).getExceptionsOfType[DeadPlaceException]();
            if (deadExList != null) {
                if (TM_DEBUG) {
                    Console.OUT.println("Tx["+txId+"] ApplicationException["+ex.getMessage()+"] ME throw recoverException ");
                    ex.printStackTrace();
                }
                throw new RecoverDataStoreException("MultipleException->RecoverDataStoreException", here);
            }//else it is a conflict exception
            
            if (TM_DEBUG) {
                Console.OUT.println("Tx["+txId+"] ApplicationException["+ex.getMessage()+"] ME retry ");
                ex.printStackTrace();
            }
            
            newTrial = trial +1 ;
        } else {
            if (TM_DEBUG) {
                Console.OUT.println("Tx["+txId+"] ApplicationException["+ex.getMessage()+"] EX throw recoverException");
                ex.printStackTrace();
            }
            throw new RecoverDataStoreException("Exception->RecoverDataStoreException", here); 
        }
        return newTrial;
    }
    
    public static def restoreProgress(map:ResilientNativeMap, placeIndex:Long, defaultProg:Long){
        val tx = map.startLocalTransaction();
        try {
            val cl = tx.get("p"+placeIndex);
            tx.commitIgnoreDeadSlave();
            var res:Long = defaultProg;
            if (cl != null)
                res = (cl as CloneableLong).v;
            
            if (TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress here["+here+"] placeIndex["+placeIndex+"] progress["+res+"]");
            return res;
        }
        catch(ex:Exception) {
            if (TM_DEBUG) Console.OUT.println("LocalTx["+tx.id+"] restoring progress failed, exception ["+ex.getMessage()+"]");
            throw ex;
        }
    }
    
    public static def createGroup(p1:Place) {
        val rail = new Rail[Place](1);
        rail(0) = p1;
        return new SparsePlaceGroup(rail);
    }
    
    public static def createGroup(p1:Place, p2:Place) {
        if (p1.id == p2.id)
            return createGroup(p1);
        val rail = new Rail[Place](2);
        rail(0) = p1;
        rail(1) = p2;
        return new SparsePlaceGroup(rail);
    }
    
    public static def createGroup(p1:Place, p2:Place, p3:Place) {
        if (p1.id == p2.id && p2.id == p3.id)
            return createGroup(p1);
        
        if (p1.id == p2.id)
            return createGroup(p1, p3);
        else if (p2.id == p3.id || p1.id == p3.id)
            return createGroup(p1, p2);
        val rail = new Rail[Place](3);
        rail(0) = p1;
        rail(1) = p2;
        rail(2) = p3;
        return new SparsePlaceGroup(rail);
    }
}

class RecoverDataStoreException(place:Place) extends Exception{
    public def this(message:String, place:Place) {
        super(message);
        property(place);
    }
}