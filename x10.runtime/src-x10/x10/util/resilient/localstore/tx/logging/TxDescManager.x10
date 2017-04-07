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

package x10.util.resilient.localstore.tx.logging;

import x10.util.resilient.localstore.tx.TxManager;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.TxConfig;

public class TxDescManager(map:ResilientNativeMap) {
    
    //false
    public def add(id:Long, mapName:String, members:Rail[Long], ignoreDeadSlave:Boolean) {
        val desc = new TxDesc(id, mapName);
        desc.addVirtualMembers(members);
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.add localTx["+localTx.id+"] started ...");
        localTx.put("tx"+id, desc);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.add localTx["+localTx.id+"] completed ...");
    }
    
    //true
    public def delete(id:Long, ignoreDeadSlave:Boolean) {
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.delete localTx["+localTx.id+"] started ...");
        localTx.delete("tx"+id);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.delete localTx["+localTx.id+"] completed ...");
    }
    
    public def updateStatus(id:Long, newStatus:Long, ignoreDeadSlave:Boolean) {
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.updateStatus localTx["+localTx.id+"] started ...");
        val desc = localTx.get("tx"+id) as TxDesc;
        assert (desc != null) : "TxDesc bug detected in updateStatus NULL desc";
        desc.status = newStatus;
        localTx.put("tx"+id, desc);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.updateStatus localTx["+localTx.id+"] completed ...");
    }
    
    public def addVirtualMembers(id:Long, vMembers:Rail[Long], ignoreDeadSlave:Boolean) {
        var s:String = "";
        for (r in vMembers)
            s += r + " ";
            
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.addVirtualMembers{"+s+"} localTx["+localTx.id+"] started ...");
        val desc = localTx.get("tx"+id) as TxDesc;
        assert (desc != null) : "TxDesc bug detected in addVirtualMembers NULL desc";
        desc.addVirtualMembers(vMembers);
        localTx.put("tx"+id, desc);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.addVirtualMembers{"+s+"} localTx["+localTx.id+"] completed ...");
    
    }
    
    public def getVirtualMembers(id:Long) {
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] TxDesc.getVirtualMembers localTx["+localTx.id+"] started ...");
        val desc = localTx.get("tx"+id) as TxDesc;
        localTx.commit(true);
        if (desc == null) {
            if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] TxDesc.getVirtualMembers localTx["+localTx.id+"] completed resulet [NULL]  ...");
            return null;
        }
        val result = desc.getVirtualMembers();
        var s:String = "";
        for (r in result)
            s += r + " ";
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " here["+here+"] TxDesc.getVirtualMembers localTx["+localTx.id+"] completed resulet ["+s+"]  ...");
        return result;
    }
    
    
}