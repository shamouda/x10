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
    public static val FROM_SLAVE = false;
    public static val FROM_MASTER = true;
    
    public def add(id:Long, members:Rail[Long], ignoreDeadSlave:Boolean) {
        val staticMembers = members != null && members.size > 0;
        val desc = new TxDesc(id, map.name, staticMembers);
        desc.addVirtualMembers(members);
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.add localTx["+localTx.id+"] started ...");
        localTx.put("tx"+id, desc);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.add localTx["+localTx.id+"] completed ...");
    }
    
    public def delete(id:Long, ignoreDeadSlave:Boolean) {
        val localTx = map.startLocalTransaction();
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.delete localTx["+localTx.id+"] started ...");
        localTx.deleteTxDesc("tx"+id);
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
        var desc:TxDesc = localTx.get("tx"+id) as TxDesc;
        if (desc == null)
            desc = new TxDesc(id, map.name, false); 
        desc.addVirtualMembers(vMembers);
        localTx.put("tx"+id, desc);
        localTx.commit(ignoreDeadSlave);
        if(TxConfig.get().TM_DEBUG) Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString(id) + " TxDesc.addVirtualMembers{"+s+"} localTx["+localTx.id+"] completed ...");
    
    }
    
    public def getVirtualMembers(id:Long, masterType:Boolean) {
        try {
            if (masterType)
                return getVirtualMembersFromMasterStore(id);
            else
                return getVirtualMembersFromSlaveStore(id);
        } catch (spe:StorePausedException) {
            return null;
        }
    }
    
    private def getVirtualMembersFromMasterStore(id:Long) {
        val localTx = map.startLocalTransaction();
        val desc = localTx.get("tx"+id) as TxDesc;
        localTx.commit(true);
        if (desc == null) {
            return null;
        }
        return desc.getVirtualMembers();
    }
    
    private def getVirtualMembersFromSlaveStore(id:Long) {
        val desc = map.plh().slaveStore.getTransDescriptor(id);
        if (desc == null) {
            return null;
        }
        else
            return desc.getVirtualMembers();
    }
    
    
    public def deleteTxDescFromSlaveStore(id:Long) {
        map.plh().slaveStore.deleteTransDescriptor(id);
    }
    
    
}