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
import x10.util.resilient.localstore.tx.StorePausedException;
import x10.util.resilient.localstore.LocalStore;
import x10.compiler.Uncounted;

public class TxDescManager[K] {K haszero} {
	public val plh:PlaceLocalHandle[LocalStore[K]];
    public static val FROM_SLAVE = false;
    public static val FROM_MASTER = true;
    protected static val resilient = x10.xrx.Runtime.RESILIENT_MODE > 0;
    
    public def this (plh:PlaceLocalHandle[LocalStore[K]]) {K haszero} {
    	this.plh =  plh;
    }
    
    /*Static members only*/
    public def add(id:Long, members:Rail[Long], ignoreDeadSlave:Boolean) {
        val staticMembers = true;
        val desc = new TxDesc(id, staticMembers);
        desc.addVirtualMembers(members);
        if (resilient && !TxConfig.get().DISABLE_SLAVE) {
            try {
                at (plh().slave) {
                    plh().slaveStore.putTransDescriptor(id, desc);
                }
            } catch(exSl:Exception) {
                plh().asyncSlaveRecovery();
            	if (!ignoreDeadSlave)
            		throw exSl;
            }
        }
        plh().getMasterStore().getState().putTxDesc(id, desc);
        
    }
    
    /**
     * We use an uncounted async to delete the slave's TxDesc
     * that won't harm while recovery. The side effect is that we
     * may need to recommit/reabort an already committed/aborted transaction.
     * The master already discards such duplication.
     * */
    public def delete(id:Long, ignoreDeadSlave:Boolean) {
        if (resilient && !TxConfig.get().DISABLE_SLAVE) {
            try {
                at (plh().slave) @Uncounted async {
                    plh().slaveStore.deleteTransDescriptor(id);
                }
            } catch(exSl:Exception) {
                plh().asyncSlaveRecovery();
            	if (!ignoreDeadSlave)
            		throw exSl;
            }
        }
    	plh().getMasterStore().getState().removeTxDesc(id);
    }
    
    public def updateStatus(id:Long, newStatus:Long, ignoreDeadSlave:Boolean) {
        if (resilient && !TxConfig.get().DISABLE_SLAVE) {
            try {
                at (plh().slave) {
                    plh().slaveStore.updateTxDescStatus(id, newStatus);
                }
            } catch(exSl:Exception) {
                plh().asyncSlaveRecovery();
            	if (!ignoreDeadSlave)
            		throw exSl;
            }
        }
    	plh().getMasterStore().getState().updateTxDescStatus(id, newStatus);
    }
    
    public def addVirtualMembers(id:Long, vMembers:Rail[Long], ignoreDeadSlave:Boolean) {
        if (resilient && !TxConfig.get().DISABLE_SLAVE) {
            try {
                at (plh().slave) {
                    plh().slaveStore.addTxDescMembers(id, vMembers);
                }
            } catch(exSl:Exception) {
                plh().asyncSlaveRecovery();
            	if (!ignoreDeadSlave)
            		throw exSl;
            }
        }
    	plh().getMasterStore().getState().addTxDescMembers(id, vMembers);
    }
    
    public def addVirtualMember(id:Long, memId:Long, ignoreDeadSlave:Boolean) {
        if (resilient && !TxConfig.get().DISABLE_SLAVE) {
            try {
                at (plh().slave) {
                    plh().slaveStore.addTxDescMember(id, memId);
                }
            } catch(exSl:Exception) {
                plh().asyncSlaveRecovery();
            	if (!ignoreDeadSlave)
            		throw exSl;
            }
        }
    	plh().getMasterStore().getState().addTxDescMember(id, memId);
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
        val desc = plh().getMasterStore().getState().getTxDesc(id);
        if (desc == null)
        	return null;
        return desc.getVirtualMembers();
    }
    
    private def getVirtualMembersFromSlaveStore(id:Long) {
        val desc = plh().slaveStore.getTransDescriptor(id);
        if (desc == null) 
            return null;
        else
            return desc.getVirtualMembers();
    }
    
    public def deleteTxDescFromSlaveStore(id:Long) {
        plh().slaveStore.deleteTransDescriptor(id);
    }
}