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

import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.LocalStore;
import x10.util.GrowableRail;
import x10.util.resilient.localstore.TxConfig;
import x10.util.resilient.localstore.tx.TxManager;
import x10.util.ArrayList;

public class TxDesc(id:Long) {
    public var status:Long = STARTED;
    public val virtualMembers:GrowableRail[Long];
    public val staticMembers:Boolean;
    public static val STARTED=1;
    public static val COMMITTING=2;
    public static val COMMITTED=3;

    public def this(id:Long, staticMembers:Boolean) {
        property (id);
        this.status = STARTED;
        this.staticMembers = staticMembers;
        this.virtualMembers = new GrowableRail[Long](TxConfig.get().PREALLOC_MEMBERS);
    }
    
    public def addVirtualMembers(ids:Rail[Long]) {
        if (ids == null)
            return;
        for (id in ids) {
            var found:Boolean = false;
            for (var i:Long = 0; i < virtualMembers.size(); i++) {
                if (virtualMembers(i) == id) {
                    found = true;
                    break;
                }
            }
            if (!found)
                virtualMembers.add(id);
        }
    }
    
    public def getVirtualMembers() {            
        return virtualMembers;
    }
    
    public def getStatusDesc() {
        if (status == STARTED)
            return "STARTED";
        else if (status == COMMITTING)
            return "COMMITTING";
        else if (status == COMMITTED)
            return "COMMITTED";
        return "";
    }
    
    public def toString() {
        var str:String = "";
        for (var i:Long = 0; i < virtualMembers.size(); i++)
            str += virtualMembers(i) + " ";
        return "TxDesc id["+id+"] status["+getStatusDesc()+"] virtualMembers["+str+"]";
    }
}