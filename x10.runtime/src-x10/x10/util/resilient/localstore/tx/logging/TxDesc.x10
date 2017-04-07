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

public class TxDesc(id:Long, mapName:String) implements Cloneable{
    public var status:Long = STARTED;
    public val virtualMembers:GrowableRail[Long];

    public static val STARTED=1;
    public static val COMMITTING=2;
    public static val COMMITTED=3;

    public def this(id:Long, mapName:String) {
        property (id, mapName);
        this.status = STARTED;
        this.virtualMembers = new GrowableRail[Long]();
    }
    
    //used for clone only
    private def this(id:Long, mapName:String, status:Long, rail:GrowableRail[Long]) {
        property (id, mapName);
        this.status = status;
        this.virtualMembers = new GrowableRail[Long](rail.size());
        this.virtualMembers.addAll(rail);
    }
    
    public def clone():Cloneable {
        return new TxDesc(id, mapName, status, virtualMembers);
    }
    
    public def addVirtualMembers(ids:Rail[Long]) {
        if (ids == null)
            return;
        virtualMembers.addAll(ids);
        if (TxConfig.get().TM_DEBUG) {
            var str:String = "";
            for (x in ids)
                str += x + " ";
            Console.OUT.println("Tx["+id+"] " + TxManager.txIdToString (id) + " adding members ["+str+"]");
        }
    }
    
    public def getVirtualMembers() {
        return virtualMembers.toRail();
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
        for ( p in virtualMembers.toRail() )
            str += p + " ";
        return "TxDesc id["+id+"] mapName["+mapName+"] status["+getStatusDesc()+"] virtualMembers["+str+"]";
    }
}