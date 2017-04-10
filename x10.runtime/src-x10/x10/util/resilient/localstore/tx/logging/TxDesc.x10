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

public class TxDesc(id:Long, mapName:String) implements Cloneable{
    public var status:Long = STARTED;
    public var virtualMembers:Rail[Long];
    public val staticMembers:Boolean;
    public static val STARTED=1;
    public static val COMMITTING=2;
    public static val COMMITTED=3;

    public def this(id:Long, mapName:String, staticMembers:Boolean) {
        property (id, mapName);
        this.status = STARTED;
        this.virtualMembers = new Rail[Long]();
        this.staticMembers = staticMembers;
    }
    
    //used for clone only
    private def this(id:Long, mapName:String, status:Long, rail:Rail[Long], staticMembers:Boolean) {
        property (id, mapName);
        this.status = status;
        this.virtualMembers = rail;
        this.staticMembers = staticMembers;
    }
    
    public def clone():Cloneable {
        return new TxDesc(id, mapName, status, virtualMembers, staticMembers);
    }
    
    public def addVirtualMembers(ids:Rail[Long]) {
        if (ids == null)
            return;
        val list = new ArrayList[Long]();
        for (id in ids) {
            var found:Boolean = false;
            for (v in virtualMembers) {
                if (v == id) {
                    found = true;
                    break;
                }
            }
            if (!found)
                list.add(id);
        }
        val newSize = virtualMembers.size + list.size();
        val newMembers = new Rail[Long] (newSize);
        var lastIndex:Long = 0;
        for (var i:Long = 0 ; i < newSize; i++) {
            if (i < virtualMembers.size)
                newMembers(i) = virtualMembers(i);
            else 
                newMembers(i) = list.get(lastIndex++);
        }
        virtualMembers = newMembers;
    }
    
    public def getVirtualMembers() = virtualMembers;
    
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
        for ( p in virtualMembers )
            str += p + " ";
        return "TxDesc id["+id+"] mapName["+mapName+"] status["+getStatusDesc()+"] virtualMembers["+str+"]";
    }
}