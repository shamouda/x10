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

package x10.util.resilient.localstore.tx;

import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.LocalStore;

public class TxDesc(id:Long, mapName:String, virtualMembers:Rail[Long]) implements Cloneable{
    public var status:Long = STARTED;
    
    public static val STARTED=1;
    public static val COMMITTING=2;
    public static val COMMITTED=3;

    public def this(id:Long, mapName:String, virtualMembers:Rail[Long], status:Long) {
        property (id, mapName, virtualMembers);
        this.status = status;
    }
    
    public def clone():Cloneable {
        return new TxDesc(id, mapName, virtualMembers, status);
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
        for ( p in virtualMembers )
            str += p + " ";
        return "TxDesc id["+id+"] mapName["+mapName+"] status["+getStatusDesc()+"] virtualMembers["+str+"]";
    }
}