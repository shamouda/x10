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

import x10.util.concurrent.Lock;
import x10.util.resilient.localstore.Cloneable;

public class MemoryUnit {
    private static val TM_DEBUG = System.getenv("TM_DEBUG") != null && System.getenv("TM_DEBUG").equals("1");
    private var version:Int;
    private var value:Cloneable;
    
    public def this(v:Cloneable) {
        value = v;
    }
    
    public def this(v:Cloneable, ver:Int) {
        value = v;
        version = ver;
    }
    
    //the memory unit must be locked before calling this
    public def getValue(copy:Boolean, key:String, txId:Long) {
        var v:Cloneable = value;
        if (copy)
            v = value == null ? null:value.clone();
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] getvv key["+key+"] ver["+version+"] val["+v+"]");
        return new MemoryUnit(v, version);
    }
    
    //the memory unit must be locked before calling this
    public def setValue(v:Cloneable, key:String, txId:Long) {
        val oldValue = value;
        version++;
        value = v;
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] setvv key["+key+"] ver["+version+"] val["+value+"]");
        return oldValue;
    }
    
    public def rollbackValue(oldValue:Cloneable, oldVersion:Int, key:String, txId:Long) {
        version = oldVersion; 
        value = oldValue;
        if (TM_DEBUG) Console.OUT.println("Tx["+txId+"] rollsetvv key["+key+"] ver["+version+"] val["+value+"]");
    }
    
    public def toString() {
        return "version:"+version+":value:"+value;
    }
}