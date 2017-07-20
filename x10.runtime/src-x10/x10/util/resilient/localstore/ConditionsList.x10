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

package x10.util.resilient.localstore;

import x10.util.concurrent.Lock;
import x10.util.concurrent.Condition;
import x10.util.ArrayList;

public class ConditionsList {
    
    private transient val slaveCondList:ArrayList[Condition] = new ArrayList[Condition]();
    private transient val lock:Lock = new Lock();
    private var slave:Long;
    private static val instance = new ConditionsList();
    
    public static def get() {
        return instance;
    }
    
    public def releaseAll() {
        //if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " ConditionsList.releaseAll ...");
        try {
            lock.lock();
            if (Place(slave).isDead()) {
                //if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " ConditionsList.releaseAll slave ["+Place(slave)+"] is dead...");
                for (cond in slaveCondList){
                    //if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " ConditionsList.release cond["+cond+"] ...");
                    cond.release();
                }
                slaveCondList.clear();
            }
            /*
            else {
                if (TxConfig.get().TM_DEBUG) Console.OUT.println(here + " ConditionsList.releaseAll slave ["+Place(slave)+"] not dead...");        
            }*/
        }finally {
            lock.unlock();
        }
    }
    
    public def add(cond:Condition) {
        try {
            lock.lock();
            slaveCondList.add(cond);
        }finally {
            lock.unlock();
        }
    }
    
    public def remove(cond:Condition) {
        try {
            lock.lock();
            slaveCondList.remove(cond);
        }finally {
            lock.unlock();
        }
    }
    
    
    public def setSlave(id:Long) {
        slave = id;
    }
}