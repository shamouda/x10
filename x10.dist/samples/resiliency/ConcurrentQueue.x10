/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2017.
 */

import x10.util.concurrent.Lock;
import x10.util.GrowableRail;

public class ConcurrentQueue[T] {T haszero} {
    val buffer = new GrowableRail[T](10);
    val lock = new Lock();
    
    public def add(t:T) {
        try {
            lock.lock();
            buffer.add(t);
        }
        finally {
            lock.unlock();
        }
    }
    
    public def poll():T {
        try {
            lock.lock();
            if (buffer.size() == 0)
                return Zero.get[T]();
            var first:T = buffer(0);
            var last:T = buffer(buffer.size()-1);
            val tmp = first;
            first = last;
            last = tmp;
            return buffer.removeLast();
        }
        finally {
            lock.unlock();
        }
    }
    
    public static def main(arg:Rail[String]) {
        val q = new ConcurrentQueue[String]();
        q.add("A");
        q.add("B");
        q.add("C");
        q.add("D");
        
        Console.OUT.println("1-" + q.poll());
        Console.OUT.println("2-" + q.poll());
        Console.OUT.println("3-" + q.poll());
        Console.OUT.println("4-" + q.poll());
        Console.OUT.println("5-" + q.poll());
        Console.OUT.println("6-" + q.poll());
        
        q.add("E");
        q.add("F");
        q.add("G");
        q.add("H");
        
        Console.OUT.println("7-" + q.poll());
        Console.OUT.println("8-" + q.poll());
        Console.OUT.println("9-" + q.poll());
        Console.OUT.println("10-" + q.poll());
        Console.OUT.println("11-" + q.poll());
        Console.OUT.println("12-" + q.poll());
        
        
    }
}