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

public class CloneableLong implements Cloneable {
    public var v:Long;
        
    public def this(v:Long) {
        this.v = v;
    }
    
    public def clone():Cloneable {
        return new CloneableLong(v);
    }
    
    public def toString() {
        return "CloneableLong[" + v + "]";
    }
    
    public operator this + (other:CloneableLong) {
        return new CloneableLong (v + other.v );
    }
    
    public operator this - (other:CloneableLong) {
        return new CloneableLong (v - other.v );
    }
    
}