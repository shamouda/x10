/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

package x10.util.concurrent;

import x10.compiler.NativeClass;
import x10.compiler.Pinned;
import x10.io.Unserializable;

/**
 * <p>X10 wrapper class for native semaphore with 1 permit.
 * Unlike Lock.x10, a Semaphore can be unlocked by a different thread</p>
 */
@NativeClass("java", "x10.core.concurrent", "NativeSemaphore")
@NativeClass("c++", "x10.lang", "Semaphore")
@Pinned public class Semaphore(permits:Int) implements Unserializable {
    public native def this(permits:Int);

    public native def acquire():void;

    public native def tryAcquire():Boolean;

    public native def release():void;
    
    public native def availablePermits():Int;
}
