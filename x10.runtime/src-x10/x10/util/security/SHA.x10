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

package x10.util.security;

import x10.compiler.NativeClass;
import x10.compiler.Pinned;
import x10.io.Unserializable;

@NativeClass("java", "x10.core.security", "NativeSHA")
@NativeClass("c++", "x10.lang", "SHA")
@Pinned public class SHA implements Unserializable {
    
    public native def this();
    
    public native def digest(hash:Rail[Byte], offset:Int, len:Int):void;
    
    public native def update(hash:Rail[Byte], offset:Int, len:Int):void;
}
