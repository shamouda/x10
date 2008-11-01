/*
 *
 * (C) Copyright IBM Corporation 2006-2008.
 *
 *  This file is part of X10 Language.
 *
 */

package x10.lang;

import x10.compiler.Native;
import x10.compiler.NativeRep;

@NativeRep("java", "java.lang.IllegalArgumentException")
public value IllegalArgumentException extends RuntimeException {
    public native def this(): IllegalArgumentException;
    public native def this(message: String): IllegalArgumentException;
    public native def this(message: String, cause: Throwable): IllegalArgumentException;
    public native def this(cause: Throwable): IllegalArgumentException;
}
