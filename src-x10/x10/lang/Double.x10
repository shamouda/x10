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

@NativeRep("java", "double", "x10.core.BoxedDouble", "x10.types.Types.DOUBLE")
@NativeRep("c++", "x10_double", "x10_double", null)
public final value Double {
    // Binary and unary operations and conversions are built-in.  No need to declare them here.
    
    @Native("java", "java.lang.Double.POSITIVE_INFINITY")
    @Native("c++", "x10aux::double_utils::fromLongBits(0x7ff0000000000000LL)")
    public const POSITIVE_INFINITY: Double = Double.fromLongBits(0x7ff0000000000000L);

    @Native("java", "java.lang.Double.NEGATIVE_INFINITY")
    @Native("c++", "x10aux::double_utils::fromLongBits(0xfff0000000000000LL)")
    public const NEGATIVE_INFINITY: Double = Double.fromLongBits(0xfff0000000000000L);

    @Native("java", "java.lang.Double.NaN")
    @Native("c++", "x10aux::double_utils::fromLongBits(0x7ff8000000000000LL)")
    public const NaN: Double = Double.fromLongBits(0x7ff8000000000000L);

    @Native("java", "java.lang.Double.MAX_VALUE")
    @Native("c++", "x10aux::double_utils::fromLongBits(0x7fefffffffffffffLL)")
    public const MAX_VALUE: Double = Double.fromLongBits(0x7fefffffffffffffL);

    @Native("java", "java.lang.Double.MIN_VALUE")
    @Native("c++", "x10aux::double_utils::fromLongBits(0x1LL)")
    public const MIN_VALUE: Double = Double.fromLongBits(0x1L);
    
    @Native("java", "java.lang.Double.toHexString(#0)")
    @Native("c++", "x10aux::double_utils::toHexString(#0)")
    public native def toHexString(): String;    
    
    @Native("java", "java.lang.Double.toString(#0)")
    @Native("c++", "x10aux::double_utils::toString(#0)")
    public native def toString(): String;
    
    @Native("java", "java.lang.Double.parseDouble(#1)")
    @Native("c++", "x10aux::double_utils::parseDouble(#1)")
    public native static def parseDouble(String): Double throws NumberFormatException;
    
    @Native("java", "java.lang.Double.isNaN(#0)")
    @Native("c++", "x10aux::double_utils::isNaN(#0)")
    public native def isNaN(): boolean;

    @Native("java", "java.lang.Double.isInfinite(#0)")
    @Native("c++", "x10aux::double_utils::isInfinite(#0)")
    public native def isInfinite(): boolean;
    
    @Native("java", "java.lang.Double.doubleToLongBits(#0)")
    @Native("c++", "x10aux::double_utils::doubleToLongBits(#0)")
    public native def toLongBits(): Long;

    @Native("java", "java.lang.Double.doubleToRawLongBits(#0)")
    @Native("c++", "x10aux::double_utils::doubleToRawLongBits(#0)")
    public native def toRawLongBits(): Long;

    @Native("java", "java.lang.Double.longBitsToDouble(#1)")
    @Native("c++", "x10aux::double_utils::longBitsToDouble(#1)")
    public static native def fromLongBits(Long): Double;
}
