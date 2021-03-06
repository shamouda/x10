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

package x10.rtt;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentHashMap;

import x10.core.Any;
import x10.serialization.SerializationConstants;
import x10.serialization.X10JavaDeserializer;
import x10.serialization.X10JavaSerializable;
import x10.serialization.X10JavaSerializer;

public class RuntimeType<T> implements Type<T>, X10JavaSerializable {

    public enum Variance {INVARIANT, COVARIANT, CONTRAVARIANT}
    
    public Class<?> javaClass;
    private int numParams;
    public Type<?>[] parents;

    // Just for allocation
    public RuntimeType() {
    }
    
    public RuntimeType(Class<?> javaClass) {
        this(javaClass, 0, null);
    }

    protected RuntimeType(Class<?> javaClass, int numParents) {
        this(javaClass, numParents, null);
    }

    protected RuntimeType(Class<?> javaClass, Type<?>[] parents) {
        this(javaClass, 0, parents);
    }
    
    protected RuntimeType(Class<?> javaClass, int numParams, Type<?>[] parents) {
        this.javaClass = javaClass;
        assert numParams <= Byte.MAX_VALUE;
        this.numParams = numParams;
        this.parents = parents;
    }
  
    private static final boolean useCache = true;
    private static final ConcurrentHashMap<Class<?>, RuntimeType<?>> typeCache = new ConcurrentHashMap<Class<?>, RuntimeType<?>>();
    public static <T> RuntimeType/*<T>*/ make(Class<?> javaClass) {
        if (useCache) {
            RuntimeType<?> type = typeCache.get(javaClass);
            if (type == null) {
                RuntimeType<?> type0 = Types.getRTTForKnownType(javaClass);
                if (type0 == null) {
                    type0 = new RuntimeType<T>(javaClass, 0, null);
                }
                type = typeCache.putIfAbsent(javaClass, type0);
                if (type == null) type = type0;
            }
            return (RuntimeType<T>) type;
        } else {
            RuntimeType<?> type = Types.getRTTForKnownType(javaClass);
            if (type == null) {
                type = new RuntimeType<T>(javaClass, 0, null);
            }
            return (RuntimeType<T>) type;
        }
    }

    public static <T> RuntimeType/*<T>*/ make(Class<?> javaClass, int numParams) {
        if (useCache) {
            RuntimeType<?> type = typeCache.get(javaClass);
            if (type == null) {
                RuntimeType<?> type0 = new RuntimeType<T>(javaClass, numParams, null);
                type = typeCache.putIfAbsent(javaClass, type0);
                if (type == null) type = type0;
            }
            return (RuntimeType<T>) type;
        } else {
            return new RuntimeType<T>(javaClass, numParams, null);
        }
    }

    public static <T> RuntimeType/*<T>*/ make(Class<?> javaClass, Type<?>[] parents) {
        if (useCache) {
            RuntimeType<?> type = typeCache.get(javaClass);
            if (type == null) {
                RuntimeType<?> type0 = new RuntimeType<T>(javaClass, 0, parents);
                type = typeCache.putIfAbsent(javaClass, type0);
                if (type == null) type = type0;
            }
            return (RuntimeType<T>) type;
        } else {
            return new RuntimeType<T>(javaClass, 0, parents);
        }
    }
    
    public static <T> RuntimeType/*<T>*/ make(Class<?> javaClass, int numParams, Type<?>[] parents) {
        if (useCache) {
            RuntimeType<?> type = typeCache.get(javaClass);
            if (type == null) {
                RuntimeType<?> type0 = new RuntimeType<T>(javaClass, numParams, parents);
                type = typeCache.putIfAbsent(javaClass, type0);
                if (type == null) type = type0;
            }
            return (RuntimeType<T>) type;
        } else {
            return new RuntimeType<T>(javaClass, numParams, parents);
        }
    }

    @Override
    public Class<?> getJavaClass() {
        return javaClass;
    }
    
    // Note: function types override this
    protected Variance getVariance(int i) {
            return Variance.INVARIANT;
    }
    
    protected final int numParams() {
        return numParams;
    }
    
    public Type<?>[] getParents() {
        return parents;
    }
    
    @Override
    public String toString() {
        return typeName();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof RuntimeType<?>) {
            RuntimeType<?> rt = (RuntimeType<?>) o;
            if (!javaClass.equals(rt.javaClass)) {
                return false;
            }
            // N.B. for given javaClass, we assume variances and parents are unique.
            // Therefore we don't need to compare them.
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return javaClass.hashCode();
    }

    @Override
    public boolean isAssignableTo(Type<?> superType) {
        if (this == superType) return true;
        if (superType == Types.ANY) return true;
        if (superType instanceof RuntimeType<?>) {
            RuntimeType<?> rt = (RuntimeType<?>) superType;
            if (rt.javaClass.isAssignableFrom(javaClass)) {
                return true;
            }
        }
        if (superType instanceof ParameterizedType) {
            ParameterizedType<?> pt = (ParameterizedType<?>) superType;
            if (pt.getRawType().isAssignableFrom(pt.getActualTypeArguments(), this, null)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasZero() {
        return true;
    }

    @Override
    public boolean isref() {
        return true;
    }

    @Override
    public boolean isInstance(Object o) {
        if (o == null) {return false;}
        if (o.getClass() == javaClass) {
            return true;
        }
        return javaClass.isInstance(o);
    }

    private static final boolean subtypeTestForParam(Variance variance, Type<?> superParam, Type<?> subParam) {
        switch (variance) {
        case INVARIANT:
            return superParam.equals(subParam);
        case COVARIANT:
            return subParam.isAssignableTo(superParam);
        case CONTRAVARIANT:
            return superParam.isAssignableTo(subParam);
        }
//        assert false; // should never happen
        return true;
    }
    // o instanceof this and thisParams
    public final boolean isInstance(Object o, Type<?>... thisParams) {
        if (o == null) {return false;}
        Class<?> target = o.getClass();
        if (target == javaClass || checkAnonymous(target)) {
            for (int i = 0, s = thisParams.length; i < s; i++) {
                Variance variance;
                Type<?> subParam;
                Type<?> thisParam;
                variance = getVariance(i);
                subParam = Types.getParam(o, i);
                thisParam = thisParams[i];
                if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            }
            return true;
        }
        else if (javaClass.isInstance(o)) { // i.e. type of o != This
            return checkParents(o, thisParams);
        }
        // not needed for Java primitives. not sure for String
        /*
        else if (o instanceof String || o instanceof Number) {
            // @NativeRep'ed type
            return checkParents(o, thisParams);
        }
        */
        else {
            return false;
        }
    }

    private boolean checkAnonymous(Class<?> target) {
        if (!target.isAnonymousClass()) {
            return false;
        }
        if (target.getSuperclass() != Object.class && target.getSuperclass() == javaClass) {
            return true;
        }
        if (target.getInterfaces().length == 1 && target.getInterfaces()[0] == javaClass) {
            return true;
        }
        return false;
    }

    private final boolean checkParents(Object o, Type<?>... params) {
        if (o instanceof Any) {
            Any any = (Any) o;
            RuntimeType<?> rtt = any.$getRTT(); // o.$RTT
            if (rtt == null) {
                return true;
            }
            return instantiateCheck(params, rtt, any);
        }
        if (o instanceof String) {
            RuntimeType<?> rtt = Types.STRING;
            return instantiateCheck(params, rtt, o);
        }
        return false;
    }

    private static Type<?> resolveUnresolvedType(RuntimeType<?> rtt, Object o_or_any_or_subParams, Type<?> origTypeArgument) {
        Type<?> t = origTypeArgument;
        while (t instanceof UnresolvedType) {
            int index = ((UnresolvedType) t).getIndex();
            if (o_or_any_or_subParams instanceof Any) {
                Any any = (Any) o_or_any_or_subParams;
                t = index == -1 ? rtt : any.$getParam(index);
            } else if (o_or_any_or_subParams instanceof Type<?>[]) {
                Type<?>[] paramsRTT = (Type<?>[]) o_or_any_or_subParams;
                t = index == -1 ? rtt : paramsRTT[index];
            } else {
                assert(index == -1);
                t = rtt;                
            }
        }
        if (t instanceof ParameterizedType<?>) {
            ParameterizedType<?> pt = (ParameterizedType<?>) t;
            Type<?>[] origTypeArgumentsT = pt.getActualTypeArguments();
            Type<?>[] resolvedTypeArgumentsT = new Type<?>[origTypeArgumentsT.length];
            for (int i = 0; i < origTypeArgumentsT.length; i++) {
                resolvedTypeArgumentsT[i] = resolveUnresolvedType(rtt, o_or_any_or_subParams, origTypeArgumentsT[i]);
            }
            if (origTypeArgumentsT.length == 1) {
                t = ParameterizedType.make(pt.getRawType(), resolvedTypeArgumentsT[0]);
            } else if (origTypeArgumentsT.length == 2) {
                t = ParameterizedType.make(pt.getRawType(), resolvedTypeArgumentsT[0], resolvedTypeArgumentsT[1]);                
            } else {
                t = ParameterizedType.make(pt.getRawType(), resolvedTypeArgumentsT);
            }
        }
        return t;
    }

    // e.g. C[T1,T2]:Super[Int, T1] -> C[Int,Double]:Super[Int,Int] 
    private final boolean instantiateCheck(Type<?>[] params, RuntimeType<?> rtt, Object o_or_any_or_subParams) {
        if (rtt.parents != null) {
            for (Type<?> t : rtt.parents) {
                if (javaClass.isAssignableFrom(t.getJavaClass())) {
                    if (t instanceof ParameterizedType<?>) {
                        ParameterizedType<?> pt = (ParameterizedType<?>) t;
                        Type<?>[] origTypeArgumentsT = pt.getActualTypeArguments();
                        Type<?>[] resolvedTypeArgumentsT = new Type<?>[origTypeArgumentsT.length];
                        for (int i = 0; i < origTypeArgumentsT.length; i++) {
                            resolvedTypeArgumentsT[i] = resolveUnresolvedType(rtt, o_or_any_or_subParams, origTypeArgumentsT[i]);
                        }
                        if (isAssignableFrom(params, pt.getRawType(), resolvedTypeArgumentsT)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    // check "subType and subParams" <: "this and thisParams"
    final boolean isAssignableFrom(Type<?>[] thisParams, RuntimeType<?> subType, Type<?>[] subParams) {
        if (javaClass == subType.getJavaClass()) {
            if (thisParams != null) {
                for (int i = 0, s = thisParams.length; i < s; i ++) {
                    Variance variance;
                    Type<?> subParam;
                    Type<?> thisParam;
                    variance = getVariance(i);
                    subParam = subParams[i];
                    thisParam = thisParams[i];
                    if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
                }
            }
            return true;
        }
        else if (javaClass.isAssignableFrom(subType.getJavaClass())) {
            return instantiateCheck(thisParams, subType, subParams);
        }
        else {
            return false;
        }
    }
    
    @Override
    public Object makeArray(int dim0) {
        return Array.newInstance(javaClass, dim0);
    }

    @Override
    public Object makeArray(int dim0, int dim1) {
        return Array.newInstance(javaClass, new int[] { dim0, dim1 });
    }
    
    @Override
    public Object makeArray(int dim0, int dim1, int dim2) {
        return Array.newInstance(javaClass, new int[] { dim0, dim1, dim2 });
    }
    
    @Override
    public Object makeArray(int dim0, int dim1, int dim2, int dim3) {
        return Array.newInstance(javaClass, new int[] { dim0, dim1, dim2, dim3 });
    }
    
    @Override
    public Object makeArray(int... dims) {
        return Array.newInstance(javaClass, dims);
    }
    
    @Override
    public T getArray(Object array, int i) {
        // avoid native method
        //return (T) Array.get(array, i);
        return ((T[])array)[i];
    }

    @Override
    public void setArray(Object array, int i, T v) {
        // avoid native method
        //Array.set(array, i, v);
        ((T[])array)[i] = v;
    }
    
    @Override
    public int arrayLength(Object array) {
        // avoid native method
        //return Array.getLength(array);
        return ((T[])array).length;
    }

    private static final String X10_INTEROP_JAVA_ARRAY = "x10.interop.Java.array";
    private static String typeName(Class<?> javaClass) {
        RuntimeType<?> rtt = null;
        if (javaClass.isArray()) {
            return X10_INTEROP_JAVA_ARRAY + "[" + typeName(javaClass.getComponentType()) + "]";
        } else if ((rtt = Types.getRTTForKnownType(javaClass)) != null) {
            return rtt.typeName();
        } else {
            return javaClass.getName();            
        }
    }
    @Override
    public String typeName() {
        return typeName(javaClass);
    }

    protected final String typeNameForFun(Object o) {
        int numParams = numParams();
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        int i;
        for (i = 0; i < numParams - 1; i++) {
            if (i != 0) sb.append(",");
            sb.append(((Any) o).$getParam(i).typeName());
        }
        sb.append(")=>");
        sb.append(((Any) o).$getParam(i).typeName());
        return sb.toString();
    }
    protected final String typeNameForVoidFun(Object o) {
        int numParams = numParams();
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        if (numParams > 0) {
            for (int i = 0; i < numParams; i++) {
                if (i != 0) sb.append(",");
                sb.append(((Any) o).$getParam(i).typeName());
            }
        }
        sb.append(")=>void");
        return sb.toString();
    }
    protected final String typeNameForOthers(Object o) {
        int numParams = numParams();
        StringBuilder sb = new StringBuilder();
        sb.append(typeName());
        if (numParams > 0) {
            if (o instanceof Any) {
                sb.append("[");
                for (int i = 0; i < numParams; i++) {
                    if (i != 0) sb.append(",");
                    sb.append(Types.getParam(o, i).typeName());
                }
                sb.append("]");
            }
        }
        return sb.toString();
    }
    // should be overridden by RTT of all function types
    public String typeName(Object o) {
        return typeNameForOthers(o);
    }
    
    // for shortcut
    public boolean isInstance(Object o, Type<?> thisParam0) {
        if (o == null) {return false;}
        Class<?> target = o.getClass();
        if (target == javaClass || checkAnonymous(target)) {
            Variance variance;
            Type<?> subParam;
            Type<?> thisParam;
            variance = getVariance(0);
            subParam = Types.getParam(o, 0);
            thisParam = thisParam0;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            return true;
        }
        else if (javaClass.isInstance(o)) {
            return checkParents(o, thisParam0);
        }
        // not needed for Java primitives. not sure for String
        /*
        else if (o instanceof String || o instanceof Number) {
            // @NativeRep'ed type
            return checkParents(o, thisParam0);
        }
        */
        else {
            return false;
        }
    }

    // for shortcut
    public final boolean isInstance(Object o, Type<?> thisParam0, Type<?> thisParam1) {
        if (o == null) {return false;}
        Class<?> target = o.getClass();
        if (target == javaClass || checkAnonymous(target)) {
            Variance variance;
            Type<?> subParam;
            Type<?> thisParam;
            variance = getVariance(0);
            subParam = Types.getParam(o, 0);
            thisParam = thisParam0;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            variance = getVariance(1);
            subParam = Types.getParam(o, 1);
            thisParam = thisParam1;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            return true;
        }
        else if (javaClass.isInstance(o)) {
            return checkParents(o, thisParam0, thisParam1);
        }
        // not needed for Java primitives. not sure for String
        /*
        else if (o instanceof String || o instanceof Number) {
            // @NativeRep'ed type
            return checkParents(o, thisParam0, thisParam1);
        }
        */
        else {
            return false;
        }
        
    }

    // for shortcut
    public final boolean isInstance(Object o, Type<?> thisParam0, Type<?> thisParam1, Type<?> thisParam2) {
        if (o == null) {return false;}
        Class<?> target = o.getClass();
        if (target == javaClass || checkAnonymous(target)) {
            Variance variance;
            Type<?> subParam;
            Type<?> thisParam;
            variance = getVariance(0);
            subParam = Types.getParam(o, 0);
            thisParam = thisParam0;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            variance = getVariance(1);
            subParam = Types.getParam(o, 1);
            thisParam = thisParam1;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            variance = getVariance(2);
            subParam = Types.getParam(o, 2);
            thisParam = thisParam2;
            if (!subtypeTestForParam(variance, thisParam, subParam)) {return false;}
            return true;
        }
        else if (javaClass.isInstance(o)) {
            return checkParents(o, thisParam0, thisParam1, thisParam2);
        }
        // not needed for Java primitives. not sure for String
        /*
        else if (o instanceof String || o instanceof Number) {
            // @NativeRep'ed type
            return checkParents(o, thisParam0, thisParam1, thisParam2);
        }
        */
        else {
            return false;
        }
    }

    public short $_get_serialization_id() {
        return SerializationConstants.NO_PREASSIGNED_ID;
    }
    
    @Override
    public void $_serialize(X10JavaSerializer serializer) throws IOException {
        short sid = serializer.getSerializationId(javaClass, null);
        serializer.writeSerializationId(sid);
        serializer.write((byte) numParams);
        // TODO parents needed?
    }

    public static X10JavaSerializable $_deserializer(X10JavaDeserializer deserializer) throws IOException {
        RuntimeType rt = new RuntimeType();
        int i = deserializer.record_reference(rt);
        X10JavaSerializable x10JavaSerializable = $_deserialize_body(rt, deserializer);
        if (rt != x10JavaSerializable) {
            deserializer.update_reference(i, x10JavaSerializable);
        }
        return x10JavaSerializable;
    }

    public static X10JavaSerializable $_deserialize_body(RuntimeType rt, X10JavaDeserializer deserializer) throws IOException {
        short classId = deserializer.readSerializationId();
        Class<?> clazz = deserializer.getClassForID(classId);
        rt.javaClass = clazz;
        rt.numParams = deserializer.readByte();
        // TODO parents needed?
        return rt;
    }
}
