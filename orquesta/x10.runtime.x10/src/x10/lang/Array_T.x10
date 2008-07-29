package x10.lang;

import x10.lang.Object;

import x10.array.BaseArray_T;

public abstract value class Array_T implements Indexable_T, Settable_T, Arithmetic_Array_T {

    //
    // properties
    // XXX change impl to support these
    //
    //public final Dist dist;
    //
    //public final Region region;
    //public final place onePlace;
    //public final boolean constant;
    //public final boolean unique;
    //
    //public final int rank;
    //public final boolean rect;
    //public final boolean zeroBased;
    //public final boolean rail;
    //

    //
    // basic information
    // XXX change to properties
    //

    public abstract Dist dist();
    public abstract Region region();
    public abstract int rank();


    //
    // factories
    //

    public static Array_T make(Dist dist, nullable<Indexable_T> init) {
        return BaseArray_T.make(dist, init);
    }

    public static Array_T make(Region region, nullable<Indexable_T> init) {
        return BaseArray_T.make(region, init);
    }

    public static Array_T make(Region region, nullable<Indexable_T> init, boolean value) {
        return BaseArray_T.make(region, init, value);
    }

    public static Array_T make(T [] r) {
        return BaseArray_T.make(r);
    }

    // XXX temp workaround b/c nullable<T> fails typechecking for libraries
    private static class NO_INIT implements Indexable_T {public T get(Point pt) {throw new Error();}}
    public static final Indexable_T NO_INIT = new NO_INIT();


    //
    // views
    //

    public abstract Array_T view(Region r);


    //
    // value access
    //

    public abstract T get(Point pt);
    public abstract T get(int i0);
    public abstract T get(int i0, int i1);
    public abstract T get(int i0, int i1, int i2);

    public abstract void set(Point pt, T v);
    public abstract void set(int i0, T v);
    public abstract void set(int i0, int i1, T v);
    public abstract void set(int i0, int i1, int i2, T v);


    //
    // raw access
    //

    public abstract T [] raw(place place);

}
