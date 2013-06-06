/*
 *  This file is part of the X10 project (http://x10-lang.org).
 * 
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 * 
 *  (C) Copyright IBM Corporation 2006-2013.
 */

package x10.array;

import x10.compiler.Inline;
import x10.compiler.CompilerFlags;
import x10.io.CustomSerialization;
import x10.io.SerialData;

/**
 * Implementation of a 1-D DistArray that distributes its data elements
 * over the places in its PlaceGroup in a 1-D blocked fashion.
 */
public class DistArray_Block[T] extends DistArray[T]{this.rank()==1} implements (Long)=>T {
    
    public property rank() = 1;

    protected val minLocalIndex:Long;
 
    protected val maxLocalIndex:Long;

    protected val globalIndices:DenseIterationSpace_1{self!=null};

    protected val localIndices:DenseIterationSpace_1{self!=null};
    
    /**
     * Construct a n-element block distributed DistArray
     * whose data is distrbuted over pg and initialized using
     * the init function.
     *
     * @param n number of elements 
     * @param pg the PlaceGroup to use to distibute the elements.
     * @param init the element initialization function
     */
    public def this(n:long, pg:PlaceGroup{self!=null}, init:(long)=>T) {
        super(pg, () => BLocalState.make[T](pg, n, init));
        val bls = localHandle() as BLocalState[T];
        globalIndices = bls.globalIndices;
        localIndices = bls.localIndices;
        minLocalIndex = localIndices.min(0);
        maxLocalIndex = localIndices.max(0);
    }


    /**
     * Construct a n-element block distributed DistArray
     * whose data is distrbuted over PlaceGroup.WORLD and 
     * initialized using the provided init closure.
     *
     * @param n number of elements
     * @param init the element initialization function
     */
    public def this(n:long, init:(long)=>T) {
        this(n, PlaceGroup.WORLD, init);
    }


    /**
     * Construct a n-elmenent block distributed DistArray
     * whose data is distrbuted over pg and zero-initialized.
     *
     * @param n number of elements 
     * @param pg the PlaceGroup to use to distibute the elements.
     */
    public def this(n:long, pg:PlaceGroup{self!=null}){T haszero} {
        this(n, pg, (long)=>Zero.get[T]());
    }


    /**
     * Construct a n-element block distributed DistArray
     * whose data is distrbuted over PlaceGroup.WORLD and 
     * zero-initialized.
     *
     * @param n number of elements
     */
    public def this(n:long){T haszero} {
        this(n, PlaceGroup.WORLD, (long)=>Zero.get[T]());
    }


    // Custom Serialization: Superclass handles serialization of localHandle
    public def serialize():SerialData = super.serialize();

    def this(sd:SerialData) { 
        super(sd.data as PlaceLocalHandle[LocalState[T]]);
        val bls = localHandle() as BLocalState[T];
        globalIndices = bls.globalIndices;
        localIndices = bls.localIndices;
        minLocalIndex = localIndices.min(0);
        maxLocalIndex = localIndices.max(0);
    }


    /**
     * Get an IterationSpace that represents all Points contained in
     * the global iteration space (valid indices) of the DistArray.
     * @return an IterationSpace for the DistArray
     */
    public @Inline final def globalIndices():DenseIterationSpace_1{self!=null} = globalIndices;


    /**
     * Get an IterationSpace that represents all Points contained in
     * the local iteration space (valid indices) of the DistArray at the current Place.
     * @return an IterationSpace for the local portion of the DistArray
     */
    public @Inline final def localIndices():DenseIterationSpace_1{self!=null} = localIndices;


    /**
     * Return the Place which contains the data for the argument
     * index or Place.INVALID_PLACE if the Point is not in the globalIndices
     * of this DistArray
     *
     * @param i the index 
     * @return the Place where i is a valid index in the DistArray; 
     *          will return Place.INVALID_PLACE if i is not contained in globalIndices
     */
    public def place(i:long):Place {
        val tmp = BlockingUtils.mapIndexToBlockPartition(globalIndices, placeGroup.size(), i);
	return tmp == -1L ? Place.INVALID_PLACE : placeGroup(tmp);
    }


    /**
     * Return the Place which contains the data for the argument
     * Point or Place.INVALID_PLACE if the Point is not in the globalIndices
     * of this DistArray
     *
     * @param p the Point to lookup
     * @return the Place where p is a valid index in the DistArray; 
     *          will return Place.INVALID_PLACE if p is not contained in globalIndices
     */
    public def place(p:Point(1)):Place = place(p(0));


    /**
     * Return the element of this array corresponding to the given index.
     * 
     * @param i the given index
     * @return the element of this array corresponding to the given index.
     * @see #set(T, Long)
     */
    public final @Inline operator this(i:long):T {
        if (CompilerFlags.checkPlace() || CompilerFlags.checkBounds()) {
            if (i < minLocalIndex || i > maxLocalIndex) {
                if (CompilerFlags.checkBounds() && (i < 0 || i >= size)) raiseBoundsError(i);
                if (CompilerFlags.checkPlace()) raisePlaceError(i);
            }
        }
        return Unsafe.uncheckedRailApply(raw, i-minLocalIndex);
    }


    /**
     * Return the element of this array corresponding to the given Point.
     * 
     * @param p the given Point
     * @return the element of this array corresponding to the given Point.
     * @see #set(T, Point)
     */
    public final @Inline operator this(p:Point(1)):T  = this(p(0));

    
    /**
     * Set the element of this array corresponding to the given index to the given value.
     * Return the new value of the element.
     * 
     * @param v the given value
     * @param i the given index 
     * @return the new value of the element of this array corresponding to the given index.
     * @see #operator(Long)
     */
    public final @Inline operator this(i:long)=(v:T):T{self==v} {
        if (CompilerFlags.checkPlace() || CompilerFlags.checkBounds()) {
            if (i < minLocalIndex || i > maxLocalIndex) {
                if (CompilerFlags.checkBounds() && (i < 0 || i >= size)) raiseBoundsError(i);
                if (CompilerFlags.checkPlace()) raisePlaceError(i);
            }
        }
        Unsafe.uncheckedRailSet(raw, i - minLocalIndex, v);
        return v;
    }


    /**
     * Set the element of this array corresponding to the given Point to the given value.
     * Return the new value of the element.
     * 
     * @param v the given value
     * @param p the given Point
     * @return the new value of the element of this array corresponding to the given Point.
     * @see #operator(Int)
     */
    public final @Inline operator this(p:Point(1))=(v:T):T{self==v} = this(p(0)) = v;
}


// TODO:  Would prefer this to be a protected static nested class, but 
//        when written that way we non-deterministically fail compilation.
class BLocalState[S] extends LocalState[S] {
    val globalIndices:DenseIterationSpace_1{self!=null};
    val localIndices:DenseIterationSpace_1{self!=null};

    def this(pg:PlaceGroup{self!=null}, data:Rail[S]{self!=null}, size:long, 
             gs:DenseIterationSpace_1{self!=null}, ls:DenseIterationSpace_1{self!=null}) {
        super(pg, data, size);
        globalIndices = gs;
        localIndices = ls;
    }

    static def make[S](pg:PlaceGroup{self!=null}, n:long, init:(long)=>S):BLocalState[S] {
        val globalSpace = new DenseIterationSpace_1(0L, n-1);
        val localSpace = BlockingUtils.partitionBlock(globalSpace, pg.numPlaces(), pg.indexOf(here));

	val data:Rail[S]{self!=null};
	if (localSpace.min(0) > localSpace.max(0)) { // TODO: add isEmpty() to IterationSpace API?
            data = new Rail[S]();
        } else {            
            val low = localSpace.min(0);
            val hi = localSpace.max(0);
            val dataSize = hi - low + 1;
            data = Unsafe.allocRailUninitialized[S](dataSize);
            for (i in low..hi) {
                val offset = i - low;
                data(offset) = init(i);
            }
        }
        return new BLocalState[S](pg, data, n, globalSpace, localSpace);
    }
}

// vim:tabstop=4:shiftwidth=4:expandtab