/*
 * Created on Oct 20, 2004
 */
package x10.array.sharedmemory;

import java.util.Iterator;



import x10.array.DoubleArray;
import x10.array.IntArray;
import x10.array.Operator;
import x10.array.IntArray.Assign;
import x10.base.Allocator;
import x10.base.MemoryBlock;
import x10.base.UnsafeContainer;
import x10.lang.Runtime;
import x10.lang.point;
import x10.lang.distribution;
import x10.lang.region;


/**
 * @author Christian Grothoff, Christoph von Praun
 */
public class DoubleArray_c extends DoubleArray implements UnsafeContainer {

    private boolean safe_;
    private final MemoryBlock arr_;
    
    /**
     *  This constructor must not be used directly by an application programmer.
     * Arrays are constructed by the corresponding factory methods in 
     * x10.lang.Runtime.
     */
    protected DoubleArray_c(Distribution_c d, boolean safe) {
        this(d, (Operator.Pointwise) null, safe);
    }
    
    protected DoubleArray_c(Distribution_c d, Operator.Pointwise c, boolean safe) {
        super(d);
        int count =  d.region.size();
        this.arr_ = safe ? Allocator.allocSafe(count, Double.TYPE) : Allocator.allocUnsafe(count, Allocator.SIZE_DOUBLE);
        if (c != null)
            pointwise(this, c);
    }
    
    /** Create a new array per the given distribution, initialized to c.
     * 
     * @param d
     * @param c
     * @param safe
     */
    public DoubleArray_c( distribution d, double c) {
	this(d, c, true);
    }
    public DoubleArray_c( distribution d, double c, boolean safe) {
    	super(d);
    	int count =  d.region.size();
    	this.arr_ = safe ? Allocator.allocSafe(count, Double.TYPE) : Allocator.allocUnsafe(count, Allocator.SIZE_DOUBLE);
    	scan(this, new Assign(c));
    	
    }
    public DoubleArray_c( distribution d, DoubleArray.pointwiseOp f) {
	this(d, f, true);
    }
    public DoubleArray_c( distribution d, DoubleArray.pointwiseOp f, boolean safe) {
    	super(d);
    	int count =  d.region.size();
    	this.arr_ = safe ? Allocator.allocSafe(count, Double.TYPE) : Allocator.allocUnsafe(count, Allocator.SIZE_DOUBLE);
    	scan(this, f);
    	
    }
    
    private DoubleArray_c( distribution d, double[] a) {
    	super(d);
    	this.arr_ = Allocator.allocSafeDoubleArray( a);
    }
    /** Return a safe IntArray_c initialized with the given local 1-d (Java) int array.
     * 
     * @param a
     * @return
     */
    public static DoubleArray_c DoubleArray_c( double[] a ) {
    	distribution d = Runtime.factory.getDistributionFactory().here(a.length);
    	return new DoubleArray_c(d, a);
    }
    
    public void keepItLive() {}
    
    public long getUnsafeAddress() {
        return arr_.getUnsafeAddress();
    }
    
    /* Overrides the superclass method - this implementation is more efficient */
    public void reduction(Operator.Reduction op) {
        int count = arr_.count();
        for (int i  = 0; i < count; ++i) 
            op.apply(arr_.getDouble(i));
    }
    
    /* Overrides the superclass method - this implementation is more efficient */
    protected void assign(DoubleArray rhs) {
    	assert rhs instanceof DoubleArray_c;
    	
    	DoubleArray_c rhs_t = (DoubleArray_c) rhs;
    	if (rhs.distribution.equals(distribution)) {
    	    int count = arr_.count();
    		for (int i  = 0; i < count; ++i) 
    			arr_.setDouble(rhs_t.arr_.getDouble(i), i);
    	} else 
    		// fall back to generic implementation
    		super.assign(rhs);
    }

	protected DoubleArray newInstance(distribution d) {
		assert d instanceof Distribution_c;
		
		return new DoubleArray_c((Distribution_c) d, safe_);	
	}
	
	protected DoubleArray newInstance(distribution d, Operator.Pointwise c) {
		assert d instanceof Distribution_c;
		
		return new DoubleArray_c((Distribution_c) d, c, safe_);	
	}
	

    public x10.lang.doubleArray lift( DoubleArray.binaryOp op, x10.lang.doubleArray arg ) {
        assert arg.distribution == this.distribution; 
        DoubleArray arg1 = (DoubleArray)arg;
        DoubleArray result = newInstance(distribution);
        for (Iterator it = distribution.region.iterator(); it.hasNext();) {
            point p = (point) it.next();
             result.set(op.apply(this.get(p), arg1.get(p)),p);
        }
        return result;
    }

    public double reduce( DoubleArray.binaryOp op, double unit ) {
        double result = unit;
        for (Iterator it = distribution.region.iterator(); it.hasNext();) {
            point p = (point) it.next();
             result = op.apply(this.get(p), result);
        }
        return result;
    }

    public x10.lang.doubleArray scan( binaryOp op, double unit ) {
        double temp = unit;
        DoubleArray result = newInstance(distribution);
        for (Iterator it = distribution.region.iterator(); it.hasNext();) {
            point p = (point) it.next();
            temp = op.apply(this.get(p), temp);
             result.set(temp, p);
        }
        return result;
    }
    
	
    /* (non-Javadoc)
     * @see x10.lang.DoubleArray#set(int, int[])
     */
    public void set(double v, point pos) {
        arr_.setDouble(v, (int) distribution.region.ordinal(pos));
    }
    
    
    public void set(double v, int d0) {
    	assert this.region.rank == 1;
        int[] pos = {d0};
    	final point p = Runtime.factory.getPointFactory().point(this.region, pos);
        set(v, p);
    }
    
    public void set(double v, int d0, int d1) {
    	assert this.region.rank == 2;
        int[] pos = {d0, d1};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
        set(v, p);
    }
    
    public void set(double v, int d0, int d1, int d2) {
    	assert this.region.rank == 3;
        int[] pos = {d0, d1, d2};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
        set(v, p);
    }
    
    public void set(double v, int d0, int d1, int d2, int d3) {
    	assert this.region.rank == 4;
        int[] pos = {d0, d1, d2, d3};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
        set(v, p);
        
    }

    /* (non-Javadoc)
     * @see x10.lang.DoubleArray#get(int[])
     */
    public double get(point pos) {
        return arr_.getDouble((int) distribution.region.ordinal(pos));
    }
    
    public double get(int d0) {
    	assert this.region.rank == 1;
        int[] pos = {d0};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
    	return get(p);
    }
    public double get(int d0, int d1) {
    	assert this.region.rank == 2;
        int[] pos = {d0, d1};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
    	return get(p);
    }
    public double get(int d0, int d1, int d2) {
    	assert this.region.rank == 3;
        int[] pos = {d0, d1, d2};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
    	return get(p);
    }
    public double get(int d0, int d1, int d2, int d3) {
    	assert this.region.rank == 4;
        int[] pos = {d0, d1, d2, d3};
        final point p = Runtime.factory.getPointFactory().point(this.region, pos);
    	return get(p);
    }
    public x10.lang.doubleArray overlay(x10.lang.doubleArray d) {
    	throw new Error("TODO");
    }
    public x10.lang.doubleArray union(x10.lang.doubleArray d) {
    	throw new Error("TODO");
    }
    public x10.lang.doubleArray restriction( distribution d) {
    	throw new Error("TODO");
    }
    public x10.lang.doubleArray restriction( region d) {
    	throw new Error("TODO");
    }
    
}
