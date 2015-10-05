/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2015.
 *  (C) Copyright Sara Salem Hamouda 2014-2015.
 */

package x10.matrix.distblock;

import x10.util.Timer;
import x10.compiler.Inline;
import x10.regionarray.Dist;
import x10.util.Pair;
import x10.util.StringBuilder;

import x10.matrix.Matrix;
import x10.matrix.util.Debug;
import x10.matrix.ElemType;
import x10.matrix.Vector;

import x10.matrix.util.MathTool;
import x10.matrix.block.Grid;
import x10.matrix.block.BlockMatrix;

import x10.util.resilient.DistObjectSnapshot;
import x10.util.resilient.Snapshottable;
import x10.util.resilient.VectorSnapshotInfo;

import x10.util.RailUtils;
import x10.util.Team;

public type DistVector(m:Long)=DistVector{self.M==m};
public type DistVector(v:DistVector)=DistVector{self==v};

public class DistVector(M:Long) implements Snapshottable {
    public var distV:PlaceLocalHandle[Vector];
    
    public var segSizePLH:PlaceLocalHandle[Rail[Int]];
    public var offsetsPLH:PlaceLocalHandle[Rail[Int]];
    
    /*
     * Time profiling
     */
    transient var commTime:Long = 0;
    transient var calcTime:Long = 0;
    
    /*The place group used for distribution*/
    private var places:PlaceGroup;
    private var team:Team;
    
    public def places() = places;
    
    //old segSize and ossets used only for remake and restore
    public var snapshotSegSizePLH:PlaceLocalHandle[Rail[Int]];
    public var snapshotOffsetsPLH:PlaceLocalHandle[Rail[Int]];

    public def this(m:Long, vs:PlaceLocalHandle[Vector], segsz:Rail[Int], pg:PlaceGroup) {
        property(m);
        assert (segsz.size == pg.size()) :
            "number of vector segments must be equal to number of places";
        distV  = vs;
        places = pg;
        team = new Team(places);
        
        segSizePLH = PlaceLocalHandle.make[Rail[Int]](pg, ()=>segsz);        
        val offsets = RailUtils.scanExclusive(segsz, (x:Int, y:Int) => x+y, 0n);
        offsetsPLH = PlaceLocalHandle.make[Rail[Int]](pg, ()=>offsets);
        
        snapshotSegSizePLH = PlaceLocalHandle.make[Rail[Int]](pg, ()=>new Rail[Int](pg.size(), -1n) );
        snapshotOffsetsPLH = PlaceLocalHandle.make[Rail[Int]](pg, ()=>new Rail[Int](pg.size(), -1n) );
    }
    
    public def getOffset():Int{
         return offsetsPLH()(places.indexOf(here));
    }
    
    public def getSegSize():Rail[Int] = segSizePLH();
    
    public static def make(m:Long, segNum:Long, pg:PlaceGroup):DistVector(m) {
        val hdv = PlaceLocalHandle.make[Vector](pg,
                            ()=>Vector.make(Grid.compBlockSize(m, segNum, pg.indexOf(here))));
        val slst = new Rail[Int](segNum, (i:Long)=>Grid.compBlockSize(m, segNum, i as Int) as Int);
        return new DistVector(m, hdv, slst, pg) as DistVector(m);
    }
    
    public static def make(m:Long, segNum:Long):DistVector(m) = make (m, segNum, Place.places()); 
    
    public static def make(m:Long, pg:PlaceGroup) = make (m, pg.size(), pg);
    
    public static def make(m:Long) = make (m, Place.places());

    public static def make(m:Long, segsz:Rail[Int], pg:PlaceGroup):DistVector(m) {
        val hdv = PlaceLocalHandle.make[Vector](pg, ()=>Vector.make(segsz(pg.indexOf(here))));
        return new DistVector(m, hdv, segsz, pg) as DistVector(m);
    }
    
    public static def make(m:Long, segsz:Rail[Int]):DistVector(m) = make (m, segsz, Place.places());

    public def alloc(m:Long, pg:PlaceGroup):DistVector(m) = make(m, pg);
    public def alloc(pg:PlaceGroup) = alloc(M, pg);
    
    public def alloc(m:Long):DistVector(m) = make(m, Place.places());
    public def alloc() = alloc(M, Place.places());
    
    
    public def clone():DistVector(M) {
        val dv = PlaceLocalHandle.make[Vector](places, 
                ()=>distV().clone());    
        return new DistVector(M, dv, getSegSize(), places) as DistVector(M);
    }
    
    public def reset() {
        finish ateach(Dist.makeUnique(places)) {
            distV().reset();
        }
    }

    public def init(dv:ElemType) : DistVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            distV().init(dv);
        }
        return this;
    }
    
    public def initRandom() : DistVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            distV().initRandom();
        }
        return this;
    }

    /**
     * Initialize this vector with random 
     * values in the specified range.
     * @param min lower bound of random values
     * @param max upper bound of random values
     */ 
    public def initRandom(min:Long, max:Long):DistVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            distV().initRandom(min, max);
        }
        return this;
    }
    
    public def init(f:(Long)=>ElemType):DistVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            distV().init(f);
        }
        return this;
    }
    
    public def copyTo(dst:DistVector(M)):void {
        finish ateach(Dist.makeUnique(places)) {
            distV().copyTo(dst.distV());
        }
    }

    public def copyTo(vec:Vector(M)):void {
        val root = here;
        val gr = new GlobalRail[ElemType](vec.d);
        finish for (place in places) at (place) async {
            val src = distV().d;
            var dst:Rail[ElemType] = null;
            if (here.id == root.id){
                dst = gr();
            }            
            team.gatherv(root, src, 0, dst, 0, getSegSize());           
        }
    }

    public def copyFrom(vec:Vector(M)): void {
        val root = here;
        val gr = new GlobalRail[ElemType](vec.d);
        finish for (place in places) at (place) async {
            var src:Rail[ElemType] = null;
            val dst = distV().d;
            if (here.id == root.id){
                src = gr();
            }            
            team.scatterv(root,src, 0, dst, 0, getSegSize());
        }
    }

    /**
     * For debug and verification use only. 
     * This method is not efficient for actual computation.
     * Use copyTo(...) method instead.
     */
    public def toVector():Vector(M) {
        val nv = Vector.make(M);
        for (var i:Long=0; i<M; i++) nv(i) = this(i);
        return nv;
    }

    private def find(var pos:Long, segments:Rail[Int]):Pair[Long, Long] {        
        for (var i:Long=0; i<segments.size; i++) {
            if (pos < segments(i))
                return new Pair[Long,Long](i, pos);
            pos -= segments(i);
        }
        throw new UnsupportedOperationException("Error in searching index in vector");
    }
    
    protected def find(var pos:Long):Pair[Long, Long] {
        assert (pos < M) : "Vector data access out of bounds";
        return find(pos, getSegSize());
    }
    
    public  operator this(x:Long):ElemType {
        val loc = find(x);
        val seg = loc.first as Int;
        val off = loc.second;
        val dat = at(places(seg)) distV()(off);
        return dat;
    }

    public operator this(x:Long)=(dv:ElemType):ElemType {
        val loc = find(x);
        val seg = loc.first as Int;
        val off = loc.second;
        at(places(seg)) distV()(off)=dv;
        return dv;
    }

    /**
     * Scaling method. All copies are updated concurrently
     */
    public def scale(a:ElemType) {
        finish ateach(Dist.makeUnique(places)) {
            distV().scale(a);
        }
        return this;
    }

    /**
     * Concurrently perform cellwise addition on all copies.
     */
    public def cellAdd(that:DistVector(M))  {
        //assert (this.M==A.M&&this.N==A.N);
        finish ateach(Dist.makeUnique(places)) {
            val dst = distV();
            val src = that.distV() as Vector(dst.M);
            dst.cellAdd(src);
        }
        return this;
    }

    public def cellAdd(dv:ElemType)  {
        finish ateach(Dist.makeUnique(places)) {
            distV().cellAdd(dv);
        }
        return this;
    }

    /**
     * Concurrently perform cellwise subtraction on all copies
     */
    public def cellSub(A:DistVector(M)) {
        finish ateach(Dist.makeUnique(places)) {
            val dst = distV();
            val src = A.distV() as Vector(dst.M);
            dst.cellSub(src);
        }
        return this;
    }

    
    /**
     * Perform cell-wise subtraction  this = this - dv.
     */
    public def cellSub(dv:ElemType):DistVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            distV().cellSub(dv);
        }
        return this;
    }

    /**
     * Cellwise multiplication. All copies are modified with
     * the corresponding vector copies.
     */
    public def cellMult(A:DistVector(M)) {
        finish ateach(Dist.makeUnique(places)) {
            val dst = this.distV();
            val src = A.distV() as Vector(dst.M);
            dst.cellMult(src);
        }
        return this;
    }

    /**
     * Cellwise division. All copies are modified with
     * the corresponding vector copies.
     */    
    public def cellDiv(A:DistVector(M)) {
        finish ateach(Dist.makeUnique(places)) {
            val dst = this.distV();
            val src = A.distV() as Vector(dst.M);
            dst.cellDiv(src);
        }
        return this;
    }
    

    // Operator overloading cellwise operations

    public operator - this            = clone().scale(-1.0 as ElemType) as DistVector(M);
    public operator (v:ElemType) + this = clone().cellAdd(v)  as DistVector(M);
    public operator this + (v:ElemType) = clone().cellAdd(v)  as DistVector(M);
    public operator this - (v:ElemType) = clone().cellSub(v)  as DistVector(M);
    public operator this / (v:ElemType) = clone().scale((1.0/v) as ElemType)   as DistVector(M);
    
    public operator this * (alpha:ElemType) = clone().scale(alpha) as DistVector(M);
    public operator (alpha:ElemType) * this = this * alpha;
    
    public operator this + (that:DistVector(M)) = clone().cellAdd(that)  as DistVector(M);
    public operator this - (that:DistVector(M)) = clone().cellSub(that)  as DistVector(M);
    public operator this * (that:DistVector(M)) = clone().cellMult(that) as DistVector(M);
    public operator this / (that:DistVector(M)) = clone().cellDiv(that)  as DistVector(M);

    public def dot(v:DupVector(M)):Double {
        val dot = finish(Reducible.SumReducer[Double]()) {
            var off:Long=0;
            val segSize = getSegSize();
            for (p in 0..(places.size()-1)) {
                val offset = off;
                val s = segSize(p);
                at(places(p)) async {
                    val dist = distV();
                    val dup = v.local();
                    var d:Double = 0.0;
                    for (i in 0..(s-1))
                        d += dist(i) * dup(offset+i);
                    offer d;
                }
                off += s;
            }
        };
        return dot;
    }
        

    // Multiplication operations 

    public def mult(mA:DistBlockMatrix(M), vB:DupVector(mA.N), plus:Boolean):DistVector(this) =
        DistDupVectorMult.comp(mA, vB, this, plus);
    
    public def mult(vB:DupVector, mA:DistBlockMatrix(vB.M, this.M), plus:Boolean):DistVector(this) =
        DistDupVectorMult.comp(vB, mA, this, plus);

    public def mult(mA:DistBlockMatrix(M), vB:DupVector(mA.N))      = DistDupVectorMult.comp(mA, vB, this, false);
    public def mult(vB:DupVector, mA:DistBlockMatrix(vB.M, this.M)) = DistDupVectorMult.comp(vB, mA, this, false);
    public def mult_local(mA:DistBlockMatrix(M), vB:DupVector(mA.N))      = DistDupVectorMult.comp_local(mA, vB, this, false);

    //FIXME: review the correctness of using places here
    public operator this % (that:DistBlockMatrix(this.M)) = 
         DistDupVectorMult.comp(this, that, DupVector.make(that.N, places), false);
    //FIXME: review the correctness of using places here
    public operator (that:DistBlockMatrix{self.N==this.M}) % this = 
        DistDupVectorMult.comp(that, this, DupVector.make(that.M, places), false);

    /** Get the sum of all elements in this vector. */
    public def sum():ElemType = reduce((a:ElemType,b:ElemType)=>{a+b}, 0.0 as ElemType);
    
    public def likeMe(that:DistVector): Boolean  {
        if (this.M!=that.M) return false;
        val segSize = getSegSize();
        for (var i:Long=0; i<segSize.size; i++)
            if (segSize(i) != that.getSegSize()(i)) return false;
        return true;
    }
        
    public def equals(dv:DistVector(this.M)):Boolean {
        var ret:Boolean = true;
        if (dv.getPlaces().equals(places)){
            for (var p:Long=0; p<places.size() &&ret; p++) {
                val pindx = p;
                ret &= at(places(pindx)) {
                    val srcv = distV();
                    val tgtv = dv.distV() as Vector(srcv.M);
                    srcv.equals(tgtv)
                };
            }
        }
        else
            ret = false;
        return ret;
    }
    public def equals(that:Vector(this.M)):Boolean {
        var ret:Boolean = true;
        var i:Long=0;
        for (; i<M&&ret; i++) ret &= MathTool.isZero(this(i)-that(i));
        if (!ret) {
            Debug.flushln("Diff found at index:"+i+" value: "+this(i)+" <> "+that(i));
        }
        return ret;
    }
    
    public def equals(dval:ElemType):Boolean {
        var ret:Boolean = true;
        for (var p:Long=0; p<places.size() &&ret; p++) {
            val pindx = p;
            ret &= at(places(pindx)) distV().equals(dval);
        }
        return ret;
    }

    /**
     * Apply the map function <code>op</code> to each element of this vector,
     * overwriting the element of this vector with the result.
     * @param op a unary map function to apply to each element of this vector
     * @return this vector, containing the result of the map
     */
    public final @Inline def map(op:(x:ElemType)=>ElemType):DistVector(this) {
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = distV();
            d.map(op);
        }
        calcTime += Timer.milliTime() - stt;
        return this;
    }

    /**
     * Apply the map function <code>op</code> to each element of <code>a</code>,
     * storing the result in the corresponding element of this vector.
     * @param a a vector of the same distribution as this vector
     * @param op a unary map function to apply to each element of vector <code>a</code>
     * @return this vector, containing the result of the map
     */
    public final @Inline def map(a:DistVector(M), op:(x:ElemType)=>ElemType):DistVector(this) {
        assert(likeMe(a));
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = distV();
            val ad = a.distV() as Vector(d.M);
            d.map(ad, op);
        }
        calcTime += Timer.milliTime() - stt;
        return this;
    }

    /**
     * Apply the map function <code>op</code> to combine each element of vector
     * <code>a</code> with the corresponding element of vector <code>b</code>,
     * overwriting the corresponding element of this vector with the result.
     * @param a first vector of the same distribution as this vector
     * @param b second vector of the same distribution as this vector
     * @param op a binary map function to apply to each element of 
     *   <code>a</code> and the corresponding element of <code>b</code>
     * @return this vector, containing the result of the map
     */
    public final @Inline def map(a:DistVector(M), b:DistVector(M), op:(x:ElemType,y:ElemType)=>ElemType):DistVector(this) {
        assert(likeMe(a));
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = distV();
            val ad = a.distV() as Vector(d.M);
            val bd = b.distV() as Vector(d.M);
            d.map(ad, bd, op);
        }
        calcTime += Timer.milliTime() - stt;
        return this;
    }

    /**
     * Combine the elements of this vector using the provided reducer function.
     * @param op a binary reducer function to combine elements of this vector
     * @param unit the identity value for the reduction function
     * @return the result of the reducer function applied to all elements
     */
    public final @Inline def reduce(op:(a:ElemType,b:ElemType)=>ElemType, unit:ElemType):ElemType {
        class Reducer implements Reducible[ElemType] {
            public def zero() = unit;
            public operator this(a:ElemType, b:ElemType) = op(a,b); 
        }
        val stt = Timer.milliTime();
        val reducer = new Reducer();
        val result = finish (reducer) {
            ateach(Dist.makeUnique(places)) {
                val d = distV();
                offer d.reduce(op, unit);
            }
        };
        calcTime += Timer.milliTime() - stt;
        return result;
    }

    public def getCalcTime() = calcTime;
    public def getCommTime() = commTime;
    public def getPlaces() = places;
    
    public def toString() :String {
        val output=new StringBuilder();
        output.add("---Distributed Vector:["+M+"], ---\n[ ");
        for (var i:Long=0; i<M-1; i++) output.add(this(i).toString()+",");
        
        output.add(this(M-1).toString()+" ]\n--------------------------------------------------\n");
        return output.toString();
    }

    public def allToString() {
        val output = new StringBuilder();
        output.add( "-------- Distributed vector :["+M+"] ---------\n");
        for (p in places) {
            output.add("Segment vector at place " + p.id() +"\n");
            output.add(at (p) { distV().toString()});
        }
        output.add("--------------------------------------------------\n");
        return output.toString();
    }
    
    /*
     * Snapshot mechanism
     */
    /**
     * Remake the DistVector over a new PlaceGroup
     */
    public def remake(segsz:Rail[Int], newPg:PlaceGroup){        
        assert (segsz.size == newPg.size()) :
            "number of vector segments must be equal to number of places";
        PlaceLocalHandle.destroy(places, distV, (Place)=>true);
        PlaceLocalHandle.destroy(places, segSizePLH, (Place)=>true);
        PlaceLocalHandle.destroy(places, offsetsPLH, (Place)=>true);
        
        distV = PlaceLocalHandle.make[Vector](newPg, ()=>Vector.make(segsz(newPg.indexOf(here))));       
        places = newPg;
        team.delete();
        team = new Team(places);
        segSizePLH = PlaceLocalHandle.make[Rail[Int]](newPg, ()=>segsz);
        
        val offsets = RailUtils.scanExclusive(segsz, (x:Int, y:Int) => x+y, 0n);
        offsetsPLH = PlaceLocalHandle.make[Rail[Int]](newPg, ()=>offsets);
    }
    
    /**
     * Remake the DistVector over a new PlaceGroup
     */
    public def remake(newPg:PlaceGroup){
        val m = M;        
        val segNum = newPg.size;
        val slst = new Rail[Int](segNum, (i:Long)=>Grid.compBlockSize(m, segNum, i as Int) as Int);
        remake (slst, newPg);
    }

    /**
     * Create a snapshot for the DistVector data 
     * @return a snapshot for the DistVector data stored in a resilient store
     */
    public def makeSnapshot():DistObjectSnapshot {
        //val startTime = Timer.milliTime();
        val snapshot = DistObjectSnapshot.make();
        finish ateach(pl in Dist.makeUnique(places)){
            val i = places.indexOf(here);
            val data = distV();
            //the segSize should only be saved only at place 0
            val distVecInfo = new VectorSnapshotInfo(i, data.d);
            snapshot.save(i, distVecInfo);
            
            Rail.copy(segSizePLH(), 0, snapshotSegSizePLH(), 0, segSizePLH().size);
            Rail.copy(offsetsPLH(), 0, snapshotOffsetsPLH(), 0, offsetsPLH().size);        
        }
        //Console.OUT.println("DistVector.SnapshotTime["+(Timer.milliTime() - startTime)+"]");
        return snapshot;
    }
    
    public def restoreSnapshot(snapshot:DistObjectSnapshot, localViewFlag:Boolean) {
        //val startTime = Timer.milliTime();
        val currentSegSizes = segSizePLH();
        val snapshotSegSize = snapshotSegSizePLH();
    
        assert (snapshotSegSize != null && currentSegSizes != null && snapshotSegSize(0) != -1n) : "Invalid segments rails";
    
        var segmentsChanged:Boolean = false;
        if (snapshotSegSize.size == currentSegSizes.size) {
            for (var i:Long = 0; i < snapshotSegSize.size; i++) {
                if (snapshotSegSize(i) != currentSegSizes(i)) {
                    segmentsChanged = true;
                    break;
                }
            }
        }
        else
            segmentsChanged = true;

        if (!localViewFlag){
            if (!segmentsChanged)
                restoreSnapshotSegmentBySegment(snapshot);
            else
                restoreSnapshotElementByElement(snapshot);
        }
        else {
            if (!segmentsChanged)
                restoreSnapshotSegmentBySegment_local(snapshot);
            else
                restoreSnapshotElementByElement_local(snapshot);
        }
        //Console.OUT.println("DistVector.RestoreTime["+(Timer.milliTime() - startTime)+"]");
    }
    
    /**
     * Restore the DistVector data using the provided snapshot object 
     * @param snapshot a snapshot from which to restore the data
     */
    public def restoreSnapshot(snapshot:DistObjectSnapshot) {
        //val startTime = Timer.milliTime();
        restoreSnapshot(snapshot, false);
        //Console.OUT.println("DistVector.RestoreTime["+(Timer.milliTime() - startTime)+"]");
    }
    
    private def restoreSnapshotSegmentBySegment(snapshot:DistObjectSnapshot) {
        //val startTime = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            //segments should be stored in places with the same order 
            //segment place index will remain the same
            val segmentPlaceIndex = places.indexOf(here);
            val storedSegment = snapshot.load(segmentPlaceIndex) as VectorSnapshotInfo;
            val srcRail = storedSegment.data;
            val dstRail = distV().d;
            Rail.copy(srcRail, 0, dstRail, 0, srcRail.size);
        }        
        //Console.OUT.println("DistVector.RestoreTimeSegmentBySegment["+(Timer.milliTime() - startTime)+"]");
    }
    
    private def restoreSnapshotElementByElement(snapshot:DistObjectSnapshot) {
        //val startTime = Timer.milliTime();       
        finish ateach(Dist.makeUnique(places)) {
            val newSegSize = segSizePLH();
            val snapshotSegSize = snapshotSegSizePLH();

            val newSegmentsOffsets = offsetsPLH();
            val oldSegmentsOffsets = snapshotOffsetsPLH();
        
            val segmentPlaceIndex = places.indexOf(here);
            val low = newSegmentsOffsets(segmentPlaceIndex);
            val high = low + newSegSize(segmentPlaceIndex);
            
            var offset:Long = 0;
            for (var i:Long = 0; i < snapshotSegSize.size; i++) {
                val low_old = oldSegmentsOffsets(i);
                val high_old = low_old + snapshotSegSize(i);
                
                var overlapFound:Boolean = false;
                if (high_old > low && low_old < high) {
                   //calculate the overlapping interval
                   var startIndex:Long = low;
                   var endIndex:Long = high;
                   if (low_old > low)
                       startIndex = low_old;
                   if (high_old < high)
                       endIndex = high_old;
                   //load the old segment from resilient store
                   var storedSegment:VectorSnapshotInfo = snapshot.load(i) as VectorSnapshotInfo;
                   val srcRail = storedSegment.data;
                   val dstRail = distV().d;
                   
                   val elemCount = endIndex - startIndex;
             
                   var srcOffset:Long = 0;
                   if (low_old < low)
                       srcOffset = low - low_old;
                   
                   Rail.copy(srcRail, srcOffset, dstRail, offset, elemCount);
                   offset+= elemCount;
                   
                   overlapFound = true;
                } else if (overlapFound) {
                    break; // no more overlapping segments exist
                }
            }
        }
        //Console.OUT.println("DistVector.RestoreTimeElementByElement["+(Timer.milliTime() - startTime)+"]");
    }
    
    //val snapshot = DistObjectSnapshot.make();
    public def makeSnapshot_local(snapshot:DistObjectSnapshot):void {
        val i = places.indexOf(here);
        val data = distV();        
        val distVecInfo = new VectorSnapshotInfo(i, data.d);
        snapshot.save(i, distVecInfo);       
        
        Rail.copy(segSizePLH(), 0, snapshotSegSizePLH(), 0, segSizePLH().size);
        Rail.copy(offsetsPLH(), 0, snapshotOffsetsPLH(), 0, offsetsPLH().size);
    }
    
    public def restoreSnapshot_local(snapshot:DistObjectSnapshot) {        
        //val startTime = Timer.milliTime();
        restoreSnapshot(snapshot, true);
        //Console.OUT.println("DistVector.RestoreTime["+(Timer.milliTime() - startTime)+"]");
    }
    
    private def restoreSnapshotSegmentBySegment_local(snapshot:DistObjectSnapshot) {
        val segmentPlaceIndex = places.indexOf(here);
        val storedSegment = snapshot.load(segmentPlaceIndex) as VectorSnapshotInfo;
        val srcRail = storedSegment.data;
        val dstRail = distV().d;
        Rail.copy(srcRail, 0, dstRail, 0, srcRail.size);
    }
    
    private def restoreSnapshotElementByElement_local(snapshot:DistObjectSnapshot) {
        //val startTime = Timer.milliTime();
        val newSegSize = segSizePLH();
        val snapshotSegSize = snapshotSegSizePLH();

        val newSegmentsOffsets = offsetsPLH();
        val oldSegmentsOffsets = snapshotOffsetsPLH();
        
        val segmentPlaceIndex = places.indexOf(here);
        val low = newSegmentsOffsets(segmentPlaceIndex);
        val high = low + newSegSize(segmentPlaceIndex);
    
        var offset:Long = 0;
        for (var i:Long = 0; i < snapshotSegSize.size; i++) {
            val low_old = oldSegmentsOffsets(i);
            val high_old = low_old + snapshotSegSize(i);
    
            var overlapFound:Boolean = false;
            if (high_old > low && low_old < high) {
                //calculate the overlapping interval
                var startIndex:Long = low;
                var endIndex:Long = high;
                if (low_old > low)
                    startIndex = low_old;
                if (high_old < high)
                    endIndex = high_old;
                //load the old segment from resilient store
                var storedSegment:VectorSnapshotInfo = snapshot.load(i) as VectorSnapshotInfo;
                val srcRail = storedSegment.data;
                val dstRail = distV().d;
                val elemCount = endIndex - startIndex;
    
                var srcOffset:Long = 0;
                if (low_old < low)
                    srcOffset = low - low_old;
    
                Rail.copy(srcRail, srcOffset, dstRail, offset, elemCount);
                offset+= elemCount;
    
                overlapFound = true;
            } else if (overlapFound) {
                break; // no more overlapping segments exist
            }
        }        
        //Console.OUT.println("DistVector.RestoreTimeElementByElement["+(Timer.milliTime() - startTime)+"]");
    }
    
}
