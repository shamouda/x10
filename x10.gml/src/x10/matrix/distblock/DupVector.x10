/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2015.
 */

package x10.matrix.distblock;

import x10.compiler.Inline;
import x10.regionarray.Dist;
import x10.util.Timer;
import x10.util.StringBuilder;

import x10.matrix.Vector;
import x10.matrix.ElemType;

import x10.matrix.util.Debug;

import x10.util.resilient.iterative.DistObjectSnapshot;
import x10.util.resilient.iterative.Snapshottable;
import x10.util.resilient.VectorSnapshotInfo;

import x10.util.Team;
import x10.util.ArrayList;

public type DupVector(m:Long)=DupVector{self.M==m};
public type DupVector(v:DupVector)=DupVector{self==v};

public class DupVector(M:Long) implements Snapshottable {
    public var dupV:PlaceLocalHandle[DupVectorLocalState];
    public var team:Team;
    private var places:PlaceGroup;
    
    /*
     * Time profiling
     */
    transient var commTime:Long = 0;
    transient var calcTime:Long = 0;   

    public def this(vs:PlaceLocalHandle[DupVectorLocalState], pg:PlaceGroup) {
        val m = vs().vec.M;
        property(m);
        dupV  = vs;
        team = new Team(pg);
        places = pg;
    }
    
    public def this(vs:PlaceLocalHandle[DupVectorLocalState], pg:PlaceGroup, team:Team) {
        val m = vs().vec.M;
        property(m);
        dupV  = vs;
        this.team = team;
        places = pg;
    }

    public static def make(v:Vector, pg:PlaceGroup):DupVector(v.M){
        val hdl = PlaceLocalHandle.make[DupVectorLocalState](pg, () => new DupVectorLocalState(v, pg.indexOf(here)));
        return new DupVector(hdl, pg) as DupVector(v.M);
    }
    
    public static def make(v:Vector):DupVector(v.M) = make (v, Place.places());
    
    public static def make(m:Long, pg:PlaceGroup) {
        val hdl = PlaceLocalHandle.make[DupVectorLocalState](pg, ()=> new DupVectorLocalState(Vector.make(m), pg.indexOf(here)) );
        return new DupVector(hdl, pg) as DupVector(m);
    }
    
    public static def make(m:Long, pg:PlaceGroup, team:Team) {
        val hdl = PlaceLocalHandle.make[DupVectorLocalState](pg, ()=> new DupVectorLocalState(Vector.make(m), pg.indexOf(here)) );
        return new DupVector(hdl, pg, team) as DupVector(m);
    }
    
    public static def make(m:Long) = make(m, Place.places());
    
    public def alloc(m:Long, pg:PlaceGroup):DupVector(m) = make(m, pg);
    public def alloc(pg:PlaceGroup) = alloc(M, pg);
    public def alloc(m:Long):DupVector(m) = make(m, Place.places());
    public def alloc() = alloc(M, Place.places());
    
    
    public def clone():DupVector(M) {
        val bs = PlaceLocalHandle.make[DupVectorLocalState](places, 
                ()=>dupV().clone());    
        return new DupVector(bs, places) as DupVector(M);
    }
    
    public def reset() {
        finish ateach(Dist.makeUnique(places)) {
            dupV().vec.reset();
        }
    }

    public def init(dv:ElemType) : DupVector(this) {
        finish ateach(Dist.makeUnique(places)) {
            dupV().vec.init(dv);
        }
        return this;
    }
    
    public def initRandom() : DupVector(this) {
        dupV().vec.initRandom();
        sync();
        return this;
    }
    
    public def initRandom_local(root:Place) {
        dupV().vec.initRandom();
        sync_local(root);
    }
    
    public def initRandom(lo:Int, up:Int) : DupVector(this) {
        dupV().vec.initRandom(lo, up);
        sync();
        return this;
    }
    
    public def init(f:(Long)=>ElemType) : DupVector(this) {
        dupV().vec.init(f);
        sync();
        return this;
    }

    public def copyTo(dst:DupVector(M)):void {
        finish ateach(Dist.makeUnique(places)) {
            dupV().vec.copyTo(dst.dupV().vec);
        }
    }

    public def copyFrom(vec:Vector(M)):void {
        vec.copyTo(local());
        sync();
    }
    
    public  operator this(x:Long):ElemType = dupV().vec(x);

    public operator this(x:Long)=(dv:ElemType):ElemType {
        finish ateach(Dist.makeUnique(places)) {
            //Remote capture: x, y, d
            dupV().vec(x) = dv;    
        }
        return dv;
    }
    
    public def local() = dupV().vec as Vector(M);

    /**
     * Scale all elements: this *= alpha
     * All copies are updated concurrently.
     */
    public def scale(alpha:ElemType)
        = map((x:ElemType)=>{alpha * x});

    /**
     * this = alpha * V
     */
    public def scale(alpha:Double, V:DupVector(M))
        = map(V, (v:Double)=> {alpha * v});
    
    /** 
     * Cellwise addition: this = this + V
     */
    public def cellAdd(V:Vector(M)) {
        val v = local();
        v.cellAdd(V as Vector(v.M));
        sync();
        return this;
    }

    /**
     * Cellwise addition: this = this + V
     * All copies are updated concurrently.
     */
    public def cellAdd(V:DupVector(M))
        = map(this, V, (x:Double, v:Double)=> {x + v});

    /**
     * Cellwise addition: this = this + d
     * All copies are updated concurrently.
     */
    public def cellAdd(d:ElemType)
        = map((x:ElemType)=> {x + d});

    /** 
     * Cellwise subtraction: this = this - V
     */
    public def cellSub(V:Vector(M)) {
        val v = local() as Vector(M);
        v.cellSub(V);
        sync();
        return this;
    }

    /**
     * Cellwise subtraction: this = this - V
     * All copies are updated concurrently.
     */
    public def cellSub(V:DupVector(M))
        = map(this, V, (x:Double, v:Double)=> {x - v});
   
    /**
     * Cellwise subtraction:  this = this - d
     * All copies are updated concurrently.
     */
    public def cellSub(d:ElemType)
        = map((x:ElemType)=> {x - d});

    /**
     * Cellwise multiplication. 
     */
    public def cellMult(V:Vector(M)) {
        val dstv = local();
        dstv.cellMult(V);
        sync();
        return this;
    }
    
    /**
     * Cellwise multiplication
     * All copies are updated concurrently.
     */
    public def cellMult(V:DupVector(M))
        = map(this, V, (x:Double, v:Double)=> {x * v});

    /**
     * Cellwise division: this = this / V
     */
    public def cellDiv(V:Vector(M)) {
        local().cellDiv(V);
        sync();
        return this;
    }

    /**
     * Cellwise division
     * All copies are updated concurrently.
     */
    public def cellDiv(V:DupVector(M))
        = map(this, V, (x:Double, v:Double)=> {x / v});
    

    // Operator overloading cellwise operations

    public operator - this            = clone().scale(-1.0 as ElemType) as DupVector(M);
    public operator (v:ElemType) + this = clone().cellAdd(v)  as DupVector(M);
    public operator this + (v:ElemType) = clone().cellAdd(v)  as DupVector(M);
    public operator this - (v:ElemType) = clone().cellSub(v)  as DupVector(M);
    public operator this / (v:ElemType) = clone().scale((1.0/v) as ElemType)   as DupVector(M);
    
    public operator this * (alpha:ElemType) = clone().scale(alpha) as DupVector(M);
    public operator (alpha:ElemType) * this = this * alpha;
    
    public operator this + (that:DupVector(M)) = clone().cellAdd(that)  as DupVector(M);
    public operator this - (that:DupVector(M)) = clone().cellSub(that)  as DupVector(M);
    public operator this * (that:DupVector(M)) = clone().cellMult(that) as DupVector(M);
    public operator this / (that:DupVector(M)) = clone().cellDiv(that)  as DupVector(M);

    /**
     * Dot (scalar) product of this vector with another DupVector
     */
    public def dot(v:DupVector(M)):Double {
        return local().dot(v.local());
    }

    /**
     * L2-norm (Euclidean norm) of this vector, i.e. the square root of the
     * sum of squares of all elements
     */
    public def norm():Double {
        return local().norm();
    }
         
    // Multiplication operations 

    public def mult(mA:DistBlockMatrix(this.M), vB:DistVector(mA.N), plus:Boolean):DupVector(this) =
        DistDupVectorMult.comp(mA, vB, this, plus);
    
    public def mult(vB:DistVector, mA:DistBlockMatrix(vB.M, this.M), plus:Boolean):DupVector(this) =
        DistDupVectorMult.comp(vB, mA, this, plus);
    
    public def mult(mA:DistBlockMatrix(this.M), vB:DupVector(mA.N), plus:Boolean):DupVector(this) =
        DistDupVectorMult.comp(mA, vB, this, plus);
    
    public def mult(vB:DupVector, mA:DistBlockMatrix(vB.M, this.M), plus:Boolean):DupVector(this) =
        DistDupVectorMult.comp(vB, mA, this, plus);

    public def mult(mA:DistBlockMatrix(this.M), vB:DistVector(mA.N)) = DistDupVectorMult.comp(mA, vB, this, false);
    public def mult(vB:DistVector, mA:DistBlockMatrix(vB.M, this.M)) = DistDupVectorMult.comp(vB, mA, this, false);
    public def mult_local(root:Place, vB:DistVector, mA:DistBlockMatrix(vB.M, this.M)) {
        dupV().multTime -= Timer.milliTime();
        dupV().multTimeIt(dupV().multTimeIndex) -= Timer.milliTime();
        
        val result = DistDupVectorMult.comp_local(root, vB, mA, this, false);
        
        dupV().multTime += Timer.milliTime();
        dupV().multTimeIt(dupV().multTimeIndex++) += Timer.milliTime();
        
        return result;
    }
    public def mult(mA:DistBlockMatrix(this.M), vB:DupVector(mA.N))  = DistDupVectorMult.comp(mA, vB, this, false);
    public def mult(vB:DupVector, mA:DistBlockMatrix(vB.M, this.M))  = DistDupVectorMult.comp(vB, mA, this, false);
    
    //FIXME: review the correctness of using places here
    public operator this % (that:DistBlockMatrix(M)) = 
        DistDupVectorMult.comp(this, that, DistVector.make(that.N, that.getAggColBs(), places, team), true);
    //FIXME: review the correctness of using places here
    public operator (that:DistBlockMatrix{self.N==this.M}) % this = 
        DistDupVectorMult.comp(that , this, DistVector.make(that.M, that.getAggRowBs(), places, team), true);


    public def sync():void {
        /* Timing */ val st = Timer.milliTime();
        val root = here;
        finish for (place in places) at (place) async {
            var src:Rail[ElemType] = null;
            val dst = dupV().vec.d;
            if (here.id == root.id){
                src = dupV().vec.d;
            }        	
            dupV().bcastTime -= Timer.milliTime(); 
            team.bcast(root, src, 0, dst, 0, M);
            dupV().bcastTime += Timer.milliTime();
        }
        /* Timing */ commTime += Timer.milliTime() - st;
    }
    
    
    public def sync_local(root:Place):void {
        /* Timing */ val st = Timer.milliTime();
        var src:Rail[ElemType] = null;
        val dst = dupV().vec.d;
        if (here.id == root.id){
            src = dupV().vec.d;
        }
        dupV().bcastTime -= Timer.milliTime();
        team.bcast(root, src, 0, dst, 0, M);  
        dupV().bcastTime += Timer.milliTime();
        /* Timing */ commTime += Timer.milliTime() - st;
    }
    

    public def reduce(op:Int): void {
        /* Timing */ val st = Timer.milliTime();        
        val root = here;
        finish for (place in places) at (place) async {
        	val src = dupV().vec.d;
            val dst = dupV().vec.d; // if null at non-root, Team hangs
            //if (here.id == root.id){
        	//    dst = dupV().d;
            //}
            
            dupV().reduceTime -= Timer.milliTime();
        	team.reduce(root, src, 0, dst, 0, M, op);
            dupV().reduceTime += Timer.milliTime();
        }
        /* Timing */ commTime += Timer.milliTime() - st;
    }
    
    public def reduceSum(): void {
        /* Timing */ val st = Timer.milliTime();        
        reduce(Team.ADD);
        /* Timing */ commTime += Timer.milliTime() - st;
    }
    
    public def allReduce(op:Int): void {
    	finish for (place in places) at (place) async {
    		val src = dupV().vec.d;
    		val dst = dupV().vec.d;
            dupV().allReduceTime -= Timer.milliTime(); 
    		team.allreduce(src, 0, dst, 0, M, op);
            dupV().allReduceTime += Timer.milliTime();
    	}
    }
    
    //Test method for the allreduce performance
    /*
    public def allReduce(op:Int, iterations:Long): void {
        finish for (place in places) at (place) async {
        
           for (i in 1..iterations){
               val src = dupV().vec.d;
               val dst = dupV().vec.d;
           
               dupV().allReduceTimeIt(dupV().allReduceTimeIndex) -= Timer.milliTime();           
               dupV().allReduceTime -= Timer.milliTime(); 
           
               team.allreduce(src, 0, dst, 0, M, op);
           
               dupV().allReduceTime += Timer.milliTime();
               dupV().allReduceTimeIt(dupV().allReduceTimeIndex++) += Timer.milliTime();
           }
        }
    }
    
    public def allReduceRewrite(op:Int, iterations:Long): void {
        finish for (place in places) at (place) async {
    
        for (i in 1..iterations){    
                dupV().vec.initRandom();
    
                val src = dupV().vec.d;
                val dst = dupV().vec.d;
    
                dupV().allReduceTimeIt(dupV().allReduceTimeIndex) -= Timer.milliTime();           
                dupV().allReduceTime -= Timer.milliTime(); 
    
                team.allreduce(src, 0, dst, 0, M, op);
    
                dupV().allReduceTime += Timer.milliTime();
                dupV().allReduceTimeIt(dupV().allReduceTimeIndex++) += Timer.milliTime();
            }
        } 
    } 
    */
    public def allReduceSum(): void {
        /* Timing */ val st = Timer.milliTime();        
        allReduce(Team.ADD);
        /* Timing */ commTime += Timer.milliTime() - st;
    }

    public def likeMe(that:DupVector): Boolean = (this.M==that.M);
        
    public def checkSync():Boolean {
        val rootvec  = dupV().vec;
        var retval:Boolean = true;
        for (var p:Long=0; p < places.size() && retval; p++) {
            if (p == here.id()) continue;
            retval &= at(places(p)) {
                    val vec = dupV().vec;
                    rootvec.equals(vec as Vector(rootvec.M))
            };
        }
        return retval;
    }
    
    public def equals(dv:DupVector(this.M)):Boolean {
        var ret:Boolean = true;
        if (dv.getPlaces().equals(places)){
            for (var p:Long=0; p<places.size() &&ret; p++) {
                ret &= at(places(p)) this.local().equals(dv.local());
            }
        }
        else
            ret = false;
        return ret;
    }
    
    public def equals(dval:ElemType):Boolean {
        var ret:Boolean = true;
        for (var p:Long=0; p<places.size() &&ret; p++) {
            ret &= at(places(p)) this.local().equals(dval);
        }
        return ret;
    }

    /**
     * Apply the map function <code>op</code> to each element of this vector,
     * overwriting the element of this vector with the result.
     * @param op a unary map function to apply to each element of this vector
     * @return this vector, containing the result of the map
     */
    public final @Inline def map(op:(x:Double)=>Double):DupVector(this) {
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = local();
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
    public final @Inline def map(a:DupVector(M), op:(x:Double)=>Double):DupVector(this) {
        assert(likeMe(a));
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = local();
            val ad = a.local() as Vector(d.M);
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
    public final @Inline def map(a:DupVector(M), b:DupVector(M), op:(x:Double,y:Double)=>Double):DupVector(this) {
        assert(likeMe(a));
        val stt = Timer.milliTime();
        finish ateach(Dist.makeUnique(places)) {
            val d = local();
            val ad = a.local() as Vector(d.M);
            val bd = b.local() as Vector(d.M);
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
    public final @Inline def reduce(op:(a:Double,b:Double)=>Double, unit:Double) 
        = local().reduce(op, unit);

    public def getCalcTime() = calcTime;
    public def getCommTime() = commTime;
    public def getPlaces() = places;

    public def toString() :String {
        val output = new StringBuilder();
        output.add("---Duplicated Vector:["+M+"], local copy---\n");
        output.add(dupV().toString());
        return output.toString();
    }

    public def allToString() {
        val output = new StringBuilder();
        output.add("-------- Duplicate vector :["+M+"] ---------\n");
        for (p in places) {
            output.add("Copy at place " + p.id() +"\n");
            output.add(at (p) { dupV().toString()});
        }
        return output.toString();
    }
    
    /**
     * Remake the DupVector over a new PlaceGroup
     */
    public def remake(newPg:PlaceGroup, newTeam:Team, addedPlaces:ArrayList[Place]){
        val oldPlaces = places;
        var spareUsed:Boolean = false;
        if (newPg.size() == oldPlaces.size() && addedPlaces.size() != 0){
            spareUsed = true;
        }
        if (!spareUsed){
            PlaceLocalHandle.destroy(oldPlaces, dupV, (Place)=>true);
            dupV = PlaceLocalHandle.make[DupVectorLocalState](newPg, ()=>new DupVectorLocalState(Vector.make(M), newPg.indexOf(here))  );
        }
        else{
            for (sparePlace in addedPlaces){
                Console.OUT.println("Adding place["+sparePlace+"] to DupVector PLH ...");
                PlaceLocalHandle.addPlace[DupVectorLocalState](dupV, sparePlace, ()=>new DupVectorLocalState(Vector.make(M), newPg.indexOf(here)));
            }
        }
        team = newTeam;
        places = newPg;
    }
    
    /*
     * Snapshot mechanism
     */
    private transient val DUMMY_KEY:Long = 8888L;

    /**
     * Create a snapshot for the DupVector by storing the current place's vector 
     * @return a snapshot for the DupVector data stored in a resilient store
     */
    public def makeSnapshot():DistObjectSnapshot {
        val snapshot = DistObjectSnapshot.make();
        val mode = System.getenv("X10_RESILIENT_STORE_MODE");
        if (mode == null || mode.equals("0")){
            val data = dupV().vec.d;
            val placeIndex = 0;
            snapshot.save(DUMMY_KEY, new VectorSnapshotInfo(placeIndex, data));
        } else {
            finish ateach(Dist.makeUnique(places)) {
                val data = dupV().vec.d;
                val placeIndex = dupV().placeIndex;
                snapshot.save(placeIndex, new VectorSnapshotInfo(placeIndex, data));
            }
        }
        return snapshot;
    }

    /**
     * Restore the DupVector data using the provided snapshot object 
     * @param snapshot a snapshot from which to restore the data
     */
    public def restoreSnapshot(snapshot:DistObjectSnapshot) {
        val mode = System.getenv("X10_RESILIENT_STORE_MODE");
        if (mode == null || mode.equals("0")){
            val dupSnapshotInfo:VectorSnapshotInfo = snapshot.load(DUMMY_KEY) as VectorSnapshotInfo;
            new Vector(dupSnapshotInfo.data).copyTo(dupV().vec);
            sync();
        } else {
            finish ateach(Dist.makeUnique(places)) {
                val segmentPlaceIndex = dupV().placeIndex;
                val storedVector = snapshot.load(segmentPlaceIndex) as VectorSnapshotInfo;
                val srcRail = storedVector.data;
                val dstRail = dupV().vec.d;
                Rail.copy(srcRail, 0, dstRail, 0, srcRail.size);
            }
        }
    }
    
    
    public def makeSnapshot_local(snapshot:DistObjectSnapshot):void {        
        val mode = System.getenv("X10_RESILIENT_STORE_MODE");
        if (mode == null || mode.equals("0")){
            if (here.id == 0){
                val data = dupV().vec.d;
                val placeIndex = 0;
                snapshot.save(DUMMY_KEY, new VectorSnapshotInfo(placeIndex, data));
            }
        } else {
            val data = dupV().vec.d;
            val placeIndex = dupV().placeIndex;
            snapshot.save(placeIndex, new VectorSnapshotInfo(placeIndex, data));            
        }
    }
    
    
    public def restoreSnapshot_local(snapshot:DistObjectSnapshot) {
        val mode = System.getenv("X10_RESILIENT_STORE_MODE");
        if (mode == null || mode.equals("0")){        
            if (here.id == 0){
                val dupSnapshotInfo:VectorSnapshotInfo = snapshot.load(DUMMY_KEY) as VectorSnapshotInfo;
                new Vector(dupSnapshotInfo.data).copyTo(dupV().vec);
            }
            sync_local(Place(0));
        } else {
            val segmentPlaceIndex = dupV().placeIndex;
            val storedVector = snapshot.load(segmentPlaceIndex) as VectorSnapshotInfo;
            val srcRail = storedVector.data;
            val dstRail = dupV().vec.d;
            Rail.copy(srcRail, 0, dstRail, 0, srcRail.size);
        }
    }
    
    public def printTimes(prefix:String, printIterations:Boolean){
        val root = here;
        finish ateach(Dist.makeUnique(places)) {
            val size = 6;
            val src = new Rail[Double](size);
            src(0) = dupV().multTime ;
            src(1) = dupV().multComptTime;
            src(2) = dupV().allReduceTime;
            src(3) = dupV().reduceTime;
            src(4) = dupV().bcastTime;
            src(5) = dupV().calcTime;            
            
            val dstMax = new Rail[Double](size);
            val dstMin = new Rail[Double](size);                        
            
            team.allreduce(src, 0, dstMax, 0, size, Team.MAX);            
            team.allreduce(src, 0, dstMin, 0, size, Team.MIN);            
            
            val maxIndexMultTime = team.indexOfMax(src(0), here.id as Int);
            val maxIndexMultComptTime = team.indexOfMax(src(1), here.id as Int);
            val maxIndexAllReduceTime = team.indexOfMax(src(2), here.id as Int);
            
            
            if (here.id == root.id){
                Console.OUT.println("["+prefix+"]  multTime:  indexOfMax("+maxIndexMultTime+")  max: " + dstMax(0) + " min: " + dstMin(0));
                Console.OUT.println("["+prefix+"]  multComptTime:   indexOfMax("+maxIndexMultComptTime+")  max: " + dstMax(1) + " min: " + dstMin(1));
                Console.OUT.println("["+prefix+"]  allReduceTime:   indexOfMax("+maxIndexAllReduceTime+")  max: " + dstMax(2) + " min:  " + dstMin(2));
             //   Console.OUT.println("["+prefix+"]  reduceTime: max: " + dstMax(3) + " min: " + dstMin(3));
            //    Console.OUT.println("["+prefix+"]  bcastTime: max: " + dstMax(4) + " min: " + dstMin(4));
            //    Console.OUT.println("["+prefix+"]  calcTime: max: " + dstMax(5) + " min: " + dstMin(5));
            }
            
            if (printIterations){
                val debugItSize = 1000;
                var descMultTime:String = prefix+"; multTime; p"+here.id+";";
                var descMultComptTime:String = prefix+"; multComptTime; p"+here.id+";";
                var descAllReduceTime:String = prefix+"; allReduceTime; p"+here.id+";";
            
                for (i in 0..(debugItSize-1)){
                    descMultTime += dupV().multTimeIt(i) + ";";
                    descMultComptTime += dupV().multComptTimeIt(i) + ";";
                    descAllReduceTime += dupV().allReduceTimeIt(i) + ";";
                }
            
                //var str:String = descMultTime + "\n" + descMultComptTime + "\n" + descAllReduceTime;
            
                Console.OUT.println(descMultTime);
                Console.OUT.println(descMultComptTime);
                Console.OUT.println(descAllReduceTime);
            }
        }    
    }
}

class DupVectorLocalState {
    public var vec:Vector;
    public val placeIndex:Long;
    
    
    
    public def this(v:Vector, placeIndex:Long){
        this.vec = v;
        this.placeIndex = placeIndex;
        
        val debugItSize = 1000;
        multTimeIt = new Rail[Long](debugItSize);    
        multComptTimeIt = new Rail[Long](debugItSize);
        allReduceTimeIt = new Rail[Long](debugItSize);
        reduceTimeIt = new Rail[Long](debugItSize);
        bcastTimeIt = new Rail[Long](debugItSize);
        calcTimeIt = new Rail[Long](debugItSize);
    }
    
    public def clone(){
        return new DupVectorLocalState(vec.clone(), placeIndex);
    }
    
    public def toString() = vec.toString();
    
    public var multTime:Long;    
    public var multComptTime:Long;
    public var allReduceTime:Long;
    public var reduceTime:Long;
    public var bcastTime:Long;
    public var calcTime:Long;
    
    public var multTimeIt:Rail[Long];
    public var multTimeIndex:Long;
    
    public var multComptTimeIt:Rail[Long];
    public var multComptTimeIndex:Long;
    
    public var allReduceTimeIt:Rail[Long];
    public var allReduceTimeIndex:Long;
    
    public var reduceTimeIt:Rail[Long];
    public var reduceTimeIndex:Long;
    
    public var bcastTimeIt:Rail[Long];
    public var bcastTimeIndex:Long;
    
    public var calcTimeIt:Rail[Long];
    public var calcTimeIndex:Long;
}
