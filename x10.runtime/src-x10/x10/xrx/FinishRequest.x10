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
package x10.xrx;

import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.concurrent.Lock;
import x10.util.concurrent.AtomicLong;
import x10.io.Deserializer;
import x10.io.Serializer;

public class FinishRequest implements x10.io.Unserializable {
    static val ADD_CHILD = 0n;
    static val TRANSIT = 1n;
    static val LIVE = 2n;
    static val TERM = 3n;
    static val EXCP = 4n;
    static val TERM_MUL = 5n; //multiple terminations in one message
    static val TRANSIT_TERM = 6n;
    
    //main identification fields
    val num:Long;
    var id:FinishResilient.Id;  //can be changed to adopter id
    var masterPlaceId:Int; //can be changed to adopter's master place
    var reqType:Int;
    var typeDesc:String;     
    var parentId:FinishResilient.Id;
    var toAdopter:Boolean; //redirect to adopter   
    
    var map:HashMap[FinishResilient.Task,Int];     //multiple termination map
    var childId:FinishResilient.Id;     //add child request
    var srcId:Int;     //transit/live/term request
    var dstId:Int;
    var kind:Int;
    var ex:CheckedThrowable;     //excp
    
    var finSrc:Int = -1n;     
    var finKind:Int = -1n;
    
    var bytes:Rail[Byte];
    
    private static val pool = new HashSet[FinishRequest]();
    private static val poolLock = new Lock();
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    private static val nextReqId = new AtomicLong(0);
    
    public def updateBytes() {
        val ser = new Serializer();
        ser.writeAny(num);
        ser.writeAny(id);
        ser.writeAny(masterPlaceId);
        ser.writeAny(reqType);
        ser.writeAny(typeDesc);
        ser.writeAny(parentId);
        ser.writeAny(toAdopter);
        ser.writeAny(childId);
        ser.writeAny(srcId);
        ser.writeAny(dstId);
        ser.writeAny(kind);
        ser.writeAny(ex);
        if (map == null) {
            ser.writeAny(-1);
        }
        else {
            ser.writeAny(map.size());
            ser.writeAny(map);
        }
        ser.writeAny(finSrc);
        ser.writeAny(finKind);
        bytes = ser.toRail();
    }
    
    static def make(bytes:Rail[Byte]) {
        val deser = new Deserializer(bytes);
        val num = deser.readAny() as Long;
        val id = deser.readAny() as FinishResilient.Id;
        val masterPlaceId = deser.readAny() as Int;
        val reqType = deser.readAny() as Int;
        val typeDesc = deser.readAny() as String;
        val parentId = deser.readAny() as FinishResilient.Id;
        val toAdopter = deser.readAny() as Boolean;
        val childId = deser.readAny() as FinishResilient.Id;
        val srcId = deser.readAny() as Int;
        val dstId = deser.readAny() as Int;
        val kind = deser.readAny() as Int;
        val tmpEx = deser.readAny();
        val ex:CheckedThrowable;
        if (tmpEx == null)
            ex = null;
        else
            ex = tmpEx as CheckedThrowable;
        val sz = deser.readAny() as Long;
        val map:HashMap[FinishResilient.Task,Int];
        if (sz == -1) {
            map = null;
        }
        else {
            map = deser.readAny() as HashMap[FinishResilient.Task,Int];
        }
        val finSrc = deser.readAny() as Int;
        val finKind = deser.readAny() as Int;
        return new FinishRequest(num, id, masterPlaceId, toAdopter, reqType, typeDesc, 
        		parentId, map, childId, srcId, dstId, kind, ex,
        		finSrc, finKind);       
    }
    
    private def this(num_:Long, id:FinishResilient.Id, masterPlaceId:Int, toAdopter:Boolean,
            reqType:Int, typeDesc:String, 
            parentId:FinishResilient.Id,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, 
            srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable,
            finSrc:Int, finKind:Int) {
        if (num_ == -1111)
            this.num = nextReqId.incrementAndGet();
        else
            this.num = num_;
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.toAdopter = toAdopter;   
        this.map = map;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
        this.finSrc = finSrc;
        this.finKind = finKind;
    }
    
    private static def allocReq(id:FinishResilient.Id, masterPlaceId:Int, toAdopter:Boolean,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable,
            finSrc:Int, finKind:Int) {
        try {
            poolLock.lock();
            if (pool.isEmpty()) {
                val req = new FinishRequest(-1111, id, masterPlaceId, toAdopter, reqType, typeDesc, parentId, map, childId, srcId, dstId, kind, ex, finSrc, finKind);
                req.updateBytes();
                return req;
            }
            else {
                val req = pool.iterator().next();
                req.init(id, masterPlaceId, toAdopter, reqType, typeDesc, parentId, map, childId, srcId, dstId, kind, ex, finSrc, finKind);
                req.updateBytes();
                pool.remove(req);
                return req;
            }
        } finally {
            poolLock.unlock();
        }
    }
    
    public static def deallocReq(req:FinishRequest) {
        try {
            poolLock.lock();
            pool.add(req);
        } finally {
            poolLock.unlock();
        }
    }
    
    private def init(id:FinishResilient.Id, masterPlaceId:Int, toAdopter:Boolean,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable,
            finSrc:Int, finKind:Int) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.toAdopter = toAdopter;   
        this.map = map;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
        this.finSrc = finSrc;
        this.finKind = finKind;
    }
    
    public def toString() {
        return "type=" + typeDesc + ",id="+id+",masterPlaceId="+masterPlaceId+",toAdopter="+toAdopter+",childId="+childId+",srcId="+srcId+",dstId="+dstId+",ex="+(ex == null? "null": ex.getMessage());
    }
    
    //pessimistic only
    public static def makeAddChildRequest(id:FinishResilient.Id, childId:FinishResilient.Id) {
        val masterPlaceId = id.home;
        return allocReq(id, masterPlaceId, false, ADD_CHILD, "ADD_CHILD", FinishResilient.UNASSIGNED, null, childId, -1n, -1n, -1n, null,
        		-1n, -1n);
    }
    
    public static def makeTransitRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int,
            finSrc:Int, finKind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, false, TRANSIT, "TRANSIT", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, true, TRANSIT, "TRANSIT", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);
        }
    }
    
    //pessimistic only
    public static def makeTransitTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int,kind:Int, ex:CheckedThrowable,
            finSrc:Int, finKind:Int) {
        if (adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = id.home;
            return allocReq(id, masterPlaceId, false, TRANSIT_TERM, "TRANSIT_TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, finSrc, finKind);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, true, TRANSIT_TERM, "TRANSIT_TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, finSrc, finKind);
        }
    }
    
    public static def makeLiveRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int,kind:Int,
            finSrc:Int, finKind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, false, LIVE, "LIVE", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, true, LIVE, "LIVE", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);
        }
    }
    
    public static def makeTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int,kind:Int,
            finSrc:Int, finKind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, false, TERM, "TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);            
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, true, TERM, "TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, finSrc, finKind);
        }
    }
    
    public static def makeExcpRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id
            ,ex:CheckedThrowable, finSrc:Int, finKind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, false, EXCP, "EXCP", parentId, null, FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, finSrc, finKind);
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, true, EXCP, "EXCP", parentId, null, FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, finSrc, finKind);
        }
    }
    
    //optimistic only
    public static def makeTermMulRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, dstId:Int,
            map:HashMap[FinishResilient.Task,Int], finSrc:Int, finKind:Int) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return allocReq(id, masterPlaceId, false, TERM_MUL, "TERM_MUL", parentId, map, FinishResilient.UNASSIGNED, -1n, dstId, -1n, null, finSrc, finKind);
    }
}