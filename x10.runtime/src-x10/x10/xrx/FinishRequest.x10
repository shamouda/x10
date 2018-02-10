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
import x10.io.CustomSerialization;
import x10.io.Deserializer;
import x10.io.Serializer;

public class FinishRequest {
    static val ADD_CHILD = 0n;
    static val TRANSIT = 1n;
    static val LIVE = 2n;
    static val TERM = 3n;
    static val EXCP = 4n;
    static val TERM_MUL = 5n; //multiple terminations in one message
    static val TRANSIT_TERM = 6n;
    
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    private static val nextReqId = new AtomicLong(0);
    
    private static val pool = new HashSet[FinishRequest]();
    private static val poolLock = new Lock();
    
    public val num:Long;
    
    //main identification fields
    var id:FinishResilient.Id;  //can be changed to adopter id
    var masterPlaceId:Int; //can be changed to adopter's master place
    var reqType:Int;
    var typeDesc:String;     
    var parentId:FinishResilient.Id;
    var toAdopter:Boolean; //redirect to adopter   
    
    var tasks:Rail[Int]; //multiple termination map
    var kinds:Rail[Int];
    var counts:Rail[Int];

    var childId:FinishResilient.Id;     //add child request
    var srcId:Int;     //transit/live/term request
    var dstId:Int;
    var kind:Int;
    var ex:CheckedThrowable;     //excp
    
    //optimistic finish source
    var finSrc:Int = -1n;
    var finKind:Int = -1n;
    
    //special backup parameters
    var backupPlaceId:Int = -1n;
    var transitSubmitDPE:Boolean = false;
    
    //output variables for non-blocking replication
    var outSubmit:Boolean; 
    var outAdopterId:FinishResilient.Id = FinishResilient.UNASSIGNED;
    
    private static def allocReq(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, finSrc:Int, finKind:Int,
            tasks:Rail[Int], kinds:Rail[Int], counts:Rail[Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        try {
            poolLock.lock();
            if (pool.isEmpty())
                return new FinishRequest(id, masterPlaceId, reqType, typeDesc, parentId, finSrc, finKind, tasks, kinds, counts, childId, srcId, dstId, kind, ex, toAdopter);
            else {
                val req = pool.iterator().next();
                req.init(id, masterPlaceId, reqType, typeDesc, parentId, finSrc, finKind, tasks, kinds, counts, childId, srcId, dstId, kind, ex, toAdopter);
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
            req.outSubmit = false;
            req.outAdopterId = FinishResilient.UNASSIGNED;
            pool.add(req);
        } finally {
            poolLock.unlock();
        }
    }
    
    public def toString() {
        return "type=" + typeDesc + ",id="+id+",outSubmit="+outSubmit+",toAdopter="+toAdopter+",childId="+childId+",srcId="+srcId+",dstId="+dstId+",ex="+(ex == null? "null": ex.getMessage());
    }
    
    private def this(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, finSrc:Int, finKind:Int,
            tasks:Rail[Int], kinds:Rail[Int], counts:Rail[Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
    	this.num = nextReqId.incrementAndGet();
    	this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.finSrc = finSrc;
        this.finKind = finKind;
        this.toAdopter = toAdopter;   
        this.tasks = tasks;
        this.kinds = kinds;
        this.counts = counts;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }
    
    public def this(num:Long, id_home:Int, id_id:Int, masterPlaceId:Int, reqType:Int, typeDesc:String,
    		parentId_home:Int, parentId_id:Int, toAdopter:Boolean, tasks:Rail[Int], kinds:Rail[Int], counts:Rail[Int], 
    		childId_home:Int, childId_id:Int,  srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, finSrc:Int, 
    		finKind:Int, backupPlaceId:Int, transitSubmitDPE:Boolean, 
    		outSubmit:Boolean, outAdopterId_home:Int, outAdopterId_id:Int) {
        this.num = num;
        this.id = FinishResilient.Id(id_home, id_id);
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = FinishResilient.Id(parentId_home, parentId_id);
        this.toAdopter = toAdopter;
        this.tasks = tasks;
        this.kinds = kinds;
        this.counts = counts;
        this.childId = FinishResilient.Id(childId_home, childId_id);
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
        this.finSrc = finSrc;
        this.finKind = finKind;
        this.backupPlaceId = backupPlaceId;
        this.transitSubmitDPE = transitSubmitDPE;
        this.outSubmit = outSubmit;
        this.outAdopterId = outAdopterId;
    }
    
    private def init(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, finSrc:Int, finKind:Int,
            tasks:Rail[Int], kinds:Rail[Int], counts:Rail[Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.finSrc = finSrc;
        this.finKind = finKind;
        this.toAdopter = toAdopter;   
        this.tasks = tasks;
        this.kinds = kinds;
        this.counts = counts;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }

    //pessimistic only
    public static def makeAddChildRequest(id:FinishResilient.Id,
        childId:FinishResilient.Id) {
        val masterPlaceId = id.home;
        return allocReq(id, masterPlaceId, ADD_CHILD, "ADD_CHILD", FinishResilient.UNASSIGNED, -1n, -1n, null, null, null, childId, -1n, -1n, -1n, null, false);
    }
    
    public static def makeTransitRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            finSrc:Int,finKind:Int,srcId:Int, dstId:Int,kind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, TRANSIT, "TRANSIT", parentId, finSrc, finKind, null,  null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, TRANSIT, "TRANSIT", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
    }
    
    //pessimistic only
    public static def makeTransitTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            finSrc:Int,finKind:Int,srcId:Int, dstId:Int,kind:Int, ex:CheckedThrowable) {
        if (adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = id.home;
            return allocReq(id, masterPlaceId, TRANSIT_TERM, "TRANSIT_TERM", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, false);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, TRANSIT_TERM, "TRANSIT_TERM", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, true);
        }
    }
    
    public static def makeLiveRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            finSrc:Int,finKind:Int,srcId:Int, dstId:Int,kind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, LIVE, "LIVE", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);    
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, LIVE, "LIVE", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
    }
    
    public static def makeTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            finSrc:Int,finKind:Int,srcId:Int, dstId:Int,kind:Int) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, TERM, "TERM", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);            
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, TERM, "TERM", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
    }
    
    public static def makeExcpRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            finSrc:Int,finKind:Int,ex:CheckedThrowable) {
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
            return allocReq(id, masterPlaceId, EXCP, "EXCP", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, false);
        } else {
            val masterPlaceId = adopterId.home;
            return allocReq(adopterId, masterPlaceId, EXCP, "EXCP", parentId, finSrc, finKind, null, null, null,  FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, true);
        }
        
    }
    
    //optimistic only
    public static def makeTermMulRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, finSrc:Int, finKind:Int, dstId:Int,
            tasks:Rail[Int], kinds:Rail[Int], counts:Rail[Int]) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return allocReq(id, masterPlaceId, TERM_MUL, "TERM_MUL", parentId, finSrc, finKind, tasks, kinds, counts, FinishResilient.UNASSIGNED, -1n, dstId, -1n, null, false);
    }
   
    public def isMasterLocal() = masterPlaceId == here.id as Int;
    public def isBackupLocal() = backupPlaceId == here.id as Int;
    
    
}