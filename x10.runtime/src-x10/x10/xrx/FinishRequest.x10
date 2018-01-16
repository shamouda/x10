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

public class FinishRequest {
    static val ADD_CHILD = 0n;
    static val TRANSIT = 1n;
    static val LIVE = 2n;
    static val TERM = 3n;
    static val EXCP = 4n;
    static val TERM_MUL = 5n; //multiple terminations in one message
    static val TRANSIT_TERM = 6n;
    
    private static val OPTIMISTIC = Configuration.resilient_mode() == Configuration.RESILIENT_MODE_DIST_OPTIMISTIC;
    
    //main identification fields
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
    
    //special backup parameters
    var backupPlaceId:Int = -1n;
    var transitSubmitDPE:Boolean = false;
    
    //optimistic finish source
    var optFinSrc:Int = -1n;
    var optFinKind:Int = -1n;
    
    private static val pool = new HashSet[FinishRequest]();
    
    private static def allocReq(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, optFinSrc:Int, optFinKind:Int,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        try {
            FinishResilient.glock.lock();
            if (pool.isEmpty())
                return new FinishRequest(id, masterPlaceId, reqType, typeDesc, parentId, optFinSrc, optFinKind, map, childId, srcId, dstId, kind, ex, toAdopter);
            else {
                val req = pool.iterator().next();
                req.init(id, masterPlaceId, reqType, typeDesc, parentId, optFinSrc, optFinKind, map, childId, srcId, dstId, kind, ex, toAdopter);
                pool.remove(req);
                return req;
            }
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    public static def deallocReq(req:FinishRequest) {
        try {
            FinishResilient.glock.lock();
            pool.add(req);
        } finally {
            FinishResilient.glock.unlock();
        }
    }
    
    public def toString() {
        return "type=" + typeDesc + ",id="+id+",toAdopter="+toAdopter+",childId="+childId+",srcId="+srcId+",dstId="+dstId+",ex="+(ex == null? "null": ex.getMessage());
    }
    
    private def this(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, optFinSrc:Int, optFinKind:Int,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.optFinSrc = optFinSrc;
        this.optFinKind = optFinKind;
        this.toAdopter = toAdopter;   
        this.map = map;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }
    
    private def init(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Int, typeDesc:String, parentId:FinishResilient.Id, optFinSrc:Int, optFinKind:Int,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable, toAdopter:Boolean) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.optFinSrc = optFinSrc;
        this.optFinKind = optFinKind;
        this.toAdopter = toAdopter;   
        this.map = map;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }

    public static def makeAddChildRequest(id:FinishResilient.Id,
        childId:FinishResilient.Id) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        return allocReq(id, masterPlaceId, ADD_CHILD, "ADD_CHILD", FinishResilient.UNASSIGNED, -1n, -1n, null, childId, -1n, -1n, -1n, null, false);
    }
    
    public static def makeTransitRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            optFinSrc:Int,optFinKind:Int,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            return allocReq(id, masterPlaceId, TRANSIT, "TRANSIT", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);    
        } else {
            return allocReq(adopterId, masterPlaceId, TRANSIT, "TRANSIT", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
    }
    
    //pessimistic only
    public static def makeTransitTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            optFinSrc:Int,optFinKind:Int,srcId:Int, dstId:Int,kind:Int, ex:CheckedThrowable) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        if (adopterId == FinishResilient.UNASSIGNED) {
            return allocReq(id, masterPlaceId, TRANSIT_TERM, "TRANSIT_TERM", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, false);    
        } else {
            return allocReq(adopterId, masterPlaceId, TRANSIT_TERM, "TRANSIT_TERM", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, ex, true);
        }
    }
    
    public static def makeLiveRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            optFinSrc:Int,optFinKind:Int,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            return allocReq(id, masterPlaceId, LIVE, "LIVE", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);    
        } else {
            return allocReq(adopterId, masterPlaceId, LIVE, "LIVE", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
        
    }
    
    public static def makeTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            optFinSrc:Int,optFinKind:Int,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            return allocReq(id, masterPlaceId, TERM, "TERM", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, false);            
        } else {
            return allocReq(adopterId, masterPlaceId, TERM, "TERM", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null, true);
        }
    }
    
    public static def makeExcpRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,adopterId:FinishResilient.Id,
            optFinSrc:Int,optFinKind:Int,ex:CheckedThrowable) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        if (OPTIMISTIC || adopterId == FinishResilient.UNASSIGNED) {
            return allocReq(id, masterPlaceId, EXCP, "EXCP", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, false);
        } else {
            return allocReq(adopterId, masterPlaceId, EXCP, "EXCP", parentId, optFinSrc, optFinKind, null, FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex, true);
        }
        
    }
    
    //optimistic only
    public static def makeTermMulRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, optFinSrc:Int, optFinKind:Int, dstId:Int,
            map:HashMap[FinishResilient.Task,Int]) {
        val masterPlaceId = OPTIMISTIC? FinishReplicator.getMasterPlace(id.home) : id.home;
        return allocReq(id, masterPlaceId, TERM_MUL, "TERM_MUL", parentId, optFinSrc, optFinKind, map, FinishResilient.UNASSIGNED, -1n, dstId, -1n, null, false);
    }
   
    public def isMasterLocal() = masterPlaceId == here.id as Int;
    public def isBackupLocal() = backupPlaceId == here.id as Int;
    
    
}