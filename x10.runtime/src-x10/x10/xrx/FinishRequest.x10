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

public class FinishRequest {
    static val ADD_CHILD = 0;
    static val TRANSIT = 1;
    static val LIVE = 2;
    static val TERM = 3;
    static val EXCP = 4;
    static val TERM_MUL = 5; //multiple terminations in one message
    static val TRANSIT_TERM = 6;
    
    //main identification fields
    var id:FinishResilient.Id;  //can be changed to adopter id
    var masterPlaceId:Int; //can be changed to adopter's master place
    val reqType:Long;
    val typeDesc:String;     
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
    
    public def toString() {
        return "type=" + typeDesc + ",id="+id+",childId="+childId+",srcId="+srcId+",dstId="+dstId;
    }
    
    private def this(id:FinishResilient.Id, masterPlaceId:Int,
            reqType:Long, typeDesc:String, parentId:FinishResilient.Id,
            map:HashMap[FinishResilient.Task,Int],
            childId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.reqType = reqType;
        this.typeDesc = typeDesc;     
        this.parentId = parentId;
        this.toAdopter = false;   
        this.map = map;
        this.childId = childId;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }

    public static def makeAddChildRequest(id:FinishResilient.Id,
        childId:FinishResilient.Id) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, ADD_CHILD, "ADD_CHILD", FinishResilient.UNASSIGNED, null, childId, -1n, -1n, -1n, null);
    }
    
    public static def makeTransitRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, TRANSIT, "TRANSIT", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null);
    }
    
    public static def makeTransitTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,srcId:Int, dstId:Int,kind:Int, ex:CheckedThrowable) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, TRANSIT_TERM, "TRANSIT_TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, ex);
    }
    
    public static def makeLiveRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, LIVE, "LIVE", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null);
    }
    
    public static def makeTermRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,srcId:Int, dstId:Int,kind:Int) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, TERM, "TERM", parentId, null, FinishResilient.UNASSIGNED, srcId, dstId, kind, null);
    }
    
    public static def makeExcpRequest(id:FinishResilient.Id,parentId:FinishResilient.Id,ex:CheckedThrowable) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, EXCP, "EXCP", parentId, null, FinishResilient.UNASSIGNED, -1n, -1n, -1n, ex);
    }
    
    public static def makeTermMulRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, dstId:Int,
            map:HashMap[FinishResilient.Task,Int]) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new FinishRequest(id, masterPlaceId, TERM_MUL, "TERM_MUL", parentId, map, FinishResilient.UNASSIGNED, -1n, dstId, -1n, null);
    }
   
    public def isMasterLocal() = masterPlaceId == here.id as Int;
    public def isBackupLocal() = backupPlaceId == here.id as Int;
    
    
}