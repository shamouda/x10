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
package x10.xrx.freq;

import x10.util.HashMap;
import x10.xrx.FinishResilient;
import x10.xrx.FinishReplicator;
import x10.util.concurrent.AtomicLong;
import x10.compiler.Inline;

public abstract class FinishRequest {
    private static val nextReqId = new AtomicLong(0);
    public val num = nextReqId.incrementAndGet();
    public var isLocal:Boolean = false;
    
    //main identification fields
    public var id:FinishResilient.Id = FinishResilient.UNASSIGNED;  //can be changed to adopter id
    public var masterPlaceId:Int = -1n;
    public var backupPlaceId:Int = -1n;
    public var parentId:FinishResilient.Id = FinishResilient.UNASSIGNED;

    //output variables for non-blocking replication
    private var outSubmit:Boolean = false; 

    public def setSubmitDPE(r:Boolean) { }
    
    public def setOutAdopterId(adopterId:FinishResilient.Id) {
    }
    
    public def setOutSubmit(submit:Boolean) {
        this.outSubmit = submit;
    }
    
    public def setToAdopter(b:Boolean) {
        
    }
    
    public def isToAdopter():Boolean = false;
    public def getFinSrc():Int = -1n;
    public def getFinKind():Int = -1n;
    public def getOutAdopterId():FinishResilient.Id = FinishResilient.UNASSIGNED;
    public def getOutSubmit():Boolean = outSubmit;
    
    public @Inline def isTransitRequest() {
        return this instanceof TransitRequestPes || this instanceof TransitRequestOpt; 
    }
    
    public @Inline def isAddChildRequest() {
        return this instanceof AddChildRequestPes; 
    }
    
    public @Inline def createOK() {
        return this instanceof AddChildRequestPes || 
                /*in some cases, the parent may be transiting at the same time as its child, 
                  the child may not find the parent's backup during globalInit, so it needs to create it*/
                this instanceof TransitRequestPes ||
                this instanceof TransitRequestOpt ||
                this instanceof ExcpRequestPes ||
                this instanceof ExcpRequestOpt ||
                (this instanceof TermRequestPes && id.home == here.id as Int) ||
                (this instanceof TermRequestOpt && id.home == here.id as Int);
    }
    
    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id) {
        this.id = id;
        this.masterPlaceId = masterPlaceId;
        this.parentId = parentId;
    }
    
    public static def makePesAddChildRequest(id:FinishResilient.Id, childId:FinishResilient.Id) {
        return new AddChildRequestPes(id, id.home, childId);
    }
    
    public static def makeOptTransitRequest(id:FinishResilient.Id, parentId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new TransitRequestOpt(id, masterPlaceId, parentId, srcId, dstId, kind);    
    }
    
    public static def makePesTransitRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int) {
        val reqId = (adopterId == FinishResilient.UNASSIGNED) ? id : adopterId;
        val toAdopter = (adopterId != FinishResilient.UNASSIGNED);
        return new TransitRequestPes(reqId, reqId.home, parentId, srcId, dstId, kind, toAdopter);
    }
    
    public static def makePesTransitTermRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable) {
        val reqId = (adopterId == FinishResilient.UNASSIGNED) ? id : adopterId;
        val toAdopter = (adopterId != FinishResilient.UNASSIGNED);
        return new TransitTermRequestPes(reqId, reqId.home, parentId, srcId, dstId, kind, ex, toAdopter);
    }
    
    public static def makePesLiveRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int) {
        val reqId = (adopterId == FinishResilient.UNASSIGNED) ? id : adopterId;
        val toAdopter = (adopterId != FinishResilient.UNASSIGNED);
        return new LiveRequestPes(reqId, reqId.home, parentId, srcId, dstId, kind, toAdopter);
    }
    
    public static def makeOptExcpRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, 
            ex:CheckedThrowable) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home) ;
        return new ExcpRequestOpt(id, masterPlaceId, parentId, ex);
    }
    
    public static def makePesExcpRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, adopterId:FinishResilient.Id,
            ex:CheckedThrowable) {
        val reqId = (adopterId == FinishResilient.UNASSIGNED) ? id : adopterId;
        val toAdopter = (adopterId != FinishResilient.UNASSIGNED);
        return new ExcpRequestPes(reqId, reqId.home, parentId, ex, toAdopter);
    }
    
    public static def makeOptTermRequest(id:FinishResilient.Id, parentId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new TermRequestOpt(id, masterPlaceId, parentId, srcId, dstId, kind, ex);    
    }
    
    public static def makeOptRemoveGhostChildRequest(id:FinishResilient.Id, childId:FinishResilient.Id) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new RemoveGhostChildRequestOpt(id, masterPlaceId, childId);
    }
    
    public static def makePesTermRequest(id:FinishResilient.Id, parentId:FinishResilient.Id, adopterId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable) {
        val reqId = (adopterId == FinishResilient.UNASSIGNED) ? id : adopterId;
        val toAdopter = (adopterId != FinishResilient.UNASSIGNED);
        return new TermRequestPes(reqId, reqId.home, parentId, srcId, dstId, kind, ex ,toAdopter);
    }
    
    public static def makeOptTermMulRequest(id:FinishResilient.Id, dstId:Int, map:HashMap[FinishResilient.Task,Int]) {
        val masterPlaceId = FinishReplicator.getMasterPlace(id.home);
        return new TermMulRequestOpt(id, masterPlaceId, dstId, map);    
    }
}