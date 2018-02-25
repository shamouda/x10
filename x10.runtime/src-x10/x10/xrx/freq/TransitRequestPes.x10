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

import x10.xrx.FinishResilient;

public class TransitRequestPes extends FinishRequest {
    public val srcId:Int;
    public val dstId:Int;
    public val kind:Int;
    public var toAdopter:Boolean;
    
    public var transitSubmitDPE:Boolean;

    public var outAdopterId:FinishResilient.Id = FinishResilient.UNASSIGNED;

    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int, toAdopter:Boolean) {
        super(id, masterPlaceId, parentId);
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.toAdopter = toAdopter;
    }
    
    
    public def setOutAdopterId(adopterId:FinishResilient.Id) {
        this.outAdopterId = adopterId;
    }
    
    public def getOutAdopterId() = outAdopterId; 
    
    public def setToAdopter(b:Boolean) {
        toAdopter = b;
    }
    
    public def isToAdopter() = toAdopter;
    
    public def setSubmitDPE(r:Boolean) {
        this.transitSubmitDPE = r;
    }
    
}