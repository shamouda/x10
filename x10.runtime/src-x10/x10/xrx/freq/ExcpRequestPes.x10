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

public class ExcpRequestPes extends FinishRequest {
    public ex:CheckedThrowable;
    public var toAdopter:Boolean;

    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id,
            ex:CheckedThrowable, toAdopter:Boolean) {
        super(id, masterPlaceId, parentId);
        this.ex = ex;
        this.toAdopter = toAdopter;
    }
    
    public var outAdopterId:FinishResilient.Id = FinishResilient.UNASSIGNED;
    public def setOutAdopterId(adopterId:FinishResilient.Id) {
        this.outAdopterId = adopterId;
    }
    public def getOutAdopterId() = outAdopterId; 
    
    public def setToAdopter(b:Boolean) {
        toAdopter = b;
    }
    public def isToAdopter() = toAdopter;

}