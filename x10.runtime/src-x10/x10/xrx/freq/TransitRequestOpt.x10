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

public class TransitRequestOpt extends FinishRequest {
    public val srcId:Int;
    public val dstId:Int;
    public val kind:Int;
    
    public var transitSubmitDPE:Boolean;

    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id, srcId:Int, dstId:Int, kind:Int) {
        super(id, masterPlaceId, parentId);
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
    }
    public def setSubmitDPE(r:Boolean) {
        this.transitSubmitDPE = r;
    }
}