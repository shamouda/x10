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
import x10.xrx.Tx;

public class TransitRequestOpt extends FinishRequest {
    public val srcId:Int;
    public val dstId:Int;
    public val kind:Int;
    
    public var transitSubmitDPE:Boolean;
    public val tx:Tx;

    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id, 
            srcId:Int, dstId:Int, kind:Int, tx:Tx) {
        super(id, masterPlaceId, parentId);
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.tx = tx;
    }
    public def setSubmitDPE(r:Boolean) {
        this.transitSubmitDPE = r;
    }

    public def gc():Boolean = true;
    
    public def getTx() = tx;
}