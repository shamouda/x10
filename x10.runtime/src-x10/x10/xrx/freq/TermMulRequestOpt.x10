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

public class TermMulRequestOpt extends FinishRequest {
    public val dstId:Int;
    public map:HashMap[FinishResilient.Task,Int];
    public ex:CheckedThrowable;
    public isTx:Boolean;
    public isTxRO:Boolean;

    public def this(id:FinishResilient.Id, masterPlaceId:Int,
            dstId:Int, map:HashMap[FinishResilient.Task,Int], ex:CheckedThrowable,
            isTx:Boolean, isTxRO:Boolean) {
        super(id, masterPlaceId, FinishResilient.UNASSIGNED);
        this.dstId = dstId;
        this.map = map;
        this.ex = ex;
        this.isTx = isTx;
        this.isTxRO = isTxRO;
    }
}