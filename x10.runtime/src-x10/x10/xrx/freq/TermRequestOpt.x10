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

public class TermRequestOpt extends FinishRequest {
    public val finSrc:Int;
    public val finKind:Int;
    public val srcId:Int;
    public val dstId:Int;
    public val kind:Int;
    public ex:CheckedThrowable;
 
    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:Int,
            finSrc:Int, finKind:Int, srcId:Int, dstId:Int, kind:Int, ex:CheckedThrowable) {
        super(id, masterPlaceId, parentId);
        this.finSrc = finSrc;
        this.finKind = finKind;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.ex = ex;
    }
}