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

public class ExcpRequestOpt extends FinishRequest {
    public ex:CheckedThrowable;

    public def this(id:FinishResilient.Id, masterPlaceId:Int, parentId:FinishResilient.Id,
            ex:CheckedThrowable) {
        super(id, masterPlaceId, parentId);
        this.ex = ex;
    }
}