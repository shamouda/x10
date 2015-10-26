/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2014.
 *  (C) Copyright Sara Salem Hamouda 2014.
 */
package x10.util.resilient;

import x10.util.HashMap;
import x10.matrix.ElemType;
import x10.util.resilient.iterative.Snapshot;

public class VectorSnapshotInfo(placeIndex:Long, data:Rail[ElemType]{self!=null}) implements Snapshot {
    private static val DEBUG_DATA_SIZE:Boolean = (System.getenv("X10_GML_DEBUG_DATA_SIZE") != null 
 												&& System.getenv("X10_GML_DEBUG_DATA_SIZE").equals("1"));

    public def clone():Any {  
        return new VectorSnapshotInfo(placeIndex, new Rail[ElemType](data));
    }
    
    public final def remoteCopyAndSave(key:Any, hm:PlaceLocalHandle[HashMap[Any,Any]], backupPlace:Place) {
        val srcbuf = new GlobalRail[ElemType](data);
        val srcbufCnt = data.size;
        val idx = placeIndex;
        at(backupPlace) {
            val dstbuf = new Rail[ElemType](srcbufCnt);
            if (DEBUG_DATA_SIZE) Console.OUT.println("[VectorSnapshot] remoteCopyAndSave asyncCopySize:" + srcbufCnt);
            finish Rail.asyncCopy[ElemType](srcbuf, 0, dstbuf, 0, srcbufCnt);
            atomic hm().put(key, new VectorSnapshotInfo(idx, dstbuf));
        }
    }
    

    public final def remoteClone(targetPlace:Place):GlobalRef[Any]{self.home==targetPlace} {
        val srcbuf = new GlobalRail[ElemType](data);
        val srcbufCnt = data.size;
        val idx = placeIndex;
        val resultGR = at(targetPlace) {
            val dstbuf = new Rail[ElemType](srcbufCnt);
            if (DEBUG_DATA_SIZE) Console.OUT.println("[VectorSnapshot] remoteClone asyncCopySize:" + srcbufCnt);
            finish Rail.asyncCopy[ElemType](srcbuf, 0, dstbuf, 0, srcbufCnt);
            val gr = new GlobalRef[Any](new VectorSnapshotInfo(idx, dstbuf));
            gr
        };
        return resultGR;
    }
}

