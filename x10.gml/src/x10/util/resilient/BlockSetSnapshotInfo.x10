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
import x10.matrix.distblock.BlockSet;
import x10.matrix.ElemType;
import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.LocalStore;

public class BlockSetSnapshotInfo(placeIndex:Long, blockSet:BlockSet, isSpare:Boolean) implements Cloneable {
    
    public def clone():Cloneable {
        return new BlockSetSnapshotInfo(placeIndex, blockSet.clone());
    }
    
    public final def asyncRemoteCopy(id:Long, mapName:String, key:String, plh:PlaceLocalHandle[LocalStore]) {
        val idx = placeIndex;
        val sparse = isSparse;
        val blkCnt = blockSet.blocklist.size();
        val metadata = blockSet.getBlocksMetaData();
        val totalSize = blockSet.getStorageSize();
        
        if (sparse) {
            val index = new Rail[Long](totalSize);
            val value = new Rail[ElemType](totalSize);

            blockSet.initSparseBlocksRemoteCopyAtSource();
            blockSet.flattenIndex(index);
            blockSet.flattenValue(value);
            blockSet.finalizeSparseBlocksRemoteCopyAtSource();
            
            val srcbuf_value = new GlobalRail[ElemType](value);
            val srcbuf_index = new GlobalRail[Long](index);
            val srcbufCnt_index = index.size;
            
            at (plh().slave) async {
                val dstbuf_value = Unsafe.allocRailUninitialized[ElemType](srcbuf_value.size);
                val dstbuf_index = Unsafe.allocRailUninitialized[Long](srcbufCnt_index);
                Rail.asyncCopy[ElemType](srcbuf_value, 0, dstbuf_value, 0, srcbuf_value.size);
                Rail.asyncCopy[Long](srcbuf_index, 0, dstbuf_index, 0, srcbufCnt_index);
                
                val blockSet = BlockSet.makeSparseBlockSet(blkCnt, metadata, dstbuf_index, dstbuf_value);
                val remoteValue = new BlockSetSnapshotInfo(idx, blockSet, sparse);            
                plh().slaveStore.addEntry(id, mapName, key, remoteValue);
            }
        } 
        else {
            val value = new Rail[ElemType](totalSize);
            blockSet.flattenValue(value);
            val srcbuf_value = new GlobalRail[ElemType](value);
            val srcbufCnt_index = index.size;
            at (plh().slave) async {
                val dstbuf_value = Unsafe.allocRailUninitialized[ElemType](srcbuf_value.size);
                Rail.asyncCopy[ElemType](srcbuf_value, 0, dstbuf_value, 0, srcbuf_value.size);
                
                val blockSet = BlockSet.makeDenseBlockSet(blkCnt, metadata, dstbuf_value);
                val remoteValue = new BlockSetSnapshotInfo(idx, blockSet, sparse);
                plh().slaveStore.addEntry(id, mapName, key, remoteValue);
            }
        }
    }
}
