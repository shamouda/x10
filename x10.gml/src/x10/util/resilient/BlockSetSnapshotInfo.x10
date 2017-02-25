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

public class BlockSetSnapshotInfo(placeIndex:Long, blockSet:BlockSet, isSparse:Boolean) implements Cloneable {
    
    public def clone():Cloneable {
        return new BlockSetSnapshotInfo(placeIndex, blockSet.clone(), isSparse);
    }
}
