/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 *  (C) Copyright Sara Salem Hamouda 2014-2016.
 */

package x10.util.resilient.localstore.tx;

public class MapBucket(id:Long) implements x10.io.Unserializable {
	public val map = new HashMap[String,MemoryUnit]();
	public val locks = new HashMap[String,TxLock]();
	
	public def this(id:Long) {
		property(id);
		map = new HashMap[String,MemoryUnit]();
		locks = new HashMap[String,TxLock]();
	}
	
	public def this(id:Long, serMap:SerializableMapBucket) {
		property(id);
		map = serMap.map; 
		locks = new HashMap[String,TxLock]();
	}
	
	public def getSerializableBucket() {
		return new SerializableMapBucket(id, map);
	}
	
}