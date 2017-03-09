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

package x10.util.resilient.localstore;
import x10.util.RailUtils;

public class LockingRequest {
	public val keys:Rail[KeyInfo];
	public val dest:Place;

	public def this(dest:Place, keys:Rail[KeyInfo]) {
		this.dest = dest;
		RailUtils.sort(keys);
		this.keys = keys;
	}
	
	public static struct KeyInfo(key:String, read:Boolean) implements Comparable[KeyInfo] {
		public def compareTo(that:KeyInfo) {
			return key.compareTo(that.key);
		}
	};
}

