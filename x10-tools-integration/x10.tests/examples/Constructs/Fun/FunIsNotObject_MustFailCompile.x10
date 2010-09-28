/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2010.
 */

import harness.x10Test;

/**
 * Test that a function type is not an object.
 * @author kemal, 12/2004
 */
public class FunIsNotObject_MustFailCompile extends x10Test {

	public static N: int = 100;
	var nActivities: int = 0;

	public def run() {
		val f = (x1:int, x2:int)=> x1+x2;
		val x:Object = f;
	}
	public static def main(Array[String](1)) {
		new FunIsNotObject_MustFailCompile().execute();
	}
}
