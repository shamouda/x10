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
 * Check that a float literal can be cast as float.
 */
public class FloatLit extends x10Test {
	public def run(): boolean = {
		var f: float = 0.001 as float;
		return true;
	}

	public static def main(var args: Array[String](1)): void = {
		new FloatLit().execute();
	}


}
