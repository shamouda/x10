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
 * The test checks that property syntax is accepted.
 *
 * @author pvarma
 */
public class Prop1(i:int, j:int){i == j} extends x10Test {

	public def this(k:int):Prop1 = {
	    property(k,k);
	}
	public def run()=true;
	
	public static def main(a:Array[String](1)):void = {
		new Prop1(2).execute();
	}
}
