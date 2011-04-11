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
 * The test checks that property syntax is accepted for structs
 *
 * @author vj
 */
public class PropsMustBeVisibleToOtherPropsInValue extends x10Test {

    static struct Value2(i:int, j:int{self==i})  {
        public def this(k:int):Value2{self.i==k} = {
            property(k,k);
        }
    }
    public def run():boolean = {
        Value2(4);
        return true;
    }
    public static def main(var args: Array[String](1)): void = {
        new PropsMustBeVisibleToOtherPropsInValue().execute();
    }
}
