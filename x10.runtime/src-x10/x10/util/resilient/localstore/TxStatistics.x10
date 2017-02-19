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
import x10.util.Pair;
import x10.util.ArrayList;

public class TxStatistics {
    public static def mean(values:ArrayList[Double]) {
        if (values.size() == 0)
            return 0.0;
        
        var sum:Double = 0;
        for (x in values)
            sum += x;
        return sum / values.size();
    }
    
    public static def stdev(values:ArrayList[Double], mean:Double) {
        if (values.size() == 0)
            return 0.0;
        
        var sum:Double = 0;
        for (x in values) {
            sum += Math.pow( x - mean , 2);
        }
        return Math.sqrt(sum / (values.size() -1) ); // divide by N-1 because this is just a sample, not the whole population
    }
    
    public static def boxPlot(values:ArrayList[Double]) {
        val v = values.toRail();
        RailUtils.sort(v);
        val size = values.size();
        val min = v(0);
        val max = v(size -1);
        
        val medianPair = medianRange (v , 0, size-1 );
        val q1Pair = medianRange (v , 0, medianPair.first-1 );
        val q3Pair = medianRange (v , medianPair.second+1, size-1 );
        
        val median = ( v(medianPair.first) + v(medianPair.second) ) / 2.0;
        val q1 = ( v(q1Pair.first) + v(q1Pair.second) ) / 2.0;
        val q3 = ( v(q3Pair.first) + v(q3Pair.second) ) / 2.0;
        
        return new BoxPlot(min, q1, median, q3, max);
    }
    
    public static def medianRange(rail:Rail[Double], start:Long, end:Long) {
        val size = end - start;
        var medianLoc:Long = 0;
        if (size % 2 == 0) {
            val loc = start + ( end - start ) / 2;
            return new Pair[Long,Long](loc, loc);
        }
        else {
            val loc1 = start + ( end - start ) / 2;
            val loc2 = loc1 + 1;
            return  new Pair[Long,Long](loc1, loc2);
        }
    }
}

class BoxPlot (min:Double, q1:Double, median:Double, q3:Double, max:Double) {
    public def toString() { 
        return min + ":" + q1 + ":" + median + ":" + q3 + ":" + max; 
    }
}