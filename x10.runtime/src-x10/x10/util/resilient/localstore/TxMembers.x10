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

public class TxMembers {
    public val virtual:Rail[Long];
    public val places:Rail[Long];

    public def this(v:Rail[Long], p:Rail[Long]) {
        virtual = v;
        places = p;
    }
    
    public def getPlace(indx:Long) {
        for (var i :Long = 0; i < virtual.size; i++) {
            if ( virtual(i) == indx)
                return places(i);
        }
        throw new Exception("illegal transaction configurations indx("+indx+") given("+toString()+")");
    }
    
    public def getVirtualPlaceId(p:Place) {
        for (var i :Long = 0; i < places.size; i++) {
            if ( places(i) == p.id)
                return virtual(i);
        }
        throw new Exception("illegal transaction configurations place("+p+") given("+toString()+")");
    }
    
    
    public def containsV(indx:Long) {
        for (var i :Long = 0; i < virtual.size; i++) {
            if ( virtual(i) == indx)
                return true;
        }
        return false;
    }
    
    public def containsP(placeId:Long) {
        for (var i :Long = 0; i < places.size; i++) {
            if ( places(i) == placeId)
                return true;
        }
        return false;
    }
    
    public def toString() {
        var str:String = "members=";
        for (var i :Long = 0; i < virtual.size; i++) {
            str += "("+virtual(i)+","+places(i)+")";
        }
        return str;
    }
}