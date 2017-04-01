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

public class TxMembers(size:Long) {
    public val virtual:Rail[Long];
    public val places:Rail[Place];
    private var count:Long = 0;
    
    public def this(size:Long) {
        property(size);
        virtual = new Rail[Long](size);
        places = new Rail[Place](size);
    }
    
    public def this(size:Long, v:Rail[Long], p:Rail[Place]) {
        property(size);
        virtual = v;
        places = p;
    }
    
    public def getPlace(indx:Long) {
        for (var i :Long = 0; i < size; i++) {
            if ( virtual(i) == indx)
                return places(i);
        }
        throw new Exception("illegal transaction configurations ...");
    }
    
    public def contains(indx:Long) {
        for (var i :Long = 0; i < size; i++) {
            if ( virtual(i) == indx)
                return true;
        }
        return false;
    }
    
    public def contains(place:Place) {
        for (var i :Long = 0; i < size; i++) {
            if ( places(i).id == place.id)
                return true;
        }
        return false;
    }
    
    public def addPlace(indx:Long, place:Place) {
        virtual(count) = indx;
        places(count) = place;
        count++;
    }
    
    public def toString() {
        var str:String = "members=";
        for (var i :Long = 0; i < size; i++) {
            str += "("+virtual(i)+","+places(i)+")";
        }
        return str;
    }
    
    public def pg() = new SparsePlaceGroup(places);
}