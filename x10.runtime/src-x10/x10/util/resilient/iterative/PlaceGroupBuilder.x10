/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright Australian National University 2014.
 */

package x10.util.resilient.iterative;

/**
 * A utility class to test the use of arbitrary place groups in the GML library
 */
public class PlaceGroupBuilder {

    static val mode = getEnvLong("X10_PLACE_GROUP_RESTORE_MODE", RESTORE_MODE_SHRINK);
    
    static val RESTORE_MODE_SHRINK = 0;
    static val RESTORE_MODE_REPLACE_REDUNDANT = 1;
    
    static def getEnvLong(name:String, defValue:Long) {
        val env = System.getenv(name);
        val v = (env!=null) ? Long.parseLong(env) : defValue;
        return v;
    }

    public static def createRestorePlaceGroup(oldPlaceGroup:PlaceGroup):PlaceGroup {
        //RESTORE_MODE_SHRINK//
        if (mode == RESTORE_MODE_SHRINK) {
            return oldPlaceGroup.filterDeadPlaces();            
        }
        
        //RESTORE_MODE_REPLACE_REDUNDANT//
        var maxUsedPlaceId:Long = -1;
        for (p in oldPlaceGroup){
            if (p.id > maxUsedPlaceId){
                maxUsedPlaceId = p.id;
            }
        }
        
        if (maxUsedPlaceId == Place.numPlaces()-1) {
            Console.OUT.println("[PlaceGroupBuilder Log] WARNING: No spare places available, forcing SHRINK mode ...");
            return oldPlaceGroup.filterDeadPlaces();
        }
        
        var spareIndex:Long = maxUsedPlaceId;
        val newPlaces = new x10.util.ArrayList[Place]();        
        var deadCount:Long = 0;
        var allocated:Long = 0;
        for (p in oldPlaceGroup){
            if (p.isDead()){
                deadCount++;
                if (spareIndex < Place.numPlaces()){
                    newPlaces.add(Place.places()(spareIndex++));  
                    allocated++;
                }
            }
            else
                newPlaces.add(p);
        }
        Console.OUT.println("[PlaceGroupBuilder Log] "+ deadCount +" Dead Place(s) Replaced by "+allocated+" Spare Places"); 
        return new SparsePlaceGroup(newPlaces.toRail());        
    }

    public static def execludeSparePlaces(sparePlaces:Long):PlaceGroup {
        val livePlaces = new x10.util.ArrayList[Place]();
        val allPlaces = Place.places();
        val inPlacesCount = allPlaces.size() - sparePlaces;
        for (var i:Long = 0; i < inPlacesCount; i++){
            livePlaces.add(allPlaces(i));
        }
        var placeGroup:SparsePlaceGroup = new SparsePlaceGroup(livePlaces.toRail());
        return placeGroup;
    }
    
    public static def execludePlace(placeId:Long):PlaceGroup {
        val livePlaces = new x10.util.ArrayList[Place]();
        val allPlaces = Place.places();
        for (var i:Long = 0; i < Place.numPlaces(); i++){
            if (i != placeId)
                livePlaces.add(Place(i));
        }
        var placeGroup:SparsePlaceGroup = new SparsePlaceGroup(livePlaces.toRail());
        return placeGroup;
    }
}