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
    private static val VERBOSE = (System.getenv("DEBUG_RESILIENT_EXECUTOR") != null 
                          && System.getenv("DEBUG_RESILIENT_EXECUTOR").equals("1"));
    
    static val RESTORE_MODE_SHRINK = 0;
    static val RESTORE_MODE_REPLACE_REDUNDANT = 1;
    
    static def getEnvLong(name:String, defValue:Long) {
        val env = System.getenv(name);
        val v = (env!=null) ? Long.parseLong(env) : defValue;
        return v;
    }

    public static def createRestorePlaceGroup(oldPlaceGroup:PlaceGroup):PlaceGroup {
        if (VERBOSE){
            var str:String = "";
            for (p in oldPlaceGroup){
                str += p.id + ",";
            }
            Console.OUT.println(">>> createRestorePlaceGroup: oldPlaceGroup is ["+str+"] ...");
        }
        //RESTORE_MODE_SHRINK//
        if (mode == RESTORE_MODE_SHRINK) {
            if (VERBOSE) Console.OUT.println("Shrinking oldPlaceGroup ...");
            return oldPlaceGroup.filterDeadPlaces();            
        }
        
        
        
        //RESTORE_MODE_REPLACE_REDUNDANT//
        var maxUsedPlaceId:Long = -1;
        for (p in oldPlaceGroup){
            if (p.id > maxUsedPlaceId){
                maxUsedPlaceId = p.id;
            }
        }
        
        if (VERBOSE) Console.OUT.println("Max used place id is: ["+maxUsedPlaceId+"] ...");
        
        if (maxUsedPlaceId == Place.numPlaces()-1) {
            Console.OUT.println("[PlaceGroupBuilder Log] WARNING: No spare places available, forcing SHRINK mode ...");
            return oldPlaceGroup.filterDeadPlaces();
        }
        
        var spareIndex:Long = maxUsedPlaceId+1;
        val newPlaces = new x10.util.ArrayList[Place]();        
        var deadCount:Long = 0;
        var allocated:Long = 0;
        for (p in oldPlaceGroup){
            if (p.isDead()){
                deadCount++;
                if (spareIndex < Place.numPlaces()){
                    if (VERBOSE) Console.OUT.println("adding place at spareIndex["+spareIndex+"] because p["+p.id+"] is dead ");
                    newPlaces.add(Place(spareIndex++));  
                    allocated++;
                }
            }
            else{
                if (VERBOSE) Console.OUT.println("adding place p["+p.id+"]");
                newPlaces.add(p);
            }
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
    
    //returns the places in pgNew that replaced dead places in pgOld
    //assumes live places have not changed their order in the place group
    public static def getReplacedPlaces(pgOld:PlaceGroup, pgNew:PlaceGroup):PlaceGroup {
        if (pgOld.size() != pgNew.size())
            throw new Exception("getReplacedPlaces requires the input groups to have same size: pgOld["+pgOld.size()+"] pgNew["+pgNew.size()+"]");
    
        val result = new x10.util.ArrayList[Place]();
        for (i in 0..(pgNew.size()-1)){
            if (pgOld(i).id != pgNew(i).id)
                result.add(pgNew(i));
        }        
        var placeGroup:SparsePlaceGroup = new SparsePlaceGroup(result.toRail());
        return placeGroup;
    }
}