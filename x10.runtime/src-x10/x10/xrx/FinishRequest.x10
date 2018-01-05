/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */
package x10.xrx;

import x10.util.HashMap;

public class FinishRequest {
    static val ADD_CHILD = 0;
    static val TRANSIT = 1;
    static val LIVE = 2;
    static val TERM = 3;
    static val EXCP = 4;
    static val TERM_MUL = 5; //multiple terminations in one message
    
    val reqType:Long;
    val typeDesc:String;
    val id:FinishResilient.Id;
    var parentId:FinishResilient.Id;
    var map:HashMap[FinishResilient.Task,Int];
    
    //add child request
    var childId:FinishResilient.Id = FinishResilient.UNASSIGNED;
    
    //transit/live/term request
    var srcId:Int;
    var dstId:Int;
    var kind:Int;
    
    //excp
    var ex:CheckedThrowable;
    
    //redirect to adopter
    var toAdopter:Boolean = false;
    var adopterId:FinishResilient.Id;
    
    var backupPlaceId:Int = -1n;
    
    public def toString() {
        return "type=" + typeDesc + ",id="+id+",childId="+childId+",srcId="+srcId+",dstId="+dstId;
    }
    
    //ADD_CHILD
    public def this(reqType:Long, id:FinishResilient.Id,
        childId:FinishResilient.Id) {
        this.reqType = reqType;
        this.id = id;
        this.childId = childId;
        this.toAdopter = false;
        this.parentId = parentId;
        this.typeDesc = "ADD_CHILD";
    }
    
    //TRANSIT/TERM  -> parentId must be given to create backup if needed
    public def this(reqType:Long, id:FinishResilient.Id,
            parentId:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int) {
        this.reqType = reqType;
        this.id = id;
        this.toAdopter = false;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        this.parentId = parentId;
        if (reqType == TRANSIT)
            typeDesc = "TRANSIT";
        else if (reqType == LIVE)
            typeDesc = "LIVE" ;
        else if (reqType == TERM)
            typeDesc = "TERM" ; 
        else
            typeDesc = "" ;
    }
    
    //LIVE -> parent not needed, backup must have been already created.
    public def this(reqType:Long, id:FinishResilient.Id,
            srcId:Int, dstId:Int, kind:Int) {
        this.reqType = reqType;
        this.id = id;
        this.toAdopter = false;
        this.srcId = srcId;
        this.dstId = dstId;
        this.kind = kind;
        if (reqType == TRANSIT)
            typeDesc = "TRANSIT";
        else if (reqType == LIVE)
            typeDesc = "LIVE" ;
        else if (reqType == TERM)
            typeDesc = "TERM" ; 
        else
            typeDesc = "" ;
    }
    
    
    //EXCP  -> parentId must be given to create backup if needed
    public def this(reqType:Long, id:FinishResilient.Id,
            parentId:FinishResilient.Id,
            ex:CheckedThrowable) {
        this.reqType = reqType;
        this.id = id;
        this.toAdopter = false;
        this.parentId = parentId;
        this.ex = ex;
        typeDesc = "EXCP" ;
    }
    
    
    //TERM_MULT  -> parentId must be given to create backup if needed
    public def this(reqType:Long, id:FinishResilient.Id,
            parentId:FinishResilient.Id,
            dstId:Int,
            map:HashMap[FinishResilient.Task,Int]) {
        this.reqType = reqType;
        this.id = id;
        this.toAdopter = false;
        this.parentId = parentId;
        this.dstId = dstId;
        this.map = map;
        typeDesc = "TERM_MULT" ;
    }
    
    public def isLocal() = (id.home == here.id as Int);
    
}