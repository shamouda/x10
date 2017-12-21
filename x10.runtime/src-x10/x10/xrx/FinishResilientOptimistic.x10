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

import x10.compiler.AsyncClosure;
import x10.compiler.Immediate;
import x10.compiler.Inline;

import x10.io.CustomSerialization;
import x10.io.Deserializer;
import x10.io.Serializer;
import x10.util.concurrent.AtomicInteger;
import x10.util.concurrent.SimpleLatch;
import x10.util.GrowableRail;
import x10.util.HashMap;
import x10.util.HashSet;

/**
 * Distributed Resilient Finish (records transit tasks only)
 * This version is a corrected implementation of the distributed finish described in PPoPP14,
 * that was released in version 2.4.1
 */
class FinishResilientOptimistic extends FinishResilient implements CustomSerialization {    
	// local finish object
	protected transient var me:FinishResilient; 

    //create root finish
    public def this (me:OptimisticFinishRootMaster) {
    	this.me = me;
    }
    
    //make root finish    
    static def make(parent:FinishState) {
        val me = new OptimisticFinishRootMaster(parent);
        val fs = new FinishResilientOptimistic(me);
        return fs;
    }
    
    //create remote finish
    private def this(deser:Deserializer) {
        val root = deser.readAny() as GlobalRef[OptimisticFinishRootMaster];
        val id = deser.readAny() as Id;
        me = new OptimisticFinishRemote(root, id);
    }
    
    //serialize root finish
    public def serialize(ser:Serializer) {
        if (me instanceof OptimisticFinishRootMaster)
            (me as OptimisticFinishRootMaster).serialize(ser);
        else assert false;
    }
    
    /* forward finish actions to the specialized implementation */
    def notifySubActivitySpawn(dstPlace:Place):void {
    	me.notifySubActivitySpawn(dstPlace);
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
    	me.notifyShiftedActivitySpawn(dstPlace);
    }

    def notifyRemoteContinuationCreated():void {
    	me.notifyRemoteContinuationCreated();
    }

    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
    	return me.notifyActivityCreation(srcPlace, activity);
    }

    def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
    	return me.notifyShiftedActivityCreation(srcPlace);
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
    	me.notifyActivityCreationFailed(srcPlace, t);
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
    	me.notifyActivityCreatedAndTerminated(srcPlace);
    }

    def notifyActivityTermination():void {
    	me.notifyActivityTermination();
    }

    def notifyShiftedActivityCompletion(srcPlace:Place):void {
    	me.notifyShiftedActivityCompletion(srcPlace);
    }

    def pushException(t:CheckedThrowable):void {
    	me.pushException(t);
    }

    def waitForFinish():void {
    	me.waitForFinish();
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}

final class OptimisticFinishRemote extends FinishResilient {
    //root finish reference
    private val root:GlobalRef[OptimisticFinishRootMaster];
    
    //root finish id
    private transient val id:Id;
    
    public def this (root:GlobalRef[OptimisticFinishRootMaster], id:Id) {
        this.id = id;
        this.root = root;
    }
        
    def notifySubActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyRemoteContinuationCreated():void {
        // no-op for remote finish
    }

    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
        return true;
    }

    def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
        return true;
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
        
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        
    }

    def notifyActivityTermination():void {
        
    }

    def notifyShiftedActivityCompletion(srcPlace:Place):void {
        notifyActivityTermination();
    }

    def pushException(t:CheckedThrowable):void {
        
    }

    def waitForFinish():void {
        assert false;
    }
}

final class OptimisticFinishRootMaster extends FinishResilient {
    //root finish id
    private val id:Id;
    
    //root finish reference
    private val root = GlobalRef[OptimisticFinishRootMaster](this);

    //parent of root finish
    private transient var parent:FinishState;
    
    //flag to indicate whether finish has been resiliently replicated (true) or not (false)
    private transient var isGlobal:Boolean = false;

    public def this (parent:FinishState) {
        this.parent = parent;
        this.id = Id(here.id as Int, nextId.getAndIncrement());
    }
    
    public def serialize(ser:Serializer) {
        if (!isGlobal)
            globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(root);
        ser.writeAny(id);
    }
    
    public def globalInit() {
        
    }
    
    def notifySubActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyRemoteContinuationCreated():void {
        
    }

    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
        return true;
    }

    def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
        return true;
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
        
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        
    }

    def notifyActivityTermination():void {
        
    }

    def notifyShiftedActivityCompletion(srcPlace:Place):void {
        notifyActivityTermination();
    }

    def pushException(t:CheckedThrowable):void {
        
    }

    def waitForFinish():void {
        
    }
}

final class OptimisticFinishRootBackup {
    
    def notifySubActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyShiftedActivitySpawn(dstPlace:Place):void {
        
    }

    def notifyRemoteContinuationCreated():void {
        
    }

    def notifyActivityCreation(srcPlace:Place, activity:Activity):Boolean {
        return true;
    }

    def notifyShiftedActivityCreation(srcPlace:Place):Boolean {
        return true;
    }

    def notifyActivityCreationFailed(srcPlace:Place, t:CheckedThrowable):void {
        
    }

    def notifyActivityCreatedAndTerminated(srcPlace:Place):void {
        
    }

    def notifyActivityTermination():void {
        
    }

    def notifyShiftedActivityCompletion(srcPlace:Place):void {
        notifyActivityTermination();
    }

    def pushException(t:CheckedThrowable):void {
        
    }
}
