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
	protected transient var me:FinishState; 

	//root finish reference
	private transient val rootRef:GlobalRef[FinishResilientOptimistic] = GlobalRef[FinishResilientOptimistic](this);
    
    //flag to indicate whether finish has been resiliently replicated (true) or not (false)
    protected transient var isGlobal:Boolean = false;
    
    //root finish id
    protected val id:Id;

    //create root finish
    public def this (parent:FinishState) {
    	id = Id(here.id as Int, nextId.getAndIncrement());
    	me = new OptimisticFinishRootMaster(parent);
    }
    
    private def this(deser:Deserializer) {
        id = deser.readAny() as Id;
        me = new OptimisticFinishRemote();
    }
    
    public def serialize(ser:Serializer) {
        if (!isGlobal) (me as OptimisticFinishRootMaster).globalInit(); // Once we have more than 1 copy of the finish state, we must go global
        ser.writeAny(id);
    }

    static def make(parent:FinishState) {
        val fs = new FinishResilientOptimistic(parent);
        return fs;
    }
    
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

    def notifyShiftedActivityCompletion():void {
    	me.notifyShiftedActivityCompletion();
    }

    def pushException(t:CheckedThrowable):void {
    	me.pushException(t);
    }

    def waitForFinish():void {
    	me.waitForFinish();
    }
    
    final class OptimisticFinishRemote extends FinishResilientOptimistic {
        
    	public def this () {
    		isGlobal = true;
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
	
	    def notifyShiftedActivityCompletion():void {
	    	
	    }
	
	    def pushException(t:CheckedThrowable):void {
	    	
	    }
	
	    def waitForFinish():void {
	    	assert false;
	    }
    }
    
    final class OptimisticFinishRootMaster extends FinishResilientOptimistic {
    	
        //parent of root finish
        private transient var parent:FinishState;
    
        public def this (parent:FinishState) {
        	this.parent = parent;
        	isGlobal = false;
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
	
	    def notifyShiftedActivityCompletion():void {
	    	
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
	
	    def notifyShiftedActivityCompletion():void {
	    	
	    }
	
	    def pushException(t:CheckedThrowable):void {
	    	
	    }
    }
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}