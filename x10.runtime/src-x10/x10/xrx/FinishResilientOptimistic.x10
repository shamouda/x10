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
abstract class FinishResilientOptimistic extends FinishResilient implements CustomSerialization {
    private static val verbose = FinishResilient.verbose;
    
    private def this() {
    	
    }
    
    private def this(p:FinishState) { 
    	
    }
    
	private def this(ds:Deserializer) {
		
	}
	
    public def serialize(s:Serializer) {
    	
    }
    
    static def make(parent:FinishState) {
        //val fs = new FinishResilientOptimistic(parent);
        return null as FinishState;
    }
    
    final class OptimisticFinishRemote extends FinishResilientOptimistic {
        
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
    
    final class OptimisticFinishRootBackup extends FinishResilientOptimistic {
    	
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
    
    static def notifyPlaceDeath():void {
        if (verbose>=1) debug(">>>> notifyPlaceDeath called");
        if (verbose>=1) debug("<<<< notifyPlaceDeath returning");
    }
}