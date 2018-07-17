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

import x10.util.Set;

/**
 * Runtime representation of an async. Only to be used in the runtime implementation.
 */
public class Activity {

    // This flag is hacked to be false in the APGAS C++ library
    // TODO: refactor XRX so body is more than a ()=>void so that run
    //       can simply ask the body if it should be deallocated on completion.
    private static DEALLOC_BODY:Boolean = canDealloc();
    private static def canDealloc():Boolean = true;  // sigh. Block constant propagation.

    /**
     * The finish state that should be used as the parent
     * of the internal finish created when this activity
     * synchronously place shifts to another place via an 'at'.
     *
     * @see Runtime.runAt
     */
    private var atFinishState:FinishState;

    /**
     * the finish state primarily governing the execution of this activity (may be remote)
     */
    private var finishState:FinishState;

    /**
     * The user-specified code for this activity.
     */
    private val body:()=>void;

    /**
     * The mapping from registered clocks to phases for this activity.
     * Lazily created.
     */
    var clockPhases:Clock.ClockPhases;

    /**
     * The epoch to which this activity belongs (Cancellation support)
     */
    public var epoch:Long;

    /**
     * Depth of enclosing atomic blocks
     */
    private var atomicDepth:Int = 0n;
    
    /**
     * The place that spawned the activity
     * */
    public val srcPlace:Place;

    public var tx:Boolean = false;
    public var txReadOnly:Boolean = true;
    
    //sub transaction
    public var subMembers:Set[Int] = null;
    public var subReadOnly:Boolean = true;
    
    /**
     * Create activity.
     */
    def this(epoch:Long, body:()=>void, finishState:FinishState, srcPlace:Place) {
        this.epoch = epoch;
        this.body = body;
        this.atFinishState = FinishState.UNCOUNTED_FINISH;
        this.finishState = finishState;
        this.srcPlace = srcPlace;
    }

    /**
     * Create clocked activity.
     */
    def this(epoch:Long, body:()=>void, finishState:FinishState, clockPhases:Clock.ClockPhases, 
             srcPlace:Place) {
        this(epoch, body, finishState, srcPlace);
        this.clockPhases = clockPhases;
    }

    /**
     * Return the clock phases
     */
    def clockPhases():Clock.ClockPhases {
        if (null == clockPhases) {
            clockPhases = Clock.ClockPhases.make();
        }
        return clockPhases;
    }

    /**
     * Return the innermost finish state that governs my spawned asyncs
     */
    public def finishState():FinishState = finishState;

    /**
     * Return the finish state that should be used internally for at 
     */
    def atFinishState():FinishState = atFinishState;

    /**
     * set the finish state that governs place shifting
     * @see Runtime.runAt
     */
    def setAtFinish(f:FinishState):void {
        atFinishState = f;
    }

    /**
     * Swap the finish state that governs my spawned asyncs
     * and my dynamic execution.
     * Typical usage: entering/exiting a finish block.
     */
    def swapFinish(f:FinishState) {
        val old = finishState;
        finishState = f;
        return old;
    }

    // about atomic blocks

    def pushAtomic() {
        atomicDepth++;
    }

    def popAtomic() {
        atomicDepth--;
    }

    def ensureNotInAtomic() {
        if (atomicDepth > 0)
            throw new IllegalOperationException();
    }

    /**
     * Run activity.
     */
    def run():void {
        var ex:CheckedThrowable = null;
        try {
            body();
        } catch (wt:WrappedThrowable) {
            ex = wt.getCheckedCause();
        } catch (t:CheckedThrowable) {
            ex = t;
        }
        if (null != clockPhases) clockPhases.drop();
        try {
            if (tx) {
                finishState.notifyTxActivityTermination(srcPlace, txReadOnly, ex);
            } else {
                finishState.notifyActivityTermination(srcPlace, ex);
            }
        } catch (DeadPlaceException) {}
        if (DEALLOC_BODY) Unsafe.dealloc(body);
    }
    
    public def setSubTransaction(subMembers:Set[Int], subReadOnly:Boolean) {
        if (subMembers != null) {
            this.subMembers = subMembers;
            this.subReadOnly = subReadOnly;
        }
    }
}

// vim:shiftwidth=4:tabstop=4:expandtab
