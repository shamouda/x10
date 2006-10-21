/*
 * Created on Oct 25, 2005
 */
package com.ibm.domo.ast.x10.ssa;

import com.ibm.wala.classLoader.CallSiteReference;
import com.ibm/.wala.shrikeBT.IInvokeInstruction;
import com.ibm.wala.types.MethodReference;

public class AsyncCallSiteReference extends CallSiteReference {
    public static enum Dispath implements IInvokeInstruction.IDispatch {
      ASYNC_CALL;
    }

    public AsyncCallSiteReference(MethodReference ref, int pc) {
      super(pc, ref);
    }

    public IInvokeInstruction.IDispatch getInvocationCode() {
      return Dispatch.ASYNC_CALL;
    }

    public boolean isStatic() {
        return true;
    }

    public String getInvocationString() {
        return "async";
    }

    public String toString() {
      return "Async@" + getProgramCounter();
    }
}
