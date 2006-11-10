package polyglot.ext.x10.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import polyglot.ast.Expr;
import polyglot.ast.Formal;
import polyglot.ast.Node;
import polyglot.ast.Receiver;
import polyglot.ast.TypeNode;
import polyglot.ext.jl.ast.Call_c;
import polyglot.ext.x10.types.X10Context;
import polyglot.ext.x10.types.X10Flags;
import polyglot.ext.x10.types.X10MethodInstance;
import polyglot.ext.x10.types.X10Type;
import polyglot.ext.x10.types.X10TypeSystem;
import polyglot.ext.x10.types.constr.C_Local_c;
import polyglot.ext.x10.types.constr.C_Root;
import polyglot.ext.x10.types.constr.C_Special;
import polyglot.ext.x10.types.constr.C_Var;
import polyglot.ext.x10.types.constr.Constraint;
import polyglot.main.Report;
import polyglot.types.Flags;
import polyglot.types.MethodInstance;
import polyglot.types.NoMemberException;
import polyglot.types.SemanticException;
import polyglot.types.Type;
import polyglot.types.TypeSystem;
import polyglot.util.Position;
import polyglot.util.TypedList;
import polyglot.visit.TypeBuilder;
import polyglot.visit.TypeChecker;

/**
 * A method call wrapper to rewrite getLocation() calls on primitives
 * and array operator calls.
 * @author Igor
 */
public class X10Call_c extends Call_c {
    public X10Call_c(Position pos, Receiver target, String name,
                     List arguments) {
        super(pos, target, name, arguments);
    }

    /**
     * Rewrite getLocation() to Here for value types and operator calls for
     * array types, otherwise leave alone.
     */
    public Node typeCheck(TypeChecker tc) throws SemanticException {
    	
        X10NodeFactory xnf = (X10NodeFactory) tc.nodeFactory();
        X10TypeSystem xts = (X10TypeSystem) tc.typeSystem();
        if (this.target != null && this.target.type().isPrimitive() &&
                name().equals("getLocation") && arguments().isEmpty())
        {
            return xnf.Here(position()).typeCheck(tc);
        }

        try {
        	X10Call_c result = (X10Call_c) super.typeCheck(tc);
        	if (!  result.target().type().isCanonical()) {
        		return result;
        	}
        	result = adjustMI(result, tc);
        	checkAnnotations(result, tc);
        	return result;
        } catch (NoMemberException e) {
            if (e.getKind() != NoMemberException.METHOD || this.target == null)
                throw e;
            Type type = target.type();
            if (!xts.isX10Array(type))
                throw e;
            // Special methods on arrays
            String name = name();
            List arguments = arguments();
            Type elem = xts.baseType(type);
            //reduce(), scan(), restriction(), union(), overlay(), update(), and lift()
            if ((name.equals("reduce") && arguments.size() == 2 &&
                    xts.isSubtype(((Expr)arguments.get(0)).type(), xts.OperatorBinary()) &&
                    xts.isSubtype(((Expr)arguments.get(1)).type(), elem)) ||
                (name.equals("scan") && arguments.size() == 2 &&
                    xts.isSubtype(((Expr)arguments.get(0)).type(), xts.OperatorBinary()) &&
                    xts.isSubtype(((Expr)arguments.get(1)).type(), elem)) ||
                (name.equals("restriction") && arguments.size() == 1 &&
                    isRestrictionArgType(((Expr)arguments.get(0)).type(), xts)) ||
                (name.equals("union") && arguments.size() == 1 &&
                    xts.isSubtype(type, ((Expr)arguments.get(0)).type())) ||
                (name.equals("overlay") && arguments.size() == 1 &&
                    xts.isSubtype(type, ((Expr)arguments.get(0)).type())) ||
                (name.equals("update") && arguments.size() == 1 &&
                    xts.isSubtype(type, ((Expr)arguments.get(0)).type())) ||
                (name.equals("lift") &&
                    ((arguments.size() == 2 &&
                      xts.isSubtype(((Expr)arguments.get(0)).type(), xts.OperatorBinary()) &&
                      xts.isSubtype(type, ((Expr)arguments.get(1)).type())) ||
                     (arguments.size() == 1 &&
                      xts.isSubtype(((Expr)arguments.get(0)).type(), xts.OperatorUnary()))))
               )
            {
                TypeNode t = xnf.CanonicalTypeNode(position(), xts.ArrayOperations());
                List newargs = TypedList.copy(arguments, Expr.class, false);
                newargs.add(0, this.target);
                return ((X10Call_c)this.target(t).arguments(newargs)).superTypeCheck(tc);
            }
            throw e;
        }
    }

   
    private X10Call_c adjustMI(X10Call_c result, TypeChecker tc) throws SemanticException {
    	HashMap<C_Root, C_Var> subs = new HashMap<C_Root, C_Var>();
    	X10MethodInstance xmi = (X10MethodInstance) mi;
    	if (mi == null) return result;
    	X10Type type = (X10Type) mi.returnType();
    	if (! type.isCanonical())
    		return result;
		X10Type thisType = (X10Type) target.type();
		Constraint rc = type.realClause();
		if (rc != null ) {
			C_Var var=thisType.selfVar();
			if (var == null) var = rc.genEQV(thisType);
			subs.put(C_Special.This, var);
			
			List<Formal> f = xmi.formals();
			assert (f != null);
			if (f == null) {
				// This could happen for Java methods.
				Report.report(1, "X10Call_c: formals for " + mi + " are null.");
			} else {
				int idx = 0;
				for (Iterator<Expr> i = this.arguments.iterator(); i.hasNext(); ) {
					X10Type argType = (X10Type) i.next().type();
					C_Var v=argType.selfVar();
					if (v == null) v = rc.genEQV(argType);
					Formal formal = f.get(idx);
					C_Root orig = new C_Local_c(formal.localInstance());
					subs.put(orig, v);
					idx++;
				}
			}
			Constraint newRC = rc.substitute(subs);
			X10Type retType = type.makeVariant(newRC, null);
			mi = mi.returnType(retType);
//			TODO vj:  Is this really necessary?
			result = (X10Call_c)this.methodInstance(mi).type(mi.returnType());
		}
	    return result;

    }
    private void checkAnnotations(Call_c result, TypeChecker tc) throws SemanticException {
    	X10Context c = (X10Context) tc.context();
    	X10MethodInstance mi = (X10MethodInstance) result.methodInstance();
    	if (mi !=null) {
    		X10Flags flags = X10Flags.toX10Flags(mi.flags());
    		if (c.inNonBlockingCode() 
    				&& ! (mi.isJavaMethod() || mi.isSafe() || flags.isNonBlocking()))
    			throw new SemanticException(mi + ": Only nonblocking methods can be called from nonblocking code.", 
    					position());
    		if (c.inSequentialCode() 
    				&& ! (mi.isJavaMethod()|| mi.isSafe() || flags.isSequential()))
    			throw new SemanticException(mi + ": Only sequential methods can be called from sequential code.", 
    					position());
    		if (c.inLocalCode() 
    				&& ! (mi.isJavaMethod() || mi.isSafe() || flags.isLocal()))
    			throw new SemanticException(mi + ": Only local methods can be called from local code.", 
    					position());
    	}
    }
	private Node superTypeCheck(TypeChecker tc) throws SemanticException {
        return super.typeCheck(tc);
    }

	private boolean isRestrictionArgType(Type type, X10TypeSystem xts) {
		return xts.isPlace(type) || xts.isDistribution(type) || xts.isRegion(type);
	}
}

