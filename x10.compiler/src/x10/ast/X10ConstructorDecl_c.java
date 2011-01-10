/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2010.
 */

package x10.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import polyglot.ast.Block;
import polyglot.ast.ConstructorDecl_c;
import polyglot.ast.FlagsNode;
import polyglot.ast.Formal;
import polyglot.ast.Id;
import polyglot.ast.Node;
import polyglot.ast.NodeFactory;
import polyglot.ast.TypeNode;
import polyglot.frontend.Globals;
import polyglot.types.ClassDef;
import polyglot.types.ConstructorDef;
import polyglot.types.Context;
import polyglot.types.Flags;
import polyglot.types.LazyRef;
import polyglot.types.LocalDef;
import polyglot.types.MethodDef;
import polyglot.types.Name;
import polyglot.types.QName;
import polyglot.types.Ref;
import polyglot.types.SemanticException;
import polyglot.types.ContainerType;
import polyglot.types.Type;
import polyglot.types.TypeSystem;
import polyglot.types.Types;
import polyglot.util.CodeWriter;
import polyglot.util.CollectionUtil;
import polyglot.util.ErrorInfo;
import polyglot.util.InternalCompilerError;
import polyglot.util.Position;
import polyglot.util.TypedList;
import polyglot.visit.ContextVisitor;
import polyglot.visit.NodeVisitor;
import polyglot.visit.PrettyPrinter;
import polyglot.visit.TypeBuilder;
import polyglot.visit.TypeCheckPreparer;
import polyglot.visit.TypeChecker;
import x10.constraint.XFailure;
import x10.constraint.XName;
import x10.constraint.XNameWrapper;
import x10.constraint.XVar;
import x10.constraint.XTerm;
import x10.constraint.XTerms;
import x10.constraint.XVar;
import x10.errors.Errors;
import x10.extension.X10Del;
import x10.extension.X10Del_c;
import x10.extension.X10Ext;
import x10.types.X10ClassDef;
import x10.types.X10ClassType;
import x10.types.X10ConstructorDef;
import polyglot.types.Context;
import x10.types.X10MemberDef;
import x10.types.X10ParsedClassType;
import x10.types.X10ProcedureDef;

import x10.types.X10Context_c;
import polyglot.types.TypeSystem;
import x10.types.checker.PlaceChecker;
import x10.types.checker.ThisChecker;
import x10.types.checker.VarChecker;
import x10.types.constraints.CConstraint;
import x10.types.constraints.TypeConstraint;
import x10.types.constraints.XConstrainedTerm;
import x10.visit.CheckEscapingThis;
import x10.visit.X10TypeChecker;

/**
 * An X10ConstructorDecl differs from a ConstructorDecl in that it has a returnType.
 *
 * @author vj
 */
public class X10ConstructorDecl_c extends ConstructorDecl_c implements X10ConstructorDecl {
   
    protected DepParameterExpr guard; // ignored for now.
    protected TypeNode returnType;
    protected List<TypeParamNode> typeParameters;
    protected TypeNode hasType;
    protected TypeNode offerType; 
    
    public X10ConstructorDecl_c(Position pos, FlagsNode flags, 
            Id name, TypeNode returnType, 
            List<TypeParamNode> typeParams, List<Formal> formals, 
            DepParameterExpr guard,  TypeNode offerType, Block body) {
        super(pos, flags,  name, formals,  body);
        // null, not unknown. 
        this.returnType = returnType instanceof HasTypeNode_c ? null : returnType; 
        if (returnType instanceof HasTypeNode_c) 
			hasType = ((HasTypeNode_c) returnType).typeNode();
        this.guard = guard;
        this.typeParameters = TypedList.copyAndCheck(typeParams, TypeParamNode.class, true);
        this.offerType = offerType;
    }
    
    public TypeNode returnType() {
        return returnType;
    }

    public DepParameterExpr guard() {
        return guard;
    }

    public X10ConstructorDecl_c guard(DepParameterExpr e) {
        X10ConstructorDecl_c n = (X10ConstructorDecl_c) copy();
        n.guard = e;
        return n;
    }
    public TypeNode offerType() {
        return offerType;
    }
    public X10ConstructorDecl_c offerType(TypeNode offerType) {
        if (this.offerType != offerType)  {
            X10ConstructorDecl_c n = (X10ConstructorDecl_c) copy();
            n.offerType = offerType;
            return n;
        }
        return this;
    }
    protected X10ConstructorDecl_c hasType(TypeNode hasType) {
    	if (this.hasType != hasType)  {
    		X10ConstructorDecl_c n = (X10ConstructorDecl_c) copy();
    		n.hasType = hasType;
    		return n;
    	}
    	return this;
    }
    
    public List<TypeParamNode> typeParameters() {
	    return typeParameters;
    }
    
    public X10ConstructorDecl_c typeParameters(List<TypeParamNode> typeParams) {
	    X10ConstructorDecl_c n = (X10ConstructorDecl_c) copy();
	    n.typeParameters=TypedList.copyAndCheck(typeParams, TypeParamNode.class, true);
	    return n;
    }

    @Override
    public X10ConstructorDecl_c flags(FlagsNode flags) {
        return (X10ConstructorDecl_c) super.flags(flags);
    }
    @Override
    public X10ConstructorDecl_c name(Id name) {
        return (X10ConstructorDecl_c) super.name(name);
    }
    @Override
    public X10ConstructorDecl_c formals(List<Formal> formals) {
        return (X10ConstructorDecl_c) super.formals(formals);
    }
    @Override
    public X10ConstructorDecl_c constructorDef(ConstructorDef ci) {
        return (X10ConstructorDecl_c) super.constructorDef(ci);
    }
    @Override
    public X10ConstructorDef constructorDef() {
        return (X10ConstructorDef) super.constructorDef();
    }

    public X10ConstructorDecl_c returnType(TypeNode returnType) {
        if (returnType != this.returnType) {
            X10ConstructorDecl_c n = (X10ConstructorDecl_c) copy();
            n.returnType = returnType;
            return n;
        }
        return this;
    }

    protected ConstructorDef createConstructorDef(TypeSystem ts, ClassDef ct, Flags flags) {
    	X10ConstructorDef ci = (X10ConstructorDef) ((TypeSystem) ts).constructorDef(position(), Types.ref(ct.asType()), flags,
                Collections.<Ref<? extends Type>>emptyList(), 
                offerType == null ? null : offerType.typeRef());
        
        ci.setThisDef(((X10ClassDef) ct).thisDef());
        return ci;
    }

    @Override
    public Node buildTypesOverride(TypeBuilder tb) {
	NodeFactory nf = tb.nodeFactory();
	
        X10ConstructorDecl_c n = (X10ConstructorDecl_c) super.buildTypesOverride(tb);
        
        X10ConstructorDef ci = (X10ConstructorDef) n.constructorDef();

        n = (X10ConstructorDecl_c) X10Del_c.visitAnnotations(n, tb);

        List<AnnotationNode> as = ((X10Del) n.del()).annotations();
        if (as != null) {
            List<Ref<? extends Type>> ats = new ArrayList<Ref<? extends Type>>(as.size());
            for (AnnotationNode an : as) {
                ats.add(an.annotationType().typeRef());
            }
            ci.setDefAnnotations(ats);
        }

        ClassDef currentClass = tb.currentClass();

        // Set the constructor name to the short name of the class, to shut up the Java type-checker.
        // The X10 parser has "this" for the name.
        n = (X10ConstructorDecl_c) n.name(nf.Id(n.position(), currentClass.name()));

        TypeNode htn = (TypeNode) n.visitChild(n.hasType, tb);
        n = (X10ConstructorDecl_c) n.hasType(htn);
        
        TypeNode rtn = (TypeNode) n.visitChild(n.returnType, tb);
        // Enable return type inference for this constructor declaration.
        if (rtn == null) {
            Type rType = currentClass.asType();
            rType = Types.instantiateTypeParametersExplicitly(rType);
            if (ci.derivedReturnType()) {
                ci.inferReturnType(true);
                rtn = nf.CanonicalTypeNode(n.position(), Types.lazyRef(rType));
            } else {
                rtn = nf.CanonicalTypeNode(n.position(), rType);
            }
        }
        n = (X10ConstructorDecl_c) n.returnType(rtn);
        
        ci.setReturnType((Ref<? extends X10ClassType>) n.returnType().typeRef());
        
        if (n.guard() != null) {
            ci.setGuard(n.guard().valueConstraint());
            ci.setTypeGuard(n.guard().typeConstraint());
        }
        
        if (n.typeParameters().size() > 0) {
            Errors.issue(tb.job(), new SemanticException("Constructors cannot have type parameters.", n.position()));
            n = (X10ConstructorDecl_c) n.typeParameters(Collections.<TypeParamNode>emptyList());
        }
        
        List<LocalDef> formalNames = new ArrayList<LocalDef>(n.formals().size());
        for (Formal f : n.formals()) {
            formalNames.add(f.localDef());
        }
        ci.setFormalNames(formalNames);

        return n;
    }

    @Override
    public Context enterScope(Context c) {
        c = super.enterScope(c);
        if (!c.inStaticContext() && constructorDef().thisDef() != null)
            c.addVariable(constructorDef().thisDef().asInstance());
        return c;
    }

    @Override
    public Context enterChildScope(Node child, Context c) {
        // We should have entered the constructor scope already.
        assert c.currentCode() == this.constructorDef();

        if (child != body) {
            // Push formals so they're in scope in the types of the other formals.
            c = c.pushBlock();

            boolean isParam = false;
            for (TypeParamNode f : typeParameters) {
                if (child == f) {
                    isParam = true;
                    break;
                }
            }
            for (Formal f : formals) {
                if (child == f) {
                    isParam = true;
                    break;
                }
            }

            for (TypeParamNode f : typeParameters) {
                f.addDecls(c);
            }
            
            for (Formal f : formals) {
                f.addDecls(c);
            }
        }
        // Ensure that the place constraint is set appropriately when
        // entering the body of the method.
       
        c  = super.enterChildScope(child, c);
        Context xc = (Context) c;
        
        TypeSystem xts = (TypeSystem) c.typeSystem();
        if (child == body || child == returnType || child == hasType ||  child == offerType || (formals != null && formals.contains(child))) {
        	c = PlaceChecker.pushHereIsThisHome(xc);
        }

        if (child == body && offerType != null && offerType.typeRef().known()) {
            c = c.pushCollectingFinishScope(offerType.type());
        }

        // Add the constructor guard into the environment.
        if (guard != null) {
            Ref<CConstraint> vc = guard.valueConstraint();
            Ref<TypeConstraint> tc = guard.typeConstraint();
        
            if (vc != null || tc != null) {
                c = c.pushBlock();
                try {
					if (vc.known())
						c = ((Context) c).pushAdditionalConstraint(vc.get());
					if (tc.known())
						c = ((X10Context_c) c).pushTypeConstraintWithContextTerms(tc.get());
                } catch (SemanticException z) {
                	throw 
                	new InternalCompilerError("Unexpected inconsistent guard" + z);
                }
        //        ((X10Context) c).setCurrentConstraint(vc.get());
        //        ((X10Context) c).setCurrentTypeConstraint(tc.get());
            }            
        }


        return c;
    }

    /** Visit the children of the method. */
    public Node visitSignature(NodeVisitor v) {
    	X10ConstructorDecl_c result = (X10ConstructorDecl_c) super.visitSignature(v);
        List<TypeParamNode> typeParams = visitList(result.typeParameters, v);
        if (! CollectionUtil.allEqual(typeParams, result.typeParameters))
            result = (X10ConstructorDecl_c) result.typeParameters(typeParams);
    	TypeNode returnType = (TypeNode) visitChild(result.returnType, v);
    	if (returnType != result.returnType)
    	    result = (X10ConstructorDecl_c) result.returnType(returnType);
    	DepParameterExpr guard = (DepParameterExpr) visitChild(result.guard, v);
    	if (guard != result.guard)
    	    result = (X10ConstructorDecl_c) result.guard(guard);
    	TypeNode htn = (TypeNode) result.visitChild(result.hasType, v);
    	result = (X10ConstructorDecl_c) result.hasType(htn);
    	return result;
    }

    @Override
    public Node setResolverOverride(Node parent, TypeCheckPreparer v) {
        if (constructorDef().inferReturnType() && body() != null) {
            TypeNode tn = returnType();

            NodeVisitor childv = v.enter(parent, this);
            childv = childv.enter(this, returnType());

            if (childv instanceof TypeCheckPreparer) {
                TypeCheckPreparer tcp = (TypeCheckPreparer) childv;
                final LazyRef<Type> r = (LazyRef<Type>) tn.typeRef();
                TypeChecker tc = new X10TypeChecker(v.job(), v.typeSystem(), v.nodeFactory(), v.getMemo(), true);
                tc = (TypeChecker) tc.context(tcp.context().freeze());
                r.setResolver(new TypeCheckReturnTypeGoal(this, new Node[] { guard() }, body(), tc, r, r.getCached()));
            }
        }
        return super.setResolverOverride(parent, v);
    }

    public Node typeCheckOverride(Node parent, ContextVisitor tc) {
    	X10ConstructorDecl nn = this;
    	X10ConstructorDecl old = nn;

        TypeSystem xts = (TypeSystem) tc.typeSystem();
        
        // Step I.a.  Check the formals.
        TypeChecker childtc = (TypeChecker) tc.enter(parent, nn);
        
    	// First, record the final status of each of the type params and formals.
        List<TypeParamNode> processedTypeParams = nn.visitList(nn.typeParameters(), childtc);
        nn = (X10ConstructorDecl) nn.typeParameters(processedTypeParams);
    	List<Formal> processedFormals = nn.visitList(nn.formals(), childtc);
        nn = (X10ConstructorDecl) nn.formals(processedFormals);
        
        nn = (X10ConstructorDecl) X10Del_c.visitAnnotations(nn, childtc);

        // [NN]: Don't do this here, do it on lookup of the formal.  We don't want spurious self constraints in the signature.
//        for (Formal n : processedFormals) {
//    		Ref<Type> ref = (Ref<Type>) n.type().typeRef();
//    		Type newType = ref.get();
//    		
//    		if (n.localDef().flags().isFinal()) {
//    			CConstraint c = X10TypeMixin.xclause(newType);
//    			if (c == null) c = new CConstraint_c();
//    			try {
//				c.addSelfBinding(xts.xtypeTranslator().trans(n.localDef().asInstance()));
//			}
//			catch (XFailure e) {
//				throw new SemanticException(e.getMessage(), position());
//			}
//    			newType = X10TypeMixin.xclause(newType, c);
//    		}
//    		
//    		ref.update(newType);
//        }
        
        // Step I.b.  Check the guard.
        if (nn.guard() != null) {
        	DepParameterExpr processedWhere = (DepParameterExpr) nn.visitChild(nn.guard(), childtc);
        	nn = (X10ConstructorDecl) nn.guard(processedWhere);
        
        	// Now build the new formal arg list.
        	// TODO: Add a marker to the TypeChecker which records
        	// whether in fact it changed the type of any formal.
        	List<Formal> formals = processedFormals;
        	
        	//List newFormals = new ArrayList(formals.size());
        	X10ProcedureDef pi = (X10ProcedureDef) nn.memberDef();
        	CConstraint c = pi.guard().get();
            	try {
            		if (c != null) {
            			c = c.copy();

            			for (Formal n : formals) {
            				Ref<Type> ref = (Ref<Type>) n.type().typeRef();
            				Type newType =  ref.get();

            				// Fold the formal's constraint into the guard.
            				XVar var = xts.xtypeTranslator().trans(n.localDef().asInstance());
            				CConstraint dep = Types.xclause(newType);
            				if (dep != null) {
            				    dep = dep.copy();
            				    dep = dep.substitute(var, c.self());
                                /*
                                XPromise p = dep.intern(var);
                                dep = dep.substitute(p.term(), c.self());
                                */
            				    c.addIn(dep);
            				}

            				ref.update(newType);
            			}
            		}

            		// Report.report(1, "X10MethodDecl_c: typeoverride mi= " + nn.methodInstance());

            		// Fold this's constraint (the class invariant) into the guard.
            		{
            			Type t =  tc.context().currentClass();
            			CConstraint dep = Types.xclause(t);
            			if (c != null && dep != null) {
            				XVar thisVar = constructorDef().thisVar();
            				if (thisVar != null)
            				    dep = dep.substitute(thisVar, c.self());
//            				dep = dep.copy();
//            				XPromise p = dep.intern(xts.xtypeTranslator().transThis(t));
//            				dep = dep.substitute(p.term(), c.self());
            				c.addIn(dep);
            			}
            		}
            	}
            	catch (XFailure e) {
                    tc.errorQueue().enqueue(ErrorInfo.SEMANTIC_ERROR, e.getMessage(), position());
                    c = null;
            	}

        	// Check if the guard is consistent.
        	if (c != null && ! c.consistent()) {
                Errors.issue(tc.job(),
                        new Errors.DependentClauseIsInconsistent("constructor", guard),
                        this);
                // FIXME: [IP] mark constraint clause as invalid
        	}
        }

        // Step I.c. Check the throw types
    

        X10ConstructorDef nnci = (X10ConstructorDef) nn.constructorDef();

        // Step II. Check the return type. 
        // Now visit the returntype to ensure that its depclause, if any is processed.
        // Visit the formals so that they get added to the scope .. the return type
        // may reference them.
    	//TypeChecker tc1 = (TypeChecker) tc.copy();
    	// childtc will have a "wrong" mi pushed in, but that doesnt matter.
    	// we simply need to push in a non-null mi here.
    	TypeChecker childtc1 = (TypeChecker) tc.enter(parent, nn);
    	if (childtc1.context() == tc.context())
    	    childtc1 = (TypeChecker) childtc1.context((Context) tc.context().copy());
    	// Add the type params and formals to the context.
    	nn.visitList(nn.typeParameters(),childtc1);
    	nn.visitList(nn.formals(),childtc1);
    	childtc1.context().setVarWhoseTypeIsBeingElaborated(null);
    	TypeNode r = (TypeNode) nn.visitChild(nn.returnType(), childtc1);
    	Ref<? extends Type> ref = r.typeRef();
    	Type type = Types.get(ref);
    	X10ClassType container =  (X10ClassType) Types.instantiateTypeParametersExplicitly(tc.context().currentClass());
    	if (! tc.typeSystem().isSubtype(type, container, tc.context())) {
    	    Errors.issue(tc.job(),
    	            new Errors.ConstructorReturnTypeNotSubtypeOfContainer(type, container, position()));
    	    r = tc.nodeFactory().CanonicalTypeNode(r.position(), container);
    	}
    	nn = (X10ConstructorDecl) nn.returnType(r);
    	((Ref<Type>) nnci.returnType()).update(r.type());

        //Report.report(1, "X10MethodDecl_c: typeoverride mi= " + nn.methodInstance());

       	// Step III. Check the body. 
       	// We must do it with the correct mi -- the return type will be
       	// checked by return e; statements in the body.
       	
       	TypeChecker childtc2 = (TypeChecker) tc.enter(parent, nn);
       	// Add the formals to the context.
       	nn.visitList(nn.typeParameters(),childtc2);
       	nn.visitList(nn.formals(),childtc2);
       	//Report.report(1, "X10MethodDecl_c: after visiting formals " + childtc2.context());
       	// Now process the body.
        nn = (X10ConstructorDecl) nn.body((Block) nn.visitChild(nn.body(), childtc2));
        nn = (X10ConstructorDecl) childtc2.leave(parent, old, nn, childtc2);
        
        try {
            X10MethodDecl_c.dupFormalCheck(typeParameters, formals);
        } catch (SemanticException e) {
            Errors.issue(tc.job(), e, this);
        }
        {
            if (hasType != null) {
                final TypeNode h = (TypeNode) nn.visitChild(((X10ConstructorDecl_c) nn).hasType, childtc1);
                Type hasType = PlaceChecker.ReplaceHereByPlaceTerm(h.type(), childtc1.context());
                nn = (X10ConstructorDecl) ((X10ConstructorDecl_c) nn).hasType(h);
                if (! tc.typeSystem().isSubtype(nnci.returnType().get(), hasType,tc.context())) {
                    Errors.issue(tc.job(),
                            new Errors.TypeIsNotASubtypeOfTypeBound(type, hasType, position()));
                }
            }
        }

        return nn;
    }
    
    @Override
    public Node typeCheck(ContextVisitor tc) {
        X10ConstructorDecl_c n = this;
        
        ThisChecker thisC = new ThisChecker(tc.job());
        if (formals != null) {
            visitList(formals, thisC);
            if (thisC.error()) {
                Errors.issue(tc.job(),
                        new Errors.ThisNotPermittedInConstructorFormals(formals, position()));
            }
        }
        thisC.clearError();

        if (returnType != null) {
            if (false) { // todo: remove this after fixing XTENLANG-1770
                visitChild(returnType, thisC);
            }
            if (thisC.error()) {
                Errors.issue(tc.job(),
                        new Errors.ThisNotPermittedInConstructorReturnType(returnType, position()));
            }
        }

        n = (X10ConstructorDecl_c) super.typeCheck(tc);
        return n;
    }

    public Node conformanceCheck(ContextVisitor tc) {
        X10ConstructorDecl_c n = (X10ConstructorDecl_c) super.conformanceCheck(tc);
        TypeSystem ts = (TypeSystem) tc.typeSystem();
        
        Type retTypeBase =  n.returnType().type();
        retTypeBase = Types.baseType(retTypeBase);
        retTypeBase = Types.instantiateTypeParametersExplicitly(retTypeBase);
        
        X10ConstructorDef nnci = (X10ConstructorDef) n.constructorDef();
        // Type clazz = ((X10Type) nnci.asInstance().container()).setFlags(X10Flags.ROOTED);
        Type clazz = nnci.asInstance().container();
        clazz = Types.instantiateTypeParametersExplicitly(clazz);
        if (! ts.typeEquals(retTypeBase, clazz, tc.context())) {
            Errors.issue(tc.job(),
                    new SemanticException("The return type of the constructor (" + retTypeBase + ") must be derived from the type of the class (" + clazz + ") on which the constructor is defined.",    n.position()));
        }
        
        X10MethodDecl_c.checkVisibility(tc, this);

        return n;
    }
    /** Visit the children of the constructor. */
    public Node visitChildren(NodeVisitor v) {
        X10ConstructorDecl_c n = (X10ConstructorDecl_c) super.visitChildren(v);
        TypeNode htn = (TypeNode) n.visitChild(n.hasType, v);
        return n.hasType(htn);
    }

    public String toString() {
        return flags.flags().translate() + "this(...)";
    }

    /** Write the constructor to an output file. */
    public void prettyPrintHeader(CodeWriter w, PrettyPrinter tr) {
        w.begin(0);
        for (Iterator<AnnotationNode> i = (((X10Ext) this.ext()).annotations()).iterator(); i.hasNext(); ) {
            AnnotationNode an = i.next();
            an.prettyPrint(w, tr);
            w.allowBreak(0, " ");
        }
        tr.print(this, flags, w);
        w.write("def this(");

        w.begin(0);

        for (Iterator<Formal> i = formals.iterator(); i.hasNext(); ) {
            Formal f = i.next();
            print(f, w, tr);

            if (i.hasNext()) {
                w.write(",");
                w.allowBreak(0, " ");
            }
        }

        w.end();
        w.write(")");
/*
        if (! throwTypes().isEmpty()) {
            w.allowBreak(6);
            w.write("throws ");

            for (Iterator<TypeNode> i = throwTypes().iterator(); i.hasNext(); ) {
                TypeNode tn = i.next();
                print(tn, w, tr);

                if (i.hasNext()) {
                    w.write(",");
                    w.allowBreak(4, " ");
                }
            }
        }
*/
        w.end();
    }
}
