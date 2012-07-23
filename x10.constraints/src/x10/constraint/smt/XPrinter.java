package x10.constraint.smt;

import java.io.Writer;

/**
 * Interface allowing for output to various constraint solvers.  
 * @author lshadare
 *
 */
public abstract class XPrinter {
	Writer writer; 
	
	public XPrinter(Writer w) {
		this.writer = w; 
	}

	public abstract <T extends XSmtType> void out(XSmtTerm<T> term);
	public abstract String toString(); 
	public abstract void dump(); 
}