/**
 *
 * Ensures char arrays are implemented.
 */
public class Array3Char {

	public boolean run() {
		
		region e= [1:10];
		region r = [e,e];
		dist d=r->here;
		chk(d.equals([1:10,1:10]->here));
		char[.] ia = new char[d];
		ia[1,1] = 'a';
		return ('a' == ia[1,1]);
	
	}

    static void chk(boolean b) {if (!b) throw new Error();}
	
	
    public static void main(String[] args) {
        final boxedBoolean b=new boxedBoolean();
        try {
                finish b.val=(new Array3Char()).run();
        } catch (Throwable e) {
                e.printStackTrace();
                b.val=false;
        }
        System.out.println("++++++ "+(b.val?"Test succeeded.":"Test failed."));
        x10.lang.Runtime.setExitCode(b.val?0:1);
    }
    static class boxedBoolean {
        boolean val=false;
    }

}
