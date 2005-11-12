/*
 * NAS Parallel Benchmarks 3.0
 *
 * CG
 */
import NPB3_0_X10.*;
public class mg {

	public boolean run() {
		NPB3_0_X10.MG.mainEntry(new String[] {"-np8","CLASS=S"});
		return true;
		
	}

    public static void main(String[] args) {
        final boxedBoolean b=new boxedBoolean();
        try {
                finish async b.val=(new mg()).run();
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
