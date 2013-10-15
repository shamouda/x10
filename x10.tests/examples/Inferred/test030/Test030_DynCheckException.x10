//OPTIONS: -STATIC_CHECKS=false -CONSTRAINT_INFERENCE=false -VERBOSE_INFERENCE=true



import harness.x10Test;

public class Test030_DynCheckException extends x10Test {

    public def mustFailRun(): boolean {
	Test030_DynChecks.f(1, 1, 2);
        return true;
    }

    public def run() {
        try { mustFailRun(); return false; } catch (FailedDynamicCheckException) {}
        return true;
    }

    public static def main(Rail[String]) {
    	new Test030_DynCheckException().execute();
    }

}
