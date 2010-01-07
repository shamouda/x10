import harness.x10Test;

public class ArrayPlusEqual extends x10Test {

    val v = Rail.make[int](2, (x:int)=>0);

    public def run() {
        for ((i) in 0..1) v(i) += 5;
        for ((i) in 0..1) chk(v(i) == 5);
        return true;
    }

    public static def main(Rail[String]) {
        new ArrayPlusEqual().execute();
    }
}
