import x10.util.security.SHA;

public class MsgDigest {
    
    public static def main(args:Rail[String]) {
        val n = 100000;
        val md = new SHA();
        val size = 20;
        val rail = new Rail[Byte](n * 20n + 4n);
        
        Console.OUT.println("Iterations: " + n);
        var startedNS:Long = System.nanoTime();
        for (var i:Long = 0; i < n; i++) {
            val st = i*size;
            val end = (i+1)*size;
            md.digest(rail, st as Int, size as Int);
        }
        Console.OUT.println("Update Time:" + (System.nanoTime() - startedNS)/1e9 + " seconds ");
        
        startedNS = System.nanoTime();
        for (var i:Long = 0; i < n; i++) {
            val st = i*size;
            val end = (i+1)*size;
            md.digest(rail, st as Int, size as Int);
        }
        Console.OUT.println("Digest Time:" + (System.nanoTime() - startedNS)/1e9 + " seconds ");
    }
}