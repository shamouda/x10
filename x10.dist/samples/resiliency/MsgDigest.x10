import x10.util.security.SHA;

public class MsgDigest {
    
    public static def main(args:Rail[String]) {
        val md = new SHA();
        val size = 20;
        val rail = new Rail[Byte](60);
        
        for (var i:Long = 0; i < 3; i++) {
            val st = i*size;
            val end = (i+1)*size;
            md.update(rail, st as Int, size as Int);
            md.digest(rail, st as Int, size as Int);
            for (var j:Long = 0; j < 60; j++)
                Console.OUT.print(j + "- " + rail(j) + " \n");
            Console.OUT.println("=======================");
        }
    }
}