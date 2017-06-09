import x10.util.security.SHA;

public class MsgDigest {
    
    public static def main(args:Rail[String]) {
        val md = new SHA();
        val rail = new Rail[Byte](30);
        rail(0) = 10 as Byte;
        rail(1) = 20 as Byte;
        rail(2) = 30 as Byte;
        md.update(rail, 0n, 20n);
        md.digest(rail, 5n, 20n);
        for (var i:Long = 0; i < 30; i++)
            Console.OUT.print(rail(i) + " ");
        Console.OUT.println();
        /*
        Console.OUT.println("=======================");
        md.update(rail, 60n, 20n);
        md.digest(rail, 20n, 20n);
        for (var i:Long = 0; i < 30; i++)
            Console.OUT.print(rail(i) + " ");
        */
    }
}


