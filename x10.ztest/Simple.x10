public class SimpleNestedFinish {
    
    public static def main (args:Rail[String]) {
        finish {
            Console.OUT.println("finish1");
            async at (Place(1)) {
                Console.OUT.println("p1 task");
                finish {
                    Console.OUT.println("finish2");
                    finish {
                        Console.OUT.println("finish3");
                        at (Place(2)) async {
                            Console.OUT.println("p2 task");
                        }
                    }
                }
            }
        }
    }
}