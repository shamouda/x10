public class SimpleAt {
    
    public static def main (args:Rail[String]) {
        
        Console.OUT.println("==========");
        finish {
            at (Place(1)) async {
                Console.OUT.println("p1 task");
                finish {
                    at (Place(2)) async {
                        Console.OUT.println("p2 task");
                    }
                }
            }
            
            at (Place(3)) async {
                Console.OUT.println("p3 task");
            }
            
            at (Place(3)) async {
                Console.OUT.println("p3.2 task");
            }
        }
        
    }
}