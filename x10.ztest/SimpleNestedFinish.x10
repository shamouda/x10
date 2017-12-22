public class Simple {
    
    public static def main (args:Rail[String]) {
        
        Console.OUT.println("==========");
        finish {
            async at (Place(1)) {
                Console.OUT.println("p1 task");
                finish {
                    async at (Place(2)) {
                        Console.OUT.println("p2 task");
                    }
                }
            }
            
            async at (Place(3)) {
                Console.OUT.println("p3 task");
            }
            
            async at (Place(3)) {
                Console.OUT.println("p3.2 task");
            }
        }
        
    }
}