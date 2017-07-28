import x10.xrx.Runtime;

public class Test {
    
    public static def main(args:Rail[String]) {
//        at (Place(3)){

        val home = here;
        val next = Place(1);

        finish {
             at (next) async {
                 at (home) async {
                    think();
                 }
             }
        }


/*
        finish {
            at (next) async { think(); }
        }
*/

/*
       finish for (p in Place.places()) at (p) async {
           think();
       }
*/
  //  }    

    }

    public static def think() {
        for (var i:Long = 0; i < 100; i++) {
                     }

    }
}
