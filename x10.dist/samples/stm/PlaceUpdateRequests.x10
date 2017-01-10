import x10.util.Random;

public class PlaceUpdateRequests(size:Long){
    val accountsRail:Rail[Long];
    val amountsRail:Rail[Long];
    var amountsSum:Long = 0;
    public def this (s:Long) {
        property(s);
        accountsRail = new Rail[Long](s);
        amountsRail = new Rail[Long](s);
    }
    
    public def initRandom(accountsMAX:Long) {
        val rand = new Random(System.nanoTime());
        for (var i:Long = 0; i < size; i++) {
            accountsRail(i) = Math.abs(rand.nextLong()% accountsMAX);
            amountsRail(i) = Math.abs(rand.nextLong()%1000);
            amountsSum+= amountsRail(i) ;
        }
    }
}