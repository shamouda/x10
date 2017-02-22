import x10.util.Random;

public class PlaceRandomRequests(size:Long, num:Long,readPercent:Float)
{
    val keys1:Rail[Long];
    val values1:Rail[Long];
    var valuesSum1:Long = 0;

	var keys2:Rail[Long];
	var values2:Rail[Long];
	var valuesSum2:Long = 0;

    var isRead:Rail[Boolean];

    public def this (s:Long, num:Long, readPercent:Float) {
        property(s, num, readPercent);
        keys1 = new Rail[Long](s);
        values1 = new Rail[Long](s);
        if (num == 2) {
        	keys2 = new Rail[Long](s);
            values2 = new Rail[Long](s);
        }
        
        if (readPercent != -1.0F) {
        	isRead = new Rail[Boolean](s);
        }
    }

	public def initRandom(accountsMAX:Long, accountPerPlace:Long, pid:Long) {
        val rand = new Random(pid);
        for (var i:Long = 0; i < size; i++) {
        	keys1(i) = Math.abs(rand.nextLong()% accountsMAX);        	
        	values1(i) = Math.abs(rand.nextLong()%1000);
            valuesSum1+= values1(i);            
        }
        
        if (num == 2) {
        	for (var i:Long = 0; i < size; i++) {
        		val p1 = keys1(i)/accountPerPlace;
        		do {
        			keys2(i) = Math.abs(rand.nextLong()% accountsMAX);
        		}while(keys2(i)/accountPerPlace == p1);
        		values2(i) = Math.abs(rand.nextLong()%1000);
            	valuesSum2 += values2(i);            	
        	}
        }
        
        if (readPercent != -1F) {
        	var remainingReads:Long = ( size * readPercent ) as Long;
        	var remainingWrites:Long = size - remainingReads;
        	
        	for (var i:Long = 0; i < size; i++) {
        		if (remainingReads == 0)
        			isRead(i) = false;
        		else if (remainingWrites == 0)
        			isRead(i) = true;	
        		else {
        			if (rand.nextFloat() < readPercent)
        				isRead(i) = true;
        			else
        				isRead(i) = false;
        		}
        	
        		if (isRead(i))
        			remainingReads--;
        		else
        			remainingWrites--;
        	}
        }
    }
    
}