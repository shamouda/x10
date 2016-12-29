package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.resilient.localstore.Cloneable;

/*Master state as stored at the slave*/
class SlaveMasterState {
    public var maps:HashMap[String,HashMap[String,Cloneable]];
   
    public def this(maps:HashMap[String,HashMap[String,Cloneable]]) {
        this.maps = maps;
    }
    
    public def getMapData(mapName:String) {
        var data:HashMap[String,Cloneable] = maps.getOrElse(mapName, null);
        if (data == null) {
            data = new HashMap[String,Cloneable]();
            maps.put(mapName, data);
        }
        return data;
    }
}