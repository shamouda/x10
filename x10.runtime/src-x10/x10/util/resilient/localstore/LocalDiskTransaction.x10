package x10.util.resilient.localstore;

import x10.util.HashMap;
import x10.util.ArrayList;
import x10.util.HashSet;
import x10.compiler.Ifdef;
import x10.xrx.Runtime;
import x10.io.*;

public class LocalDiskTransaction (id:Long, placeIndex:Long, randomRunId:Long) {
    private val moduleName = "LocalDiskTransaction";

    public def put(key:String, newValue:Cloneable) {
    	val nameStr = "x10chkpt_"+placeIndex+"_"+randomRunId+"_"+key+".bin";
    	val sizeStr = "x10chkpt_"+placeIndex+"_"+randomRunId+"_"+key+".size";   	
        val ser = new Serializer();
        ser.writeAny(newValue);
        val bytes = ser.toRail();
        val size = bytes.size;
        val sizeFile = new FileWriter(new File(sizeStr));
        sizeFile.write((size+""));        
        val dataFile = new FileWriter(new File(nameStr));
        dataFile.write(bytes, 0, size);
        dataFile.close();
        sizeFile.close();
    }
      
    public def get(key:String):Cloneable {
    	val nameStr = "x10chkpt_"+placeIndex+"_"+randomRunId+"_"+key+".bin";
    	val sizeStr = "x10chkpt_"+placeIndex+"_"+randomRunId+"_"+key+".size";   
    	
    	val sizeReader = new FileReader(new File(sizeStr));
    	val size = Long.parseLong(sizeReader.readLine());
    	
    	val dataReader = new FileReader(new File(nameStr));
    	
        val bytes = new Rail[Byte](size);
        dataReader.read(bytes, 0, size);
         
        val des = new Deserializer(bytes);
        val s = des.readAny() as Cloneable;
        dataReader.close();
        sizeReader.close();
        return s;
    }
}