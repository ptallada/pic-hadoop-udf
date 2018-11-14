package es.pic.astro.hive_udf;

import java.lang.Math;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import healpix.essentials.HealpixProc;
import healpix.essentials.Vec3;

@Description(
    name="neighbours",
    value="_FUNC_(order, pix, nest) - Returns a struct with 8 neighbours",
    extended="SELECT _FUNC_(3, 646, false) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFNeighbours extends GenericUDF {

    private final Object[] result = new Object[8];
    private final LongWritable[] nWritable = new LongWritable[8];    

    private Converter[] intconverter = new Converter[1];
    private Converter[] longconverter = new Converter[1];
    private Converter[] booleanconverter = new Converter[1];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("neighbours() takes at least 2 arguments, not more than 3: order, pix, nest");
        }
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        
        intconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[0], intOI);
        longconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[1], longOI);
        if (arguments.length == 3) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }
        int i;        
        for(i = 0; i < 8; i++) {
           fieldNames.add("N"+i); 
        }
        for(i = 0; i < 8; i++) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        }
        for(i = 0; i < 8; i++) {
            result[i] = nWritable[i]; 
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        IntWritable argsw1 = new IntWritable();
        LongWritable argsw2 = new LongWritable();
        BooleanWritable argsw3 = new BooleanWritable(false);
        argsw1 = (IntWritable) intconverter[0].convert(arguments[0].get());
        argsw2 = (LongWritable) longconverter[0].convert(arguments[1].get());
        int i;
        if (arguments.length == 3) {
            argsw3 = (BooleanWritable) booleanconverter[0].convert(arguments[2].get());
        }
        if (argsw1 == null || argsw2 == null){
            for(i = 0; i < 8; i++){ result[i] = null;   }
            return result;
        }

        int order = argsw1.get();
        long pix = argsw2.get();
        boolean nest = argsw3.get();

        long[] res = new long[8];
        for(i = 0; i < 8; i++)
        {
            nWritable[i] = new LongWritable();
            res[i] = 0L;         
        }

        try{
            if (nest == true) {
                res = HealpixProc.neighboursNest(order, pix);
            } else {
                res = HealpixProc.neighboursRing(order, pix);
            }    
        } catch (Exception e) {}

        for(i = 0; i < 8; i++){ 
            nWritable[i].set(res[i]);
            result[i] = nWritable[i];
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "neighbours(order, pix, nest)";
    }
}
