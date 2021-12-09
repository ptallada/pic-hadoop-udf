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

    private Converter intconverter;
    private Converter longconverter;
    private Converter booleanconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("neighbours() takes at least 2 arguments, not more than 3: order, pix, nest");
        }
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        
        intconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], intOI);
        longconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[1], longOI);
        if (arguments.length == 3) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }
        
        return ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableLongObjectInspector
        );
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
        IntWritable argsw1 = new IntWritable();
        LongWritable argsw2 = new LongWritable();
        BooleanWritable argsw3 = new BooleanWritable(false);
        argsw1 = (IntWritable) intconverter.convert(arguments[0].get());
        argsw2 = (LongWritable) longconverter.convert(arguments[1].get());
        if (arguments.length == 3) {
            argsw3 = (BooleanWritable) booleanconverter.convert(arguments[2].get());
        }
        
        if (argsw1 == null || argsw2 == null){
            return null;
        }

        int order = argsw1.get();
        long pix = argsw2.get();
        boolean nest = argsw3.get();

        ArrayList<LongWritable> result = new ArrayList<LongWritable>();        
        long[] res;
        try{
            if (nest == true) {
                res = HealpixProc.neighboursNest(order, pix);
            } else {
                res = HealpixProc.neighboursRing(order, pix);
            }
            
            for(int i = 0; i < 8; i++){ 
                result.add(new LongWritable(res[i]));
            }
        } catch (Exception e) {}

        return result;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "neighbours(order, pix, nest)";
    }
}
