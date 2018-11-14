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
import healpix.essentials.Pointing;

@Description(
    name="ring2nest",
    value="_FUNC_(order, ring) - return nest ordering of a ring ordering",
    extended="SELECT _FUNC_(10, 646) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFRing2Nest extends GenericUDF {

    private final Object[] result = new Object[1];
    private final LongWritable pixWritable = new LongWritable();    

    private Converter intconverter;
    private Converter longconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("ring2nest() takes 2 arguments: ring, nest");
        }

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);

        intconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], intOI);
        longconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[1], longOI);

        result[0] = pixWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        IntWritable argsw1 = new IntWritable();
        LongWritable argsw2 = new LongWritable();

        argsw1 = (IntWritable) intconverter.convert(arguments[0].get());
        argsw2 = (LongWritable) longconverter.convert(arguments[1].get());

       if (argsw1 == null || argsw2 == null){
            result[0] = null;
            return result;
        }

        int order = argsw1.get();
        long ring = argsw2.get();
        long nest = 0;
        
        try {
            nest = HealpixProc.ring2nest(order, ring);
        } catch (Exception e) {}
        
        pixWritable.set(nest);
        result[0] = pixWritable;

        return result[0];
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "ring2nest(order, ring)";
    }
}
