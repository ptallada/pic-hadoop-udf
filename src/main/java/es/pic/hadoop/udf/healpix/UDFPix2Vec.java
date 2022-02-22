package es.pic.astro.hive_udf;

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
    name="pix2vec",
    value="_FUNC_(order, pix, nest) - Returns a struct with the vector of the pix",
    extended="SELECT _FUNC_(3, 646, false) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFPix2Vec extends GenericUDF {

    private final Object[] result = new Object[3];
    private final DoubleWritable xWritable = new DoubleWritable();
    private final DoubleWritable yWritable = new DoubleWritable();
    private final DoubleWritable zWritable = new DoubleWritable();    

    private Converter[] intconverter = new Converter[1];
    private Converter[] longconverter = new Converter[1];
    private Converter[] booleanconverter = new Converter[1];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("pix2vec() takes at least 2 arguments, no more than 3: order, pix, nest");
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

        fieldNames.add("x");
        fieldNames.add("y");
        fieldNames.add("z");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        result[0] = xWritable;
        result[1] = yWritable;
        result[2] = zWritable;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        IntWritable argsw1 = new IntWritable();
        LongWritable argsw2 = new LongWritable();
        BooleanWritable argsw3 = new BooleanWritable(false);

        argsw1 = (IntWritable) intconverter[0].convert(arguments[0].get());
        argsw2 = (LongWritable) longconverter[0].convert(arguments[1].get());

        if (arguments.length == 3) {
            argsw3 = (BooleanWritable) booleanconverter[0].convert(arguments[2].get());
        }
        if (argsw1 == null || argsw2 == null){
            result[0] = null;
            result[1] = null;
            return result;
        }

        int order = argsw1.get();
        long pix = argsw2.get();
        boolean nest = argsw3.get();
        Vec3 res = new Vec3();

        try{     
            if (nest == true) {
                res = HealpixProc.pix2vecNest(order, pix);
            } else {
                res = HealpixProc.pix2vecRing(order, pix);
            } 
        }catch(Exception e){}
        
        xWritable.set(res.x);
        yWritable.set(res.y);
        zWritable.set(res.z);
 
        result[0] = xWritable;
        result[1] = yWritable;
        result[2] = zWritable;

        return result;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "pix2vec(order, hpix, nest)";
    }
}
