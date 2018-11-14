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
    name="vec2pix",
    value="_FUNC_(x, y, z, nest) - Returns a pix of a vector",
    extended="SELECT _FUNC_(1.27, 1.34, -0.34, false) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFVec2Pix extends GenericUDF {

    private final Object[] result = new Object[1];
    private final LongWritable pixWritable = new LongWritable();    

    private Converter intconverter;
    private Converter[] doubleconverter = new Converter[3];
    private Converter[] booleanconverter = new Converter[1];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 4 || arguments.length > 5) {
            throw new UDFArgumentLengthException("pix2vec() takes at least 3 arguments, no more than 5: order, x, y, z, nest");
        }

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

        intconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], intOI);
        for (int i = 0; i < 3; i ++) {
            doubleconverter[i] = (Converter) ObjectInspectorConverters.getConverter(arguments[i + 1], doubleOI);
        }
        if (arguments.length == 5) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[4], boolOI);
            
        }

        result[0] = pixWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        IntWritable argsw1 = new IntWritable();
        DoubleWritable[] argsw2 = new DoubleWritable[3];
        BooleanWritable argsw5 = new BooleanWritable(false);

        argsw1 = (IntWritable) intconverter.convert(arguments[0].get());
        for( int i = 0; i < 3; i ++) {
            argsw2[i] = (DoubleWritable) doubleconverter[i].convert(arguments[i+1].get());
        }

        if (arguments.length == 5) {
            argsw5 = (BooleanWritable) booleanconverter[0].convert(arguments[4].get());
        }

        if (argsw1 == null || argsw2[0] == null || argsw2[1] == null || argsw2[2] == null){
            result[0] = null;
            return result;
        }

        int order = argsw1.get();
        double[] v = new double[3];
        for ( int i = 0 ; i < 3; i ++) {
            v[i] = argsw2[i].get();
        }

        boolean nest = argsw5.get();
        long pix = 0;

        try {
            if (nest) {
                pix = HealpixProc.vec2pixNest(order,
                new Vec3(v[0],v[1],v[2])); 
            } else {
                pix = HealpixProc.vec2pixRing(order,
                new Vec3(v[0],v[1],v[2]));
            }
        } catch (Exception e) {}
        
        pixWritable.set(pix);
        result[0] = pixWritable;

        return result[0];
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "vec2pix(x, y, z, nest)";
    }
}
