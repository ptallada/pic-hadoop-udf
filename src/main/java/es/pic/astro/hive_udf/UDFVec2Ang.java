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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

@Description(
    name="vec2ang",
    value="_FUNC_(x, y, z, lonlat) - Returns a struct with the angles of a vector",
    extended="SELECT _FUNC_(3, 1.02,2.35, true) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFVec2Ang extends GenericUDF {

    private final Object[] result = new Object[2];
    private final DoubleWritable phiWritable = new DoubleWritable();
    private final DoubleWritable thetaWritable = new DoubleWritable();

    private Converter[] doubleconverter = new Converter[3];
    private Converter[] booleanconverter = new Converter[1];

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length<3|| arguments.length>4) {
            throw new UDFArgumentLengthException("vec2ang() takes at least 3 arguments, no more than 4: v1, v2, v3, lonlat");
        }
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

        for (int i = 0; i < 3; i ++) {
            doubleconverter[i] = (Converter) ObjectInspectorConverters.getConverter(arguments[i], doubleOI);
        }
        if (arguments.length == 4) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[3], boolOI);
        }

        fieldNames.add("theta_lat");
        fieldNames.add("phi_lon");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        result[1] = phiWritable;
        result[0] = thetaWritable;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DoubleWritable[] argsw1 = new DoubleWritable[3];
        BooleanWritable argsw4 = new BooleanWritable(false);

        for( int i = 0; i < 3; i ++) {
            argsw1[i] = (DoubleWritable) doubleconverter[i].convert(arguments[i].get());
        }

        if (arguments.length == 4) {
            argsw4 = (BooleanWritable) booleanconverter[0].convert(arguments[3].get());
        }

        if (argsw1[0] == null || argsw1[1] == null || argsw1[2] == null){
            result[0] = null;
            result[1] = null;
            return result;
        }

        double[] v = new double[3];

        for ( int i = 0 ; i < 3; i ++) {
            v[i] = argsw1[i].get();
        }
        boolean lonlat = argsw4.get();
        
        double dnorm = Math.sqrt(Math.pow(v[0],2) + Math.pow(v[1],2) +Math.pow(v[2],2));
        thetaWritable.set(Math.acos(v[2] / dnorm));
        phiWritable.set(Math.atan2(v[1],v[0]));

        if (phiWritable.get() < 0) {
            phiWritable.set(phiWritable.get() + 2 * Math.PI);
        }  
        if (lonlat) {
            thetaWritable.set(90 - thetaWritable.get() * 180 / Math.PI);
            phiWritable.set( phiWritable.get() * 180 / Math.PI);
            result[0] = phiWritable;
            result[1] = thetaWritable;
        } else {
            result[0] = thetaWritable;
            result[1] = phiWritable;
        }

        return result;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "vec2ang(x, y, z, lonlat)";
    }
}
