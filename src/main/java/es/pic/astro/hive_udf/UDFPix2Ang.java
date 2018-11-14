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
    name="pix2ang",
    value="_FUNC_(order, pix, nest, lonlat) - Returns a struct with the angles of the pix",
    extended="SELECT _FUNC_(3, 646, true, true) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFPix2Ang extends GenericUDF {

    private final Object[] result = new Object[2];
    private final DoubleWritable phiWritable = new DoubleWritable();
    private final DoubleWritable thetaWritable = new DoubleWritable();

    private Converter[] intconverter = new Converter[1];
    private Converter[] longconverter = new Converter[1];
    private Converter[] booleanconverter = new Converter[2];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length<2|| arguments.length>4) {
            throw new UDFArgumentLengthException("pix2ang() takes at least 2 arguments, no more than 4: order, pix, nest, lonlat");
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
        } else if (arguments.length == 4) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[2], boolOI);
            booleanconverter[1] = (Converter) ObjectInspectorConverters.getConverter(arguments[3], boolOI);
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
        IntWritable argsw1 = new IntWritable();
        LongWritable argsw2 = new LongWritable();

        BooleanWritable argsw3 = new BooleanWritable(false);
        BooleanWritable argsw4 = new BooleanWritable(false);

        argsw1 = (IntWritable) intconverter[0].convert(arguments[0].get());
        argsw2 = (LongWritable) longconverter[0].convert(arguments[1].get());

        if (arguments.length == 3) {
            argsw3 = (BooleanWritable) booleanconverter[0].convert(arguments[2].get());
        } else if (arguments.length == 4) {
            argsw3 = (BooleanWritable) booleanconverter[0].convert(arguments[2].get());
            argsw4 = (BooleanWritable) booleanconverter[1].convert(arguments[3].get());
        }
        if (argsw1 == null || argsw2 == null) {
            result[0] = null;
            result[1] = null;
            return result;
        }

        int order = argsw1.get();
        long pix = argsw2.get();
        boolean nest = argsw3.get();
        boolean lonlat = argsw4.get();
        Pointing res = new Pointing();

        try {
            if (nest == true) {     
                res = HealpixProc.pix2angNest(order, pix); 
            } else {
                res = HealpixProc.pix2angRing(order, pix);
            }
        } catch(Exception e){}
        
        if (lonlat) {
            thetaWritable.set(90 - res.theta * 180 /  Math.PI);
            phiWritable.set(res.phi * 180 / Math.PI);
	    result[0] = phiWritable;
            result[1] = thetaWritable;
        } else {
            thetaWritable.set(res.theta);
            phiWritable.set(res.phi);
            result[0] = thetaWritable;
            result[1] = phiWritable;
	}

        return result;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "pix2ang(order, hpix, nest, lonlat)";
    }
}
