package es.pic.astro.hive_udf;

import java.lang.Math;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

@Description(
    name="ang2pix",
    value="_FUNC_(order, theta, phi, nest, lonlat) - Returns the pix of the angles",
    extended="SELECT _FUNC_(10, 1.27, 1.34, true,true) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFAng2Pix extends GenericUDF {

    private final Object[] result = new Object[1];
    private final LongWritable pixWritable = new LongWritable();    

    private Converter intconverter;
    private Converter[] doubleconverter = new Converter[2];
    private Converter[] booleanconverter = new Converter[2];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 3 || arguments.length > 5) {
            throw new UDFArgumentLengthException("ang2pix() takes at least 3 arguments, no more than 5: order, theta, phi, nest, lonlat");
        }

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

        intconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], intOI);
        doubleconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        doubleconverter[1] = (Converter) ObjectInspectorConverters.getConverter(arguments[2], doubleOI);

        if (arguments.length == 4) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.
                getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.
                getConverter(arguments[3], boolOI);
        } else if (arguments.length == 5) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.
                getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.
                getConverter(arguments[3], boolOI);
            booleanconverter[1] = (Converter) ObjectInspectorConverters.
                getConverter(arguments[4], boolOI);
        }

        result[0] = pixWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        IntWritable argsw1 = new IntWritable();
        DoubleWritable[] argsw2 = new DoubleWritable[2];
        BooleanWritable argsw4 = new BooleanWritable(false);
        BooleanWritable argsw5 = new BooleanWritable(false);

        argsw1 = (IntWritable) intconverter.convert(arguments[0].get());
        argsw2[0] = (DoubleWritable) doubleconverter[0].convert(arguments[1].get());
        argsw2[1] = (DoubleWritable) doubleconverter[1].convert(arguments[2].get());

        if (arguments.length == 4) {
            argsw4 = (BooleanWritable) booleanconverter[0].convert(arguments[3].get());
        } else if (arguments.length == 5) {
            argsw4 = (BooleanWritable) booleanconverter[0].convert(arguments[3].get());
            argsw5 = (BooleanWritable) booleanconverter[1].convert(arguments[4].get());
        }
        if (argsw1 == null || argsw2[0] == null || argsw2[1] == null){
            result[0] = null;
            return result;
        }

        int order = argsw1.get();
        double theta = argsw2[0].get();
        double phi = argsw2[1].get();
        boolean nest = argsw4.get();
        boolean lonlat = argsw5.get();
        long pix = 0;
        
        if (lonlat) {
            theta = theta * Math.PI / 180;
            phi = Math.PI / 2 - phi * Math.PI / 180;
            double aux = theta;
            theta = phi;
            phi = aux;
        }

        try {
            if (nest) {
                pix = HealpixProc.ang2pixNest(order,
                    new Pointing(theta, phi));
            } else {
                pix = HealpixProc.ang2pixRing(order,
                    new Pointing(theta,phi));
            }
        } catch (Exception e) {}
        
        pixWritable.set(pix);
        result[0] = pixWritable;

        return result[0];
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "ang2pix(order, theta, phi, nest, lonlat)";
    }
}
