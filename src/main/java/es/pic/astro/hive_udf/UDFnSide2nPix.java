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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import healpix.essentials.HealpixProc;
import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;

@Description(
    name="nside2npix",
    value="_FUNC_(nside) - return nPix of a nSide",
    extended="SELECT _FUNC_(3, 646, true, true) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFnSide2nPix extends GenericUDF {

    private final Object[] result = new Object[1];
    private final LongWritable pixWritable = new LongWritable();

    private Converter longconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("npix2nside() must take 1 argument: nside");
        }

        ObjectInspector longOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        longconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], longOI);

        result[0] = pixWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG); 
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        LongWritable argsw1 = new LongWritable();

        argsw1 = (LongWritable) longconverter.convert(arguments[0].get());

       if (argsw1 == null) {
            result[0] = null;
            return result;
        }

        long side = argsw1.get();
        long pix = 0;
        try {
            pix = HealpixBase.nside2Npix(side);    
        } catch(Exception e){}
        pixWritable.set(pix); 
        result[0] = pixWritable;

        return result[0];
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "nside2npix(nside)";
    }
}
