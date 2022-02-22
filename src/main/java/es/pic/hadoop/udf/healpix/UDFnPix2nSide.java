package es.pic.astro.hive_udf;

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
import org.apache.hadoop.io.LongWritable;
import healpix.essentials.HealpixBase;

@Description(
    name="npix2nside",
    value="_FUNC_(npix) - return nSide of a nPix",
    extended="SELECT _FUNC_(3, 646, true, true) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFnPix2nSide extends GenericUDF {

    private final Object[] result = new Object[1];
    private final LongWritable sideWritable = new LongWritable();

    private Converter longconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("npix2nside() must take 1 argument: npix");
        }

        ObjectInspector longOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        longconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], longOI);

        result[0] = sideWritable;

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

        long pix = argsw1.get();
        long side = 0;
        try {
            side = HealpixBase.npix2Nside(pix);    
        } catch(Exception e){}
        sideWritable.set(side); 
        result[0] = sideWritable;

        return result[0];
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "npix2nside(npix)";
    }
}
