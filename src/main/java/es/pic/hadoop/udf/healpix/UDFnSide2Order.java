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
import org.apache.hadoop.io.IntWritable;
import healpix.essentials.HealpixBase;

@Description(
    name="nside2order",
    value="_FUNC_(nside) - return Order of a nSide",
    extended="SELECT _FUNC_(2**15) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFnSide2Order extends GenericUDF {

    private final Object[] result = new Object[1];
    private final IntWritable orderWritable = new IntWritable();

    private Converter longconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("nside2order() must take 1 argument: nside");
        }

        ObjectInspector longOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        longconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], longOI);

        result[0] = orderWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT); 
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
        int order = 0;
        try {
            order = HealpixBase.nside2order(side);    
        } catch(Exception e){}
        orderWritable.set(order); 
        result[0] = orderWritable;

        return result[0];
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "nside2order(nside)";
    }
}
