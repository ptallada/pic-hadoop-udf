package es.pic.hadoop.udf.healpix;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixBase;

// @formatter:off
@Description(
    name = "order2npix",
    value = "_FUNC_(order:tinyint) -> npix:bigint",
    extended = "Return the number of pixels for a given resolution order."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFOrder2Npix extends GenericUDF {
    Converter orderConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BYTE);
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);

    IntWritable orderArg;

    int order;
    long npix;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: order");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (IntWritable) orderConverter.convert(arguments[0].get());

        if (orderArg == null) {
            return null;
        }

        order = orderArg.get();

        try {
            npix = HealpixBase.nside2Npix(order);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(npix);
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%d)", order);
    }
}
