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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;

// @formatter:off
@Description(
    name = "nest2ring",
    value = "_FUNC_(order:tinyint, ipix_nest:bigint) -> ipix_ring:bigint",
    extended = "Convert pixel number from NESTED ordering to RING ordering."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFNest2Ring extends GenericUDF {
    Converter orderConverter;
    Converter ipixNestConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

    ByteWritable orderArg;
    LongWritable ipixNestArg;

    byte order;
    long ipixnest;
    long ipixring;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("This function takes 2 arguments: order, ipix_nest");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        ipixNestConverter = ObjectInspectorConverters.getConverter(arguments[1], longOI);

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        ipixNestArg = (LongWritable) ipixNestConverter.convert(arguments[1].get());

        if (orderArg == null || ipixNestArg == null) {
            return null;
        }

        order = orderArg.get();
        ipixnest = ipixNestArg.get();

        try {
            ipixring = HealpixProc.nest2ring(order, ipixnest);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(ipixring);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("nest2ring", children);
    }
}
