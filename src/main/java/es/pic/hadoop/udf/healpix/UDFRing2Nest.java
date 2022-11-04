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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;

// @formatter:off
@Description(
    name = "ring2nest",
    value = "_FUNC_(order:tinyint, ipix_ring:bigint) -> ipix_nest:bigint",
    extended = "Convert pixel number from RING ordering to NESTED ordering."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFRing2Nest extends GenericUDF {
    Converter orderConverter;
    Converter ipixRingConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

    ByteWritable orderArg;
    LongWritable ipixRingArg;

    byte order;
    long ipixRing;
    long ipixNest;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("This function takes 2 arguments: order, ipix_ring");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        ipixRingConverter = ObjectInspectorConverters.getConverter(arguments[1], longOI);

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        ipixRingArg = (LongWritable) ipixRingConverter.convert(arguments[1].get());

        if (orderArg == null || ipixRingArg == null) {
            return null;
        }

        order = orderArg.get();
        ipixRing = ipixRingArg.get();

        try {
            ipixNest = HealpixProc.ring2nest(order, ipixRing);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(ipixNest);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("ring2nest", children);
    }
}
