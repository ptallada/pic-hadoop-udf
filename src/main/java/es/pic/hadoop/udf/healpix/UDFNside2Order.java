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
import org.apache.hadoop.io.IntWritable;

import healpix.essentials.HealpixBase;

// @formatter:off
@Description(
    name = "nside2order",
    value = "_FUNC_(nside:int) -> order:tinyint",
    extended = "Give the resolution order for a given nside."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFNside2Order extends GenericUDF {
    Converter nsideConverter;

    final static ObjectInspector intOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;

    IntWritable nsideArg;

    int nside;
    int order;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: nside");
        }

        nsideConverter = ObjectInspectorConverters.getConverter(arguments[0], intOI);

        return byteOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        nsideArg = (IntWritable) nsideConverter.convert(arguments[0].get());

        if (nsideArg == null) {
            return null;
        }

        nside = nsideArg.get();

        try {
            order = HealpixBase.nside2order(nside);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new ByteWritable((byte) order);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("nside2order", children);
    }
}
