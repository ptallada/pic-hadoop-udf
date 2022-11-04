package es.pic.hadoop.udf.healpix;

import java.util.ArrayList;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;

// @formatter:off
@Description(
    name = "neighbours",
    value = "_FUNC_(order:tinyint, ipix:bigint, [nest:bool=False]) -> array<ipix:bigint>",
    extended = "Return the nearest 8 pixels (SW, W, NW, N, NE, E, SE and S neighbours). If a neighbor does not exist the corresponding pixel number will be -1."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFNeighbours extends GenericUDF {
    Converter orderConverter;
    Converter ipixConverter;
    Converter nestConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    ByteWritable orderArg;
    LongWritable ipixArg;
    BooleanWritable nestArg = new BooleanWritable();

    byte order;
    long ipix;
    boolean nest;

    long[] neighbours;
    ArrayList<LongWritable> result = new ArrayList<LongWritable>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 2 arguments, not more than 3: order, pix, nest");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        ipixConverter = ObjectInspectorConverters.getConverter(arguments[1], longOI);

        if (arguments.length == 3) {
            nestConverter = ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(longOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        ipixArg = (LongWritable) ipixConverter.convert(arguments[1].get());

        if (arguments.length == 3) {
            nestArg = (BooleanWritable) nestConverter.convert(arguments[2].get());
        }
        if (orderArg == null || ipixArg == null) {
            return null;
        }

        order = orderArg.get();
        ipix = ipixArg.get();
        nest = nestArg.get();

        long[] neighbours;
        try {
            if (nest == true) {
                neighbours = HealpixProc.neighboursNest(order, ipix);
            } else {
                neighbours = HealpixProc.neighboursRing(order, ipix);
            }

            result.clear();
            for (int i = 0; i < 8; i++) {
                result.add(new LongWritable(neighbours[i]));
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%d, %d, %b)", order, ipix, nest);
    }
}
