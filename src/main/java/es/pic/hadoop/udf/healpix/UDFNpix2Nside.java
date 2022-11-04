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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixBase;

// @formatter:off
@Description(
    name = "npix2nside",
    value = "_FUNC_(npix:bigint) -> nside:int",
    extended = "Return the NSIDE parameter for the given number of pixels."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFNpix2Nside extends GenericUDF {
    Converter npixConverter;

    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    final static ObjectInspector intOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;

    LongWritable npixArg;

    long npix;
    long nside;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: npix");
        }

        npixConverter = ObjectInspectorConverters.getConverter(arguments[0], longOI);

        return intOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        npixArg = (LongWritable) npixConverter.convert(arguments[0].get());

        if (npixArg == null) {
            return null;
        }

        npix = npixArg.get();

        try {
            nside = HealpixBase.npix2Nside(npix);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new IntWritable(Math.toIntExact(nside));
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("npix2nside", children);
    }
}
