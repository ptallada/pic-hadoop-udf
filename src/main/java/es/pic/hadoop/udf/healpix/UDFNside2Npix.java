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
    name = "nside2npix",
    value = "_FUNC_(nside:int) -> npix:bigint",
    extended = "Return the number of pixels for a given nside."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFNside2Npix extends GenericUDF {
    Converter nsideConverter;

    final static ObjectInspector intOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

    IntWritable nsideArg;

    int nside;
    long npix;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: nside");
        }

        nsideConverter = ObjectInspectorConverters.getConverter(arguments[0], intOI);

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        nsideArg = (IntWritable) nsideConverter.convert(arguments[0].get());

        if (nsideArg == null) {
            return null;
        }

        nside = nsideArg.get();

        try {
            npix = HealpixBase.nside2Npix(nside);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(npix);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("nside2npix", children);
    }
}
