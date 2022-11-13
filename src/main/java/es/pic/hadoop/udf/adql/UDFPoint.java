package es.pic.hadoop.udf.adql;

import java.util.Arrays;
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
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

// @formatter:off
@Description(
    name = "point",
    value = "_FUNC_(ra:double, dec:double) | _FUNC_(ipix:long) -> point:ADQLGeometry",
    extended = "Construct an ADQL point type from sky coordinates."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFPoint extends GenericUDF {
    final static int MAX_ORDER = 29;

    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    Converter raConverter;
    Converter decConverter;
    Converter ipixConverter;

    DoubleWritable raArg;
    DoubleWritable decArg;
    LongWritable ipixArg;

    boolean is_pixel;
    Pointing pt;

    Object geom;
    List<DoubleWritable> value;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            is_pixel = true;
            ipixConverter = ObjectInspectorConverters.getConverter(arguments[0], longOI);

        } else if (arguments.length == 2) {
            is_pixel = false;
            raConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
            decConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);

        } else {
            throw new UDFArgumentLengthException("This function takes 1 or 2 arguments: either (ipix) or (ra, dec)");
        }

        return geomOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (is_pixel) {
            ipixArg = (LongWritable) ipixConverter.convert(arguments[0].get());

            if (ipixArg == null) {
                return null;
            }

            try {
                pt = HealpixProc.pix2angNest(MAX_ORDER, ipixArg.get());
            } catch (Exception e) {
                throw new HiveException(e);
            }

            raArg = new DoubleWritable(pt.phi * 180 / Math.PI);
            decArg = new DoubleWritable(90 - pt.theta * 180 / Math.PI);

        } else {
            raArg = (DoubleWritable) raConverter.convert(arguments[0].get());
            decArg = (DoubleWritable) decConverter.convert(arguments[1].get());

            if (raArg == null || decArg == null) {
                return null;
            }
        }

        value = Arrays.asList(new DoubleWritable[] {
                raArg, decArg
        });

        geom = geomOI.create();
        geomOI.setFieldAndTag(geom, value, ADQLGeometry.Kind.POINT.tag);

        return geom;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("point", children);
    }
}
