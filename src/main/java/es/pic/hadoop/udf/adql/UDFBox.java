package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

// @formatter:off
@Description(
    name = "box",
    value = "_FUNC_(ra:double, dec:double, width:double, height:double) | _FUNC_(pt:point, width:double, height:double) -> polygon:ADQLGeometry",
    extended = "Construct an ADQL box from the sky coordinates of its center, a width and a height."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFBox extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    Converter raConverter;
    Converter decConverter;
    Converter widthConverter;
    Converter heightConverter;

    DoubleWritable raArg;
    DoubleWritable decArg;
    DoubleWritable widthArg;
    DoubleWritable heightArg;
    Object geom;
    ADQLGeometry.Kind kind;

    double ra;
    double dec;
    double width;
    double height;

    Object polygon;
    List<DoubleWritable> value;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 3) {
            if (arguments[0] != geomOI) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }

            widthConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
            heightConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);

        } else if (arguments.length == 4) {
            raConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
            decConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
            widthConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);
            heightConverter = ObjectInspectorConverters.getConverter(arguments[3], doubleOI);

        } else {
            throw new UDFArgumentLengthException(
                    "This function takes 3 or 4 arguments: either (point, width, height) or (ra, dec, width, height)");
        }

        return geomOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length == 3) {
            geom = arguments[0].get();

            if (geom == null) {
                return null;
            }

            kind = ADQLGeometry.Kind.valueOfTag(geomOI.getTag(geom));

            if (kind != ADQLGeometry.Kind.POINT) {
                throw new UDFArgumentTypeException(0,
                        String.format("Provided geometry is not a POINT, but a %s.", kind.name()));
            }

            @SuppressWarnings("unchecked")
            List<DoubleWritable> coords = (List<DoubleWritable>) geomOI.getField(geom);

            raArg = coords.get(0);
            decArg = coords.get(1);

            widthArg = (DoubleWritable) widthConverter.convert(arguments[1].get());
            heightArg = (DoubleWritable) heightConverter.convert(arguments[2].get());

        } else {
            raArg = (DoubleWritable) raConverter.convert(arguments[0].get());
            decArg = (DoubleWritable) decConverter.convert(arguments[1].get());
            widthArg = (DoubleWritable) widthConverter.convert(arguments[2].get());
            heightArg = (DoubleWritable) heightConverter.convert(arguments[3].get());
        }

        if (raArg == null || decArg == null || widthArg == null || heightArg == null) {
            return null;
        }

        ra = raArg.get();
        dec = decArg.get();
        width = widthArg.get();
        height = heightArg.get();

        // Build CCW vertex list, interior is on the left.
        value = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(ra - width / 2), new DoubleWritable(dec - height / 2),
                new DoubleWritable(ra + width / 2), new DoubleWritable(dec - height / 2),
                new DoubleWritable(ra + width / 2), new DoubleWritable(dec + height / 2),
                new DoubleWritable(ra - width / 2), new DoubleWritable(dec + height / 2),
        });

        polygon = geomOI.create();
        geomOI.setFieldAndTag(polygon, value, ADQLGeometry.Kind.POLYGON.tag);

        return polygon;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("polygon", children);
    }
}
