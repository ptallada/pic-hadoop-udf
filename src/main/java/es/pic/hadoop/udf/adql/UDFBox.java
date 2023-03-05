package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

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

    Converter raConverter;
    Converter decConverter;
    Converter widthConverter;
    Converter heightConverter;

    DoubleWritable raArg;
    DoubleWritable decArg;
    DoubleWritable widthArg;
    DoubleWritable heightArg;

    double ra;
    double dec;
    double width;
    double height;

    Object geom;
    ADQLGeometry.Kind kind;

    S2LatLng center;
    S2LatLng size;
    S2LatLngRect box;

    List<DoubleWritable> coords;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 3) {
            if (arguments[0] != ADQLGeometry.OI) {
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

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length == 3) {
            geom = arguments[0].get();

            if (geom == null) {
                return null;
            }

            kind = ADQLGeometry.getTag(geom);

            if (kind != ADQLGeometry.Kind.POINT) {
                throw new UDFArgumentTypeException(0,
                        String.format("Provided geometry is not a POINT, but a %s.", kind.name()));
            }

            List<DoubleWritable> coords = ADQLGeometry.getCoords(geom);

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

        center = S2LatLng.fromDegrees(dec, ra);
        size = S2LatLng.fromDegrees(height, width);
        box = S2LatLngRect.fromCenterSize(center, size);

        coords = new ArrayList<DoubleWritable>();
        for (int i = 0; i < 4; i++) {
            center = box.getVertex(i);

            coords.add(new DoubleWritable(center.lngDegrees()));
            coords.add(new DoubleWritable(center.latDegrees()));
        }

        return new ADQLPolygon(coords).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("polygon", children);
    }
}
