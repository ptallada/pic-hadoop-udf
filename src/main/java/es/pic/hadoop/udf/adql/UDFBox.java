package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

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
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

    StructObjectInspector inputOI;

    Converter raConverter;
    Converter decConverter;
    Converter widthConverter;
    Converter heightConverter;

    Double raArg;
    Double decArg;
    Double widthArg;
    Double heightArg;

    double ra;
    double dec;
    double width;
    double height;

    Object blob;
    ADQLGeometry geom;

    S2LatLng center;
    S2LatLng size;
    S2LatLngRect box;

    List<Double> coords;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 3) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            inputOI = (StructObjectInspector) arguments[0];

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
            blob = arguments[0].get();

            if (blob == null) {
                return null;
            }

            geom = ADQLGeometry.fromBlob(blob, inputOI);

            if (!(geom instanceof ADQLPoint)) {
                throw new UDFArgumentTypeException(0,
                        String.format("Provided geometry is not a POINT, but a %s.", geom.getKind().name()));
            }

            ADQLPoint point = (ADQLPoint) geom;

            raArg = point.getRa();
            decArg = point.getDec();

            widthArg = (Double) widthConverter.convert(arguments[1].get());
            heightArg = (Double) heightConverter.convert(arguments[2].get());

        } else {
            raArg = (Double) raConverter.convert(arguments[0].get());
            decArg = (Double) decConverter.convert(arguments[1].get());
            widthArg = (Double) widthConverter.convert(arguments[2].get());
            heightArg = (Double) heightConverter.convert(arguments[3].get());
        }

        if (raArg == null || decArg == null || widthArg == null || heightArg == null) {
            return null;
        }

        center = S2LatLng.fromDegrees(decArg, raArg);
        size = S2LatLng.fromDegrees(heightArg, widthArg);
        box = S2LatLngRect.fromCenterSize(center, size);

        coords = new ArrayList<Double>();
        for (int i = 0; i < 4; i++) {
            center = box.getVertex(i);

            coords.add(new Double(center.lngDegrees()));
            coords.add(new Double(center.latDegrees()));
        }

        return new ADQLPolygon(coords).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("polygon", children);
    }
}
