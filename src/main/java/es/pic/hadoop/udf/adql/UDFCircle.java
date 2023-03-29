package es.pic.hadoop.udf.adql;

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "circle",
    value = "_FUNC_(ra:double, dec:double, radius:double) | _FUNC_(pt:point, radius:double) -> circle:ADQLGeometry",
    extended = "Construct an ADQL circle from center sky coordinates and a radius."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFCircle extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    StructObjectInspector inputOI;

    Converter raConverter;
    Converter decConverter;
    Converter radiusConverter;

    DoubleWritable raArg;
    DoubleWritable decArg;
    DoubleWritable radiusArg;

    Object blob;
    ADQLGeometry geom;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {

            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            inputOI = (StructObjectInspector) arguments[0];

            radiusConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);

        } else if (arguments.length == 3) {
            raConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
            decConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
            radiusConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);

        } else {
            throw new UDFArgumentLengthException(
                    "This function takes 2 or 3 arguments: either (point, radius) or (ra, dec, radius)");
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length == 2) {
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
            radiusArg = (DoubleWritable) radiusConverter.convert(arguments[1].get());
        } else {
            raArg = (DoubleWritable) raConverter.convert(arguments[0].get());
            decArg = (DoubleWritable) decConverter.convert(arguments[1].get());
            radiusArg = (DoubleWritable) radiusConverter.convert(arguments[2].get());
        }

        if (raArg == null || decArg == null || radiusArg == null) {
            return null;
        }

        return new ADQLCircle(raArg, decArg, radiusArg).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("circle", children);
    }
}
