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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

// @formatter:off
@Description(
    name = "region",
    value = "_FUNC_(geom:ADQLGeometry, [order:tinyint=10]) -> region:ADQLGeometry",
    extended = "Construct an ADQL region with specified resolution from an arbitrary ADQLGeometry"
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFRegion extends GenericUDF {
    Converter orderConverter;

    final static byte DEFAULT_ORDER = 10;
    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;

    Object geom;
    ADQLGeometry.Kind kind;
    ByteWritable orderArg;

    byte order;

    Moc moc;
    Object region;

    public static Moc fromGeometry(Object geom) throws HiveException {
        return fromGeometry(geom, DEFAULT_ORDER);
    }

    public static Moc fromGeometry(Object geom, byte order) throws HiveException {
        double theta;
        double phi;
        double radius;

        Pointing pt;
        RangeSet rs;

        ADQLGeometry.Kind kind = ADQLGeometry.Kind.valueOfTag(ADQLGeometry.OI.getTag(geom));

        if (kind == ADQLGeometry.Kind.REGION) {
            BytesWritable bytes = (BytesWritable) ADQLGeometry.OI.getField(geom);

            try {
                return Moc.fromCompressed(bytes.getBytes());
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) ADQLGeometry.OI.getField(geom);

        switch (kind) {
        case CIRCLE:
            theta = Math.toRadians(90 - coords.get(1).get());
            phi = Math.toRadians(coords.get(0).get());
            radius = Math.toRadians(coords.get(2).get());

            pt = new Pointing(theta, phi);
            try {
                rs = HealpixProc.queryDiscNest(order, pt, radius);
            } catch (Exception e) {
                throw new HiveException(e);
            }
            break;

        case POLYGON:
            ArrayList<Pointing> vertices = new ArrayList<Pointing>();
            for (int i = 0; i < coords.size(); i += 2) {
                theta = Math.toRadians(90 - coords.get(i + 1).get());
                phi = Math.toRadians(coords.get(i).get());
                pt = new Pointing(theta, phi);
                vertices.add(pt);
            }
            Pointing[] pts = new Pointing[vertices.size()];
            vertices.toArray(pts);

            // FIXME: HEALPix only works with convex polygons, need to implement ear-clipping
            try {
                rs = HealpixProc.queryPolygonNest(order, pts);
            } catch (Exception e) {
                throw new HiveException(e);
            }
            break;

        default: // POINT
            throw new UDFArgumentTypeException(0, "Geometry cannot be a POINT.");
        }

        return new Moc(rs, order);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 0 || arguments.length > 2) {
            throw new UDFArgumentLengthException("This function takes 2 arguments at most: geometry, [order]");
        }

        if (arguments[0] != ADQLGeometry.OI) {
            throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
        }

        if (arguments.length == 2) {
            orderConverter = ObjectInspectorConverters.getConverter(arguments[1], byteOI);
        } else {
            // Set NSIDE=1024 as default value
            order = DEFAULT_ORDER;
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        geom = arguments[0].get();

        if (geom == null) {
            return null;
        }

        if (arguments.length == 2) {
            orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());

            if (orderArg == null) {
                return null;
            } else {
                order = orderArg.get();
            }
        }

        Moc moc = fromGeometry(geom, order);

        return new ADQLRegion(moc).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("region", children);
    }
}
