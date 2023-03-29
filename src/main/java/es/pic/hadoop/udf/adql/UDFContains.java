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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

// @formatter:off
@Description(
    name = "contains",
    value = "_FUNC_(geom1:ADQLGeometry, geom2:ADQLGeometry) -> is_contained:boolean",
    extended = "Return true if the first geometry is fully contained within the other, false otherwise."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFContains extends GenericUDF {
    final static ObjectInspector booleanOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    StructObjectInspector inputOI1;
    StructObjectInspector inputOI2;

    Object prev1;
    Object prev2;
    Object blob1;
    Object blob2;
    ADQLGeometry geom1;
    ADQLGeometry geom2;

    double ra;
    double dec;

    S2Point point1;
    S2Point point2;
    S2Cap circle1;
    S2Cap circle2;
    S1Angle radius;
    S2Loop polygon1;
    S2Loop polygon2;
    ADQLRegion region1;
    ADQLRegion region2;

    List<S2Point> vertices;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            inputOI1 = (StructObjectInspector) arguments[0];
            if (!ObjectInspectorUtils.compareTypes(arguments[1], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(1, "Second argument has to be of ADQL geometry type.");
            }
            inputOI2 = (StructObjectInspector) arguments[1];
        } else {
            throw new UDFArgumentLengthException("This function takes 2 arguments: geom1, geom2");
        }

        return booleanOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        blob1 = arguments[0].get();
        blob2 = arguments[1].get();

        if (blob1 == null || blob2 == null) {
            return null;
        }

        geom1 = ADQLGeometry.fromBlob(blob1, inputOI1);
        if (!blob2.equals(prev2)) {
            // TODO: Maybe do a special case for point-in-region where we do not parse the entire LongBuffer, and do a binary-search within.
            geom2 = ADQLGeometry.fromBlob(blob2, inputOI2);
        }

        prev1 = blob1;
        prev2 = blob2;

        if (geom2 instanceof ADQLPoint) {
            throw new UDFArgumentTypeException(1, "Second geometry cannot be a POINT.");
        }

        if (geom1 instanceof ADQLRegion || geom2 instanceof ADQLRegion) {
            // REGION combined with POINT, CIRCLE, POLYGON or another REGION
            region1 = geom1.toRegion();
            region2 = geom2.toRegion();

            return new BooleanWritable(region2.contains(region1));
        }

        if (geom1 instanceof ADQLPoint && geom2 instanceof ADQLCircle) {
            // POINT inside CIRCLE
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(geom2.getCoord(2).get()));

            return new BooleanWritable(circle2.contains(point1));
        } else if (geom1 instanceof ADQLPoint && geom2 instanceof ADQLPolygon) {
            // POINT inside POLYGON
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom2.getNumCoords(); i += 2) {
                ra = geom2.getCoord(i).get();
                dec = geom2.getCoord(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1));

        } else if (geom1 instanceof ADQLCircle && geom2 instanceof ADQLCircle) {
            // CIRCLE inside CIRCLE
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();

            circle1 = S2Cap.fromAxisAngle(point1, S1Angle.degrees(geom1.getCoord(2).get()));
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(geom2.getCoord(2).get()));

            return new BooleanWritable(circle2.contains(circle1));

        } else if (geom1 instanceof ADQLCircle && geom2 instanceof ADQLPolygon) {
            // CIRCLE inside POLYGON
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            radius = S1Angle.degrees(geom1.getCoord(2).get());

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom2.getNumCoords(); i += 2) {
                ra = geom2.getCoord(i).get();
                dec = geom2.getCoord(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1) && polygon2.getDistance(point1).greaterThan(radius));

        } else if (geom1 instanceof ADQLPolygon && geom2 instanceof ADQLCircle) {
            // POLYGON inside CIRCLE
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();
            radius = S1Angle.degrees(geom2.getCoord(2).get());
            circle2 = S2Cap.fromAxisAngle(point2, radius);

            // Circle must contain every vertex
            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom1.getNumCoords(); i += 2) {
                ra = geom1.getCoord(i).get();
                dec = geom1.getCoord(i + 1).get();

                point1 = S2LatLng.fromDegrees(dec, ra).toPoint();
                if (!circle2.contains(point1)) {
                    return new BooleanWritable(false);
                } else {
                    vertices.add(point1);
                }
            }
            polygon2 = new S2Loop(vertices);

            // And polygon cannot overlap with circle's complement.
            circle2 = circle2.complement();
            point2 = circle2.axis();
            radius = circle2.angle();
            if (polygon2.contains(point2) || polygon2.getDistance(point2).lessOrEquals(radius)) {
                return new BooleanWritable(false);
            } else {
                return new BooleanWritable(true);
            }

        } else { // (geom1 instanceof ADQLPolygon && geom2 instanceof ADQLPolygon)
            // POLYGON inside POLYGON
            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom1.getNumCoords(); i += 2) {
                ra = geom1.getCoord(i).get();
                dec = geom1.getCoord(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }
            polygon1 = new S2Loop(vertices);

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom2.getNumCoords(); i += 2) {
                ra = geom2.getCoord(i).get();
                dec = geom2.getCoord(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }
            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.containsNested(polygon1));
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("contains", children);
    }
}
