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
    name = "intersects",
    value = "_FUNC_(geom1:ADQLGeometry, geom2:ADQLGeometry) -> intersect:boolean",
    extended = "Return true if both geometries overlap, false otherwise."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFIntersects extends GenericUDF {
    final static ObjectInspector booleanOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    StructObjectInspector inputOI1;
    StructObjectInspector inputOI2;

    Object blob1;
    Object blob2;
    ADQLGeometry geom1;
    ADQLGeometry geom2;
    ADQLGeometry tmp;

    double ra;
    double dec;
    double radius1;
    double radius2;

    S2Point point1;
    S2Point point2;
    S2Cap circle1;
    S2Cap circle2;
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
        geom2 = ADQLGeometry.fromBlob(blob2, inputOI2);

        if (geom1 instanceof ADQLRegion || geom2 instanceof ADQLRegion) {
            region1 = geom1.toRegion();
            region2 = geom2.toRegion();

            return new BooleanWritable(region1.intersects(region2));
        }

        if (geom1 instanceof ADQLPoint && geom2 instanceof ADQLPoint) {
            // POINT overlaps POINT
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();

            return new BooleanWritable(point1.equalsPoint(point2));
        }

        if ((geom1 instanceof ADQLPoint && geom2 instanceof ADQLCircle)
                || (geom1 instanceof ADQLCircle && geom2 instanceof ADQLPoint)) {

            if (geom1 instanceof ADQLCircle) {
                tmp = geom1;
                geom1 = geom2;
                geom2 = tmp;
            }

            // POINT overlaps CIRCLE
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(geom2.getCoord(2).get()));

            return new BooleanWritable(circle2.contains(point1));

        } else if ((geom1 instanceof ADQLPoint && geom2 instanceof ADQLPolygon)
                || (geom1 instanceof ADQLPolygon && geom2 instanceof ADQLPoint)) {

            if (geom1 instanceof ADQLPolygon) {
                tmp = geom1;
                geom1 = geom2;
                geom2 = tmp;
            }

            // POINT overlaps POLYGON
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
            // CIRCLE overlaps CIRCLE
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(geom2.getCoord(1).get(), geom2.getCoord(0).get()).toPoint();

            radius1 = geom1.getCoord(2).get();
            radius2 = geom2.getCoord(2).get();

            circle1 = S2Cap.fromAxisAngle(point1, S1Angle.degrees(radius1));
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(radius2));

            return new BooleanWritable(circle1.interiorIntersects(circle2));

        } else if ((geom1 instanceof ADQLCircle && geom2 instanceof ADQLPolygon)
                || (geom1 instanceof ADQLPolygon && geom2 instanceof ADQLCircle)) {

            if (geom1 instanceof ADQLPolygon) {
                tmp = geom1;
                geom1 = geom2;
                geom2 = tmp;
            }

            // CIRCLE overlaps POLYGON
            point1 = S2LatLng.fromDegrees(geom1.getCoord(1).get(), geom1.getCoord(0).get()).toPoint();
            radius1 = geom1.getCoord(2).get();

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < geom2.getNumCoords(); i += 2) {
                ra = geom2.getCoord(i).get();
                dec = geom2.getCoord(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1) || polygon2.getDistance(point1).degrees() <= radius1);

        } else { // (geom1 instanceof POLYGON && geom2 instanceof POLYGON) {
            // POLYGON overlaps POLYGON
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

            return new BooleanWritable(polygon2.intersects(polygon1));
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("intersects", children);
    }
}
