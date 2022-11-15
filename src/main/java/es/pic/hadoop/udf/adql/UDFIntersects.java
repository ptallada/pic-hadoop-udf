package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

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
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    Object geom1;
    Object geom2;
    ADQLGeometry.Kind kind1;
    ADQLGeometry.Kind kind2;

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

    List<DoubleWritable> tmp;
    List<S2Point> vertices;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (arguments[0] != geomOI) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            if (arguments[1] != geomOI) {
                throw new UDFArgumentTypeException(0, "Second argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes 2 arguments: geom1, geom2");
        }

        return booleanOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        geom1 = arguments[0].get();
        geom2 = arguments[1].get();

        if (geom1 == null || geom2 == null) {
            return null;
        }

        kind1 = ADQLGeometry.Kind.valueOfTag(geomOI.getTag(geom1));
        kind2 = ADQLGeometry.Kind.valueOfTag(geomOI.getTag(geom2));

        if (kind1 == ADQLGeometry.Kind.REGION || kind2 == ADQLGeometry.Kind.REGION) {
            throw new UnsupportedOperationException("Operations on regions are not yet supported");
        }

        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords1 = (List<DoubleWritable>) geomOI.getField(geom1);
        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords2 = (List<DoubleWritable>) geomOI.getField(geom2);

        if (kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.POINT) {
            // POINT overlaps POINT
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();

            return new BooleanWritable(point1.equalsPoint(point2));

        }
        if ((kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.CIRCLE)
                || (kind1 == ADQLGeometry.Kind.CIRCLE && kind2 == ADQLGeometry.Kind.POINT)) {

            if (kind1 == ADQLGeometry.Kind.CIRCLE) {
                tmp = coords1;
                coords1 = coords2;
                coords2 = tmp;
            }

            // POINT overlaps CIRCLE
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(coords2.get(2).get()));

            return new BooleanWritable(circle2.contains(point1));

        } else if ((kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.POLYGON)
                || (kind1 == ADQLGeometry.Kind.POLYGON && kind2 == ADQLGeometry.Kind.POINT)) {

            if (kind1 == ADQLGeometry.Kind.POLYGON) {
                tmp = coords1;
                coords1 = coords2;
                coords2 = tmp;
            }

            // POINT overlaps POLYGON
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();

            double ra;
            double dec;

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords2.size(); i += 2) {
                ra = coords2.get(i).get();
                dec = coords2.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1));

        } else if (kind1 == ADQLGeometry.Kind.CIRCLE && kind2 == ADQLGeometry.Kind.CIRCLE) {
            // CIRCLE overlaps CIRCLE
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();

            radius1 = coords1.get(2).get();
            radius2 = coords2.get(2).get();

            circle1 = S2Cap.fromAxisAngle(point1, S1Angle.degrees(radius1));
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(radius2));

            return new BooleanWritable(circle1.interiorIntersects(circle2));

        } else if ((kind1 == ADQLGeometry.Kind.CIRCLE && kind2 == ADQLGeometry.Kind.POLYGON)
                || (kind1 == ADQLGeometry.Kind.POLYGON && kind2 == ADQLGeometry.Kind.CIRCLE)) {

            if (kind1 == ADQLGeometry.Kind.POLYGON) {
                tmp = coords1;
                coords1 = coords2;
                coords2 = tmp;
            }

            // CIRCLE overlaps POLYGON
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            double radius = coords1.get(2).get();

            double ra;
            double dec;

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords2.size(); i += 2) {
                ra = coords2.get(i).get();
                dec = coords2.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1) || polygon2.getDistance(point1).degrees() <= radius);

        } else { // (kind1 == ADQLGeometry.Kind.POLYGON && kind2 == ADQLGeometry.Kind.POLYGON)
            // POLYGON overlaps POLYGON
            double ra;
            double dec;

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords1.size(); i += 2) {
                ra = coords1.get(i).get();
                dec = coords1.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }
            polygon1 = new S2Loop(vertices);

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords2.size(); i += 2) {
                ra = coords2.get(i).get();
                dec = coords2.get(i + 1).get();
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
