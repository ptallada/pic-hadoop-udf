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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import healpix.essentials.HealpixBase;
import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;

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

    Object geom1;
    Object geom2;
    ADQLGeometry.Kind kind1;
    ADQLGeometry.Kind kind2;

    double ra;
    double dec;
    double theta;
    double phi;
    long ipix;

    S2Point point1;
    S2Point point2;
    S2Cap circle1;
    S2Cap circle2;
    S1Angle radius;
    S2Loop polygon1;
    S2Loop polygon2;
    Moc region1;
    Moc region2;

    List<S2Point> vertices;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (arguments[0] != ADQLGeometry.OI) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            if (arguments[1] != ADQLGeometry.OI) {
                throw new UDFArgumentTypeException(1, "Second argument has to be of ADQL geometry type.");
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

        kind1 = ADQLGeometry.Kind.valueOfTag(ADQLGeometry.OI.getTag(geom1));
        kind2 = ADQLGeometry.Kind.valueOfTag(ADQLGeometry.OI.getTag(geom2));

        if (kind2 == ADQLGeometry.Kind.POINT) {
            throw new UDFArgumentTypeException(1, "Second geometry cannot be a POINT.");

        } else if (kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.REGION) {
            // POINT inside REGION
            @SuppressWarnings("unchecked")
            List<DoubleWritable> coords1 = (List<DoubleWritable>) ADQLGeometry.OI.getField(geom1);

            theta = Math.toRadians(90 - coords1.get(1).get());
            phi = Math.toRadians(coords1.get(0).get());

            try {
                ipix = HealpixProc.ang2pixNest(HealpixBase.order_max, new Pointing(theta, phi));
            } catch (Exception e) {
                throw new HiveException(e);
            }

            region1 = new Moc();
            region1.addPixel(HealpixBase.order_max, ipix);

            region2 = UDFRegion.fromGeometry(geom2);

            return new BooleanWritable(region2.contains(region1));

        } else if (kind1 == ADQLGeometry.Kind.REGION || kind2 == ADQLGeometry.Kind.REGION) {
            // REGION combined with CIRCLE, POLYGON or REGION, in any order
            Moc region1 = UDFRegion.fromGeometry(geom1);
            Moc region2 = UDFRegion.fromGeometry(geom2);

            return new BooleanWritable(region2.contains(region1));

        }

        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords1 = (List<DoubleWritable>) ADQLGeometry.OI.getField(geom1);
        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords2 = (List<DoubleWritable>) ADQLGeometry.OI.getField(geom2);

        if (kind2 == ADQLGeometry.Kind.POINT) {
            throw new UDFArgumentTypeException(1, "Second geometry cannot be a POINT.");

        } else if (kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.CIRCLE) {
            // POINT inside CIRCLE
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(coords2.get(2).get()));

            return new BooleanWritable(circle2.contains(point1));

        } else if (kind1 == ADQLGeometry.Kind.POINT && kind2 == ADQLGeometry.Kind.POLYGON) {
            // POINT inside POLYGON
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords2.size(); i += 2) {
                ra = coords2.get(i).get();
                dec = coords2.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1));

        } else if (kind1 == ADQLGeometry.Kind.CIRCLE && kind2 == ADQLGeometry.Kind.CIRCLE) {
            // CIRCLE inside CIRCLE
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();

            circle1 = S2Cap.fromAxisAngle(point1, S1Angle.degrees(coords1.get(2).get()));
            circle2 = S2Cap.fromAxisAngle(point2, S1Angle.degrees(coords2.get(2).get()));

            return new BooleanWritable(circle2.contains(circle1));

        } else if (kind1 == ADQLGeometry.Kind.CIRCLE && kind2 == ADQLGeometry.Kind.POLYGON) {
            // CIRCLE inside POLYGON
            point1 = S2LatLng.fromDegrees(coords1.get(1).get(), coords1.get(0).get()).toPoint();
            radius = S1Angle.degrees(coords1.get(2).get());

            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords2.size(); i += 2) {
                ra = coords2.get(i).get();
                dec = coords2.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            polygon2 = new S2Loop(vertices);

            return new BooleanWritable(polygon2.contains(point1) && polygon2.getDistance(point1).greaterThan(radius));

        } else if (kind1 == ADQLGeometry.Kind.POLYGON && kind2 == ADQLGeometry.Kind.CIRCLE) {
            // POLYGON inside CIRCLE
            point2 = S2LatLng.fromDegrees(coords2.get(1).get(), coords2.get(0).get()).toPoint();
            radius = S1Angle.degrees(coords2.get(2).get());
            circle2 = S2Cap.fromAxisAngle(point2, radius);

            // Circle must contain every vertex
            vertices = new ArrayList<S2Point>();
            for (int i = 0; i < coords1.size(); i += 2) {
                ra = coords1.get(i).get();
                dec = coords1.get(i + 1).get();

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

        } else { // (kind1 == ADQLGeometry.Kind.POLYGON && kind2 == ADQLGeometry.Kind.POLYGON)
            // POLYGON inside POLYGON
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

            return new BooleanWritable(polygon2.containsNested(polygon1));

        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("contains", children);
    }
}
