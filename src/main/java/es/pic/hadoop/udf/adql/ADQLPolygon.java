package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

public class ADQLPolygon extends ADQLGeometry {

    private static List<Double> parseCoords(double... args) {
        List<Double> coords = new ArrayList<Double>(args.length);
        for (double coord : args) {
            coords.add(new Double(coord));
        }
        return coords;
    }

    public ADQLPolygon(double... args) {
        this(parseCoords(args));
    }

    protected ADQLPolygon(List<Double> coords) {
        super(ADQLGeometry.Kind.POLYGON, coords, null);
    }

    @Override
    public ADQLPolygon complement() {
        int size = getNumCoords();
        List<Double> coords = new ArrayList<Double>(size);

        for (int i = size - 2; i >= 0; i -= 2) {
            coords.add(getCoord(i));
            coords.add(getCoord(i + 1));
        }

        return new ADQLPolygon(coords);
    }

    @Override
    public ADQLPoint centroid() {
        List<S2Point> vertices = new ArrayList<S2Point>();

        double ra;
        double dec;

        for (int i = 0; i < getNumCoords(); i += 2) {
            ra = getCoord(i);
            dec = getCoord(i + 1);
            vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
        }

        S2Loop loop = new S2Loop(vertices);
        S2LatLng point = new S2LatLng(loop.getCentroid());

        return new ADQLPoint(point.lngDegrees(), point.latDegrees());
    }

    @Override
    public double area() {
        int size = getNumCoords();
        List<S2Point> vertices = new ArrayList<S2Point>();

        for (int i = 0; i < size; i += 2) {
            double ra = getCoord(i);
            double dec = getCoord(i + 1);
            vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
        }

        S2Loop loop = new S2Loop(vertices);

        return Math.toDegrees(Math.toDegrees(loop.getArea()));
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta;
        double phi;
        Pointing pt;
        RangeSet hp_rs;

        int size = getNumCoords();
        ArrayList<Pointing> vertices = new ArrayList<Pointing>();
        for (int i = 0; i < size; i += 2) {
            theta = Math.toRadians(90 - getCoord(i + 1));
            phi = Math.toRadians(getCoord(i));
            pt = new Pointing(theta, phi);
            vertices.add(pt);
        }
        Pointing[] pts = new Pointing[vertices.size()];
        vertices.toArray(pts);

        // FIXME: HEALPix only works with convex polygons, need to implement ear-clipping
        try {
            hp_rs = HealpixProc.queryPolygonInclusiveNest(order, pts, INCLUSIVE_FACTOR);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        ADQLRangeSet rs = ADQLRangeSet.fromHealPixRangeSet(hp_rs, order);

        return new ADQLRegion(rs);
    }
}