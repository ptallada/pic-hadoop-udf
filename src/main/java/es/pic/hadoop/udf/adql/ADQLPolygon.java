package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

public class ADQLPolygon extends ADQLGeometry {

    protected List<DoubleWritable> coords;

    public ADQLPolygon(double... args) {
        coords = new ArrayList<DoubleWritable>();
        for (double coord : args) {
            coords.add(new DoubleWritable(coord));
        }
    }

    protected ADQLPolygon(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    protected static ADQLPolygon fromBlob(Object blob) {
        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) OI.getField(blob);

        return new ADQLPolygon(coords);
    }

    @Override
    public ADQLPolygon complement() throws HiveException{
        List<DoubleWritable> coords = new ArrayList<DoubleWritable>(this.coords.size());
        
        for (int i=this.coords.size()-2; i>=0; i-=2) {
            coords.add(this.coords.get(i));
            coords.add(this.coords.get(i+1));
        }
                
        return new ADQLPolygon(coords);
    }

    @Override
    public double area() {
        List<S2Point> vertices = new ArrayList<S2Point>();

        for (int i = 0; i < coords.size(); i += 2) {
            double ra = coords.get(i).get();
            double dec = coords.get(i + 1).get();
            vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
        }

        S2Loop loop = new S2Loop(vertices);

        return Math.toDegrees(Math.toDegrees(loop.getArea()));
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta;
        double phi;
        Pointing pt;
        RangeSet rs;

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

        Moc moc = new Moc(rs, order);
        
        return new ADQLRegion(moc);
    }

    public Object serialize() {
        Object blob = OI.create();
        OI.setFieldAndTag(blob, coords, Kind.POLYGON.tag);

        return blob;
    }
}