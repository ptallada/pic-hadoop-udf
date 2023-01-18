package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class ADQLPolygon extends ADQLGeometry {

    List<DoubleWritable> coords;

    public ADQLPolygon(double... args) {
        coords = new ArrayList<DoubleWritable>();
        for (double coord : args) {
            coords.add(new DoubleWritable(coord));
        }
    }

    protected ADQLPolygon(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    @Override
    public double area() {
        List<S2Point> points = new ArrayList<S2Point>();

        for (int i = 0; i < coords.size(); i += 2) {
            double ra = coords.get(i).get();
            double dec = coords.get(i + 1).get();
            points.add(S2LatLng.fromDegrees(dec, ra).toPoint());
        }

        S2Loop loop = new S2Loop(points);

        return Math.toDegrees(Math.toDegrees(loop.getArea()));
    }

    public Object serialize() {
        Object blob = OI.create();
        OI.setFieldAndTag(blob, coords, Kind.POLYGON.tag);

        return blob;
    }
}