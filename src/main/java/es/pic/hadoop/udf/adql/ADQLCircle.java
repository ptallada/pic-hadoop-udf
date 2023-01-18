package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class ADQLCircle extends ADQLGeometry {

    List<DoubleWritable> coords;

    public ADQLCircle(double ra, double dec, double radius) {
        this.coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(ra), new DoubleWritable(dec), new DoubleWritable(radius)
        });
    }

    protected ADQLCircle(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    public double getRa() {
        return this.coords.get(0).get();
    }

    public double getDec() {
        return this.coords.get(1).get();
    }

    public double getRadius() {
        return this.coords.get(2).get();
    }

    @Override
    public double area() {
        S2Cap circle = S2Cap.fromAxisAngle(S2LatLng.fromDegrees(getDec(), getRa()).toPoint(), S1Angle.degrees(getRadius()));

        return Math.toDegrees(Math.toDegrees(circle.area()));
    }

    public Object serialize() {
        Object blob = OI.create();
        OI.setFieldAndTag(blob, coords, Kind.CIRCLE.tag);

        return blob;
    }
}