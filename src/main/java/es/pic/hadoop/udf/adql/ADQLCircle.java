package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

public class ADQLCircle extends ADQLGeometry {

    protected List<DoubleWritable> coords;

    public ADQLCircle(double ra, double dec, double radius) {
        this.coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(ra), new DoubleWritable(dec), new DoubleWritable(radius)
        });
    }

    protected ADQLCircle(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    protected static ADQLCircle fromBlob(Object blob) {
        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) OI.getField(blob);

        return new ADQLCircle(coords);
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
    public ADQLCircle complement() throws HiveException {
        return new ADQLCircle((getRa() + 180) % 360, -getDec(), 180 - getRadius());
    }

    @Override
    public double area() {
        S2Cap circle = S2Cap.fromAxisAngle(S2LatLng.fromDegrees(getDec(), getRa()).toPoint(),
                S1Angle.degrees(getRadius()));

        return Math.toDegrees(Math.toDegrees(circle.area()));
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta = Math.toRadians(this.getDec());
        double phi = Math.toRadians(this.getRa());
        double radius = Math.toRadians(this.getRadius());

        Pointing pt = new Pointing(theta, phi);

        RangeSet rs;
        try {
            rs = HealpixProc.queryDiscNest(order, pt, radius);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        Moc moc = new Moc(rs, order);

        return new ADQLRegion(moc);
    }

    public Object serialize() {
        Object blob = OI.create();
        OI.setFieldAndTag(blob, coords, Kind.CIRCLE.tag);

        return blob;
    }
}