package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

public class ADQLCircle extends ADQLGeometry {

    public ADQLCircle(double ra, double dec, double radius) {
        this(new DoubleWritable(ra), new DoubleWritable(dec), new DoubleWritable(radius));
    }

    public ADQLCircle(DoubleWritable ra, DoubleWritable dec, DoubleWritable radius) {
        this(Arrays.asList(new DoubleWritable[] {
                ra, dec, radius
        }));
    }

    protected ADQLCircle(List<DoubleWritable> coords) {
        super(ADQLGeometry.Kind.CIRCLE, coords, null);
    }

    public DoubleWritable getRa() {
        return getCoord(0);
    }

    public DoubleWritable getDec() {
        return getCoord(1);
    }

    public DoubleWritable getRadius() {
        return getCoord(2);
    }

    @Override
    public ADQLCircle complement() {
        return new ADQLCircle((getRa().get() + 180) % 360, -getDec().get(), 180 - getRadius().get());
    }

    @Override
    public ADQLPoint centroid() {
        return new ADQLPoint(Arrays.asList(new DoubleWritable[] {
                getRa(), getDec()
        }));
    }

    @Override
    public double area() {
        S2Cap circle = S2Cap.fromAxisAngle(S2LatLng.fromDegrees(getDec().get(), getRa().get()).toPoint(),
                S1Angle.degrees(getRadius().get()));

        return Math.toDegrees(Math.toDegrees(circle.area()));
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta = Math.toRadians(90 - this.getDec().get());
        double phi = Math.toRadians(this.getRa().get());
        double radius = Math.toRadians(this.getRadius().get());

        Pointing pt = new Pointing(theta, phi);

        RangeSet hp_rs;
        try {
            hp_rs = HealpixProc.queryDiscInclusiveNest(order, pt, radius, INCLUSIVE_FACTOR);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        ADQLRangeSet rs = ADQLRangeSet.fromHealPixRangeSet(hp_rs, order);

        return new ADQLRegion(rs);
    }
}