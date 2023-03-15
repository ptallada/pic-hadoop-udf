package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLng;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;

public class ADQLCircle extends ADQLGeometry {

    public ADQLCircle(double ra, double dec, double radius) {
        this(Arrays.asList(new Double[] {
                new Double(ra), new Double(dec), new Double(radius)
        }));
    }

    protected ADQLCircle(List<Double> coords) {
        super(ADQLGeometry.Kind.CIRCLE, coords, null);
    }

    public double getRa() {
        return getCoord(0);
    }

    public double getDec() {
        return getCoord(1);
    }

    public double getRadius() {
        return getCoord(2);
    }

    @Override
    public ADQLCircle complement() {
        return new ADQLCircle((getRa() + 180) % 360, -getDec(), 180 - getRadius());
    }

    @Override
    public ADQLPoint centroid() {
        return new ADQLPoint(getRa(), getDec());
    }

    @Override
    public double area() {
        S2Cap circle = S2Cap.fromAxisAngle(S2LatLng.fromDegrees(getDec(), getRa()).toPoint(),
                S1Angle.degrees(getRadius()));

        return Math.toDegrees(Math.toDegrees(circle.area()));
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta = Math.toRadians(90 - this.getDec());
        double phi = Math.toRadians(this.getRa());
        double radius = Math.toRadians(this.getRadius());

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