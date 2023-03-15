package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import healpix.essentials.HealpixBase;
import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

public class ADQLPoint extends ADQLGeometry {

    public ADQLPoint(double ra, double dec) {
        this(Arrays.asList(new Double[] {
                new Double(ra), new Double(dec),
        }));
    }

    protected ADQLPoint(List<Double> coords) {
        super(ADQLGeometry.Kind.POINT, coords, null);
    }

    public double getRa() {
        return getCoord(0);
    }

    public double getDec() {
        return getCoord(1);
    }

    @Override
    public ADQLGeometry complement() throws HiveException {
        throw new UnsupportedOperationException("Point geometry has no complement.");
    }

    @Override
    public ADQLPoint centroid() {
        return this;
    }

    @Override
    public double area() {
        return 0;
    }

    public ADQLRegion toRegion() throws HiveException {
        return toRegion((byte) HealpixBase.order_max);
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta = Math.toRadians(90 - this.getDec());
        double phi = Math.toRadians(this.getRa());

        Pointing pt = new Pointing(theta, phi);

        long ipix;
        try {
            ipix = HealpixProc.ang2pixNest(order, pt);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        ADQLRangeSet rs = new ADQLRangeSet();
        rs.addPixel(order, ipix);

        return new ADQLRegion(rs);
    }
}
