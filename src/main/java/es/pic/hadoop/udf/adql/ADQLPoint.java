package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import healpix.essentials.HealpixBase;
import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

public class ADQLPoint extends ADQLGeometry {

    public ADQLPoint(double ra, double dec) {
        this(new DoubleWritable(ra), new DoubleWritable(dec));
    }

    public ADQLPoint(DoubleWritable ra, DoubleWritable dec) {
        this(Arrays.asList(new DoubleWritable[] {
                ra, dec
        }));
    }

    protected ADQLPoint(List<DoubleWritable> coords) {
        super(ADQLGeometry.Kind.POINT, coords, null);
    }

    public DoubleWritable getRa() {
        return getCoord(0);
    }

    public DoubleWritable getDec() {
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
        double theta = Math.toRadians(90 - this.getDec().get());
        double phi = Math.toRadians(this.getRa().get());

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
