package es.pic.hadoop.udf.adql;

import healpix.essentials.HealpixBase;

public class ADQLRegion extends ADQLGeometry {

    final static double hpix29_area = 1.1927080055488187e-14;

    public ADQLRegion(ADQLRangeSet rs) {
        super(ADQLGeometry.Kind.REGION, null, rs);
    }

    @Override
    public ADQLRegion complement() {
        ADQLRangeSet full = new ADQLRangeSet(new long[] {
                0L, 12L * (1L << (2 * HealpixBase.order_max))
        });
        return new ADQLRegion(full.difference(getRangeSet()));
    }

    @Override
    public ADQLPoint centroid() {
        throw new UnsupportedOperationException("REGIONs centroid are not supported yet.");
    }

    @Override
    public double area() {
        return getRangeSet().nval() * hpix29_area;
    }

    public ADQLRegion toRegion() {
        return this;
    }

    @Override
    public ADQLRegion toRegion(byte order) {
        return new ADQLRegion(getRangeSet().degradedToOrder(order));
    }

    public boolean contains(ADQLRegion other) {
        return getRangeSet().contains(other.getRangeSet());
    }

    public boolean intersects(ADQLRegion other) {
        return getRangeSet().overlaps(other.getRangeSet());
    }
}
