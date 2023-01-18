package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import healpix.essentials.Moc;

public class ADQLRegion extends ADQLGeometry {

    static double hpix29_area = 1.1927080055488187e-14;

    Moc moc;

    public ADQLRegion(Moc moc) {
        this.moc = moc;
    }

    @Override
    public double area() {
        return moc.getRangeSet().nval() * hpix29_area;
    }

    public Object serialize() throws HiveException {
        Object blob = OI.create();
        try {
            OI.setFieldAndTag(blob, new BytesWritable(moc.toCompressed()), Kind.REGION.tag);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return blob;
    }
}