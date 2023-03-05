package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import healpix.essentials.HealpixBase;
import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;

public class ADQLPoint extends ADQLGeometry {

    protected List<DoubleWritable> coords;

    public ADQLPoint(double ra, double dec) {
        this.coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(ra), new DoubleWritable(dec)
        });
    }

    protected ADQLPoint(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    protected static ADQLPoint fromBlob(Object blob) {
        return fromBlob(blob, ADQLGeometry.OI);
    }

    protected static ADQLPoint fromBlob(Object blob, StructObjectInspector OI) {
        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) OI.getStructFieldData(blob, ADQLGeometry.coordsField);

        return new ADQLPoint(coords);
    }

    public double getRa() {
        return this.coords.get(0).get();
    }

    public double getDec() {
        return this.coords.get(1).get();
    }

    @Override
    public ADQLGeometry complement() throws HiveException {
        throw new UnsupportedOperationException("Point geometry has no complement.");
    }

    @Override
    public double area() {
        return 0;
    }

    public ADQLRegion toRegion() throws HiveException {
        return toRegion((byte) HealpixBase.order_max);
    }

    public ADQLRegion toRegion(byte order) throws HiveException {
        double theta = Math.toRadians(this.getDec());
        double phi = Math.toRadians(this.getRa());

        Pointing pt = new Pointing(theta, phi);

        long ipix;
        try {
            ipix = HealpixProc.ang2pixNest(order, pt);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        Moc moc = new Moc();
        moc.addPixel(order, ipix);

        return new ADQLRegion(moc);
    }

    public Object serialize() {
        Object blob = OI.create();

        OI.setStructFieldData(blob, ADQLGeometry.tagField, Kind.POINT.tag);
        OI.setStructFieldData(blob, ADQLGeometry.coordsField, coords);

        return blob;
    }
}
