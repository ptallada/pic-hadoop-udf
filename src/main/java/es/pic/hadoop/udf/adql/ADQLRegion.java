package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import healpix.essentials.Moc;

public class ADQLRegion extends ADQLGeometry {

    final static double hpix29_area = 1.1927080055488187e-14;

    protected Moc moc;

    public ADQLRegion(Moc moc) {
        this.moc = moc;
    }

    protected static ADQLRegion fromBlob(Object blob) throws HiveException {
        return fromBlob(blob, ADQLGeometry.OI);
    }

    protected static ADQLRegion fromBlob(Object blob, StructObjectInspector OI) throws HiveException {
        byte[] bytes = ADQLGeometry.getBytes(blob, OI).getBytes();
        Moc moc;

        try {
            moc = Moc.fromCompressed(bytes);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new ADQLRegion(moc);
    }

    @Override
    public ADQLRegion complement() throws HiveException {
        return new ADQLRegion(this.moc.complement());
    }

    @Override
    public double area() {
        return moc.getRangeSet().nval() * hpix29_area;
    }

    public ADQLRegion toRegion() throws HiveException {
        return this;
    }

    @Override
    public ADQLRegion toRegion(byte order) throws HiveException {
        // TODO: maybe use degradedToOrder(int order, boolean keepPartialCells)
        return this;
    }

    public boolean contains(ADQLRegion other) {
        return this.moc.contains(other.moc);
    }

    public boolean intersects(ADQLRegion other) {
        return this.moc.overlaps(other.moc);
    }

    public Object serialize() throws HiveException {
        Object blob = OI.create();
        byte[] bytes;

        try {
            bytes = moc.toCompressed();
        } catch (Exception e) {
            throw new HiveException(e);
        }

        OI.setStructFieldData(blob, ADQLGeometry.tagField, new ByteWritable(Kind.REGION.tag));
        OI.setStructFieldData(blob, ADQLGeometry.mocField, new BytesWritable(bytes));

        return blob;
    }
}
