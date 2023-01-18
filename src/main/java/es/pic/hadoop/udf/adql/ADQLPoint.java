package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class ADQLPoint extends ADQLGeometry {

    List<DoubleWritable> coords;

    public ADQLPoint(double ra, double dec) {
        this.coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(ra), new DoubleWritable(dec)
        });
    }

    protected ADQLPoint(List<DoubleWritable> coords) {
        this.coords = coords;
    }

    public double getRa() {
        return this.coords.get(0).get();
    }

    public double getDec() {
        return this.coords.get(1).get();
    }

    @Override
    public double area() {
        return 0;
    }

    public Object serialize() {
        Object blob = OI.create();
        OI.setFieldAndTag(blob, coords, Kind.POINT.tag);

        return blob;
    }
}
