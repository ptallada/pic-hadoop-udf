package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

import healpix.essentials.Moc;

public abstract class ADQLGeometry {

    public static enum Kind {
        // @formatter:off
        POINT(0),
        CIRCLE(1),
        POLYGON(2),
        REGION(3);
        // @formatter:on

        public final byte tag;

        private Kind(int tag) {
            this.tag = (byte) tag;
        }

        private static final Map<Byte, Kind> BY_TAG = new HashMap<>();

        static {
            for (Kind e : values()) {
                BY_TAG.put(e.tag, e);
            }
        }

        public static Kind valueOfTag(Byte tag) {
            return BY_TAG.get(tag);
        }
    }

    public static final StandardUnionObjectInspector OI = ObjectInspectorFactory
            .getStandardUnionObjectInspector(Arrays.asList(new ObjectInspector[] {
                    // 0 - POINT
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                    // 1 - CIRCLE
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                    // 2 - POLYGON
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                    // 3 - REGION
                    PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
            }));

    protected ADQLGeometry() {
    }

    public static ADQLGeometry fromBlob(Object blob) throws HiveException {
        Kind kind = Kind.valueOfTag(OI.getTag(blob));

        if (kind == Kind.REGION) {
            BytesWritable bytes = (BytesWritable) OI.getField(blob);
            Moc moc;

            try {
                moc = Moc.fromCompressed(bytes.getBytes());
            } catch (Exception e) {
                throw new HiveException(e);
            }

            return new ADQLRegion(moc);
        }

        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) OI.getField(blob);

        switch (kind) {
        case POINT:
            return new ADQLPoint(coords);
        case CIRCLE:
            return new ADQLCircle(coords);
        case POLYGON:
            return new ADQLPolygon(coords);
        default:
            return null;
        }
    }

    public abstract double area();

    public abstract Object serialize() throws HiveException;
}
