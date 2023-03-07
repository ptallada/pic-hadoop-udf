package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

public abstract class ADQLGeometry {

    final public static byte DEFAULT_ORDER = 10;

    public static final StandardStructObjectInspector OI = ObjectInspectorFactory
            .getStandardStructObjectInspector(Arrays.asList(new String[] {
                    "tag", "coords", "moc"
            }), Arrays.asList(new ObjectInspector[] {
                    // TAG
                    PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                    // COORDS
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                    // MOC
                    PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
            }));

    public static final StructField tagField = OI.getAllStructFieldRefs().get(0);
    public static final StructField coordsField = OI.getAllStructFieldRefs().get(1);
    public static final StructField mocField = OI.getAllStructFieldRefs().get(2);

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

    protected ADQLGeometry() {
    }

    public static Kind getTag(Object blob) {
        return getTag(blob, OI);
    }

    public static Kind getTag(Object blob, StructObjectInspector OI) {
        return Kind.valueOfTag(((ByteWritable) OI.getStructFieldData(blob, tagField)).get());
    }

    public static List<DoubleWritable> getCoords(Object blob) {
        return getCoords(blob, OI);
    }

    @SuppressWarnings("unchecked")
    public static List<DoubleWritable> getCoords(Object blob, StructObjectInspector OI) {
        return (List<DoubleWritable>) OI.getStructFieldData(blob, ADQLGeometry.coordsField);
    }

    public static BytesWritable getBytes(Object blob) {
        return getBytes(blob, OI);
    }

    public static BytesWritable getBytes(Object blob, StructObjectInspector OI) {
        return (BytesWritable) OI.getStructFieldData(blob, ADQLGeometry.mocField);
    }

    protected static ADQLGeometry fromBlob(Object blob) throws HiveException {
        return fromBlob(blob, OI);
    }

    protected static ADQLGeometry fromBlob(Object blob, StructObjectInspector OI) throws HiveException {
        Kind kind = getTag(blob, OI);

        switch (kind) {
        case POINT:
            return ADQLPoint.fromBlob(blob, OI);
        case CIRCLE:
            return ADQLCircle.fromBlob(blob, OI);
        case POLYGON:
            return ADQLPolygon.fromBlob(blob, OI);
        default: //REGION
            return ADQLRegion.fromBlob(blob, OI);
        }
    }

    public abstract ADQLGeometry complement() throws HiveException;

    public abstract double area();

    public ADQLRegion toRegion() throws HiveException {
        return toRegion(DEFAULT_ORDER);
    }

    public abstract ADQLRegion toRegion(byte order) throws HiveException;

    public abstract Object serialize() throws HiveException;
}
