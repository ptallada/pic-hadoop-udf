package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public abstract class ADQLGeometry {

    protected final static int INCLUSIVE_FACTOR = 4;
    protected final static byte DEFAULT_ORDER = 10;

    private static final PrimitiveObjectInspector tagOI = PrimitiveObjectInspectorFactory.javaByteObjectInspector;
    private static final BinaryObjectInspector rsOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    private static final ListObjectInspector coordsOI = ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

    public static final StandardStructObjectInspector OI = ObjectInspectorFactory
            .getStandardStructObjectInspector(Arrays.asList(new String[] {
                    "tag", "coords", "rs"
            }), Arrays.asList(new ObjectInspector[] {
                    tagOI, coordsOI, rsOI
            }));

    private static final StructField tagField = OI.getAllStructFieldRefs().get(0);
    private static final StructField coordsField = OI.getAllStructFieldRefs().get(1);
    private static final StructField rsField = OI.getAllStructFieldRefs().get(2);

    public static enum Kind {
        // @formatter:off
        POINT(0),
        CIRCLE(1),
        POLYGON(2),
        REGION(3);
        // @formatter:on

        public final byte value;

        private Kind(int tag) {
            this.value = (byte) tag;
        }

        private static final Map<Byte, Kind> BY_TAG = new HashMap<>();

        static {
            for (Kind e : values()) {
                BY_TAG.put(e.value, e);
            }
        }

        public static Kind valueOfTag(Byte tag) {
            return BY_TAG.get(tag);
        }
    }

    private Kind kind;
    private List<Double> coords;
    private ADQLRangeSet rs;

    private static Kind getTag(Object blob, StructObjectInspector OI) {
        StructField field = OI.getStructFieldRef("tag");
        Object obj = OI.getStructFieldData(blob, field);
        ObjectInspector extOI = field.getFieldObjectInspector();
        Converter converter = ObjectInspectorConverters.getConverter(extOI, tagOI);

        return Kind.valueOfTag((Byte) converter.convert(obj));
    }

    @SuppressWarnings("unchecked")
    private static List<Double> getCoords(Object blob, StructObjectInspector OI) {
        StructField field = OI.getStructFieldRef("coords");
        Object obj = OI.getStructFieldData(blob, field);
        if (obj == null) {
            return null;
        } else {
            ObjectInspector extOI = field.getFieldObjectInspector();
            Converter converter = ObjectInspectorConverters.getConverter(extOI, coordsOI);
            return (List<Double>) converter.convert(obj);
        }
    }

    private static ADQLRangeSet getRs(Object blob, StructObjectInspector OI) {
        StructField field = OI.getStructFieldRef("rs");
        Object obj = OI.getStructFieldData(blob, field);
        if (obj == null) {
            return null;
        } else {
            ObjectInspector extOI = field.getFieldObjectInspector();
            Converter converter = ObjectInspectorConverters.getConverter(extOI, rsOI);
            return new ADQLRangeSet((byte[]) converter.convert(obj));
        }
    }

    protected ADQLGeometry(Kind kind, List<Double> coords, ADQLRangeSet rs) {
        this.kind = kind;
        this.coords = coords;
        this.rs = rs;
    }

    protected static ADQLGeometry fromBlob(Object blob, StructObjectInspector OI) {
        Kind kind = getTag(blob, OI);
        List<Double> coords = getCoords(blob, OI);
        ADQLRangeSet rs = getRs(blob, OI);

        switch (kind) {
        case POINT:
            return new ADQLPoint(coords);
        case CIRCLE:
            return new ADQLCircle(coords);
        case POLYGON:
            return new ADQLPolygon(coords);
        default: //REGION
            return new ADQLRegion(rs);
        }
    }

    protected Kind getKind() {
        return this.kind;
    }

    protected Integer getNumCoords() {
        if (this.coords == null) {
            return null;
        } else {
            return this.coords.size();
        }
    }

    protected double getCoord(int i) {
        return this.coords.get(i).doubleValue();
    }

    protected ADQLRangeSet getRangeSet() {
        return this.rs;
    }

    public abstract ADQLGeometry complement() throws HiveException;

    public abstract ADQLPoint centroid();

    public abstract double area();

    public ADQLRegion toRegion() throws HiveException {
        return toRegion(DEFAULT_ORDER);
    }

    public abstract ADQLRegion toRegion(byte order) throws HiveException;

    public Object serialize() {
        Object blob = OI.create();

        OI.setStructFieldData(blob, ADQLGeometry.tagField, kind.value);
        OI.setStructFieldData(blob, ADQLGeometry.coordsField, coords);
        if (rs == null) {
            OI.setStructFieldData(blob, ADQLGeometry.rsField, null);
        } else {
            OI.setStructFieldData(blob, ADQLGeometry.rsField, rs.getRangesAsBytes());
        }
        return blob;
    }
}
