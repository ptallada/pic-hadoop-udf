package es.pic.hadoop.udf.adql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public interface ADQLGeometry {

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
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableLongObjectInspector),
            }));
}
