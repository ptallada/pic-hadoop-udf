package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPoint {

    UDFPoint udf = new UDFPoint();

    @Test
    void emptyArguments() {
        ObjectInspector[] params = new ObjectInspector[0];

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(params));
    }

    @Test
    void wrongNumberOfArguments() {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
    }

    @Test
    void nullPixel() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void nullCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new Double(20))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new Double(20)), new DeferredJavaObject(null),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(null),
        }));
    }

    @Test
    void invalidPixel() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new Long(-1)),
        }));
    }

    @Test
    void validPixels() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("[0, [45.0, 7.114779521089076E-8], null]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new Long(0)),
        }).toString());
    }

    @Test
    void validDoubleCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("[0, [0.0, 0.0], null]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new Double(0)), new DeferredJavaObject(new Double(0)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}