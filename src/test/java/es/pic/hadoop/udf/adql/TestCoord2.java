package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCoord2 {

    UDFCoord2 udf = new UDFCoord2();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    Object point;
    Object circle;

    public TestCoord2() {
        point = new ADQLPoint(10, 20).serialize();
        circle = new ADQLCircle(10, 20, 30).serialize();
    }

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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
    }

    @Test
    void wrongTypeOfArguments() {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(params));
    }

    @Test
    void nullPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void invalidPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle)
        }));
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("20.0", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point), new DeferredJavaObject(new DoubleWritable(30)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}