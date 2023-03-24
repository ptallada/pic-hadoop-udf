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
public class TestDistance {

    UDFDistance udf = new UDFDistance();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    Object point1;
    Object point2;
    Object circle1;
    Object circle2;

    public TestDistance() {
        point1 = new ADQLPoint(10, 20).serialize();
        point2 = new ADQLPoint(30, 40).serialize();
        circle1 = new ADQLCircle(50, 60, 70).serialize();
        circle2 = new ADQLCircle(80, 90, 100).serialize();
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
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 5)));
    }

    @Test
    void wrongTypeOfArguments() {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector, ADQLGeometry.OI,
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 1, 3)));
    }

    @Test
    void nullPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(null)
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(null)
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(point2)
        }));
    }

    @Test
    void nullCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,

        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
        }));
    }

    @Test
    void invalidPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(point2)
        }));
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(circle2)
        }));
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(circle2)
        }));
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("26.326607525563194", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(point2)
        }).toString());
    }

    @Test
    void validCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("26.326607525563194", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(10)), new DeferredJavaObject(new DoubleWritable(20)),
                new DeferredJavaObject(new DoubleWritable(30)), new DeferredJavaObject(new DoubleWritable(40)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}