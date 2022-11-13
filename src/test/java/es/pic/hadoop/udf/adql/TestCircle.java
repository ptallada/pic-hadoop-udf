package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

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
public class TestCircle {

    UDFCircle udf = new UDFCircle();

    ObjectInspector outputOI = ADQLGeometry.OI;

    Object point;
    Object circle;

    public TestCircle() {
        List<DoubleWritable> coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(10), new DoubleWritable(20)
        });
        point = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(point, coords, ADQLGeometry.Kind.POINT.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(10), new DoubleWritable(20), new DoubleWritable(30)
        });
        circle = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(circle, coords, ADQLGeometry.Kind.CIRCLE.tag);
    }

    @Test
    void emptyArguments() {
        ObjectInspector[] params = new ObjectInspector[0];
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(params));
    }

    @Test
    void wrongNumberOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 4)));
    }

    @Test
    void wrongTypeOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        // First argument is not of ADQLGeometry type.
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(params));
    }

    @Test
    void nullPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        // Both arguments are null
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(null)
        }));

        // Only point is null
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0))
        }));

        // Only radius is null
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point), new DeferredJavaObject(null)
        }));
    }

    @Test
    void nullCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(null), new DeferredJavaObject(null)
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(null),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(null),
                new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(null),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(null),
        }));
    }

    @Test
    void invalidPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle), new DeferredJavaObject(new DoubleWritable(30)),
        }));
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("1:[10.0, 20.0, 30.0]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point), new DeferredJavaObject(new DoubleWritable(30)),
        }).toString());
    }

    @Test
    void validCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("1:[10.0, 20.0, 30.0]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(10)), new DeferredJavaObject(new DoubleWritable(20)),
                new DeferredJavaObject(new DoubleWritable(30)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}