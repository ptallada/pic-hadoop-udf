package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
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

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestPolygon {

    UDFPolygon udf = new UDFPolygon();

    List<DoubleWritable> coords;
    List<Object> points = new ArrayList<Object>();
    
    Object circle;

    public TestPolygon() {
        circle = new ADQLCircle(11, 22, 33).serialize();

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(10), new DoubleWritable(20), new DoubleWritable(30), new DoubleWritable(40),
                new DoubleWritable(50), new DoubleWritable(60), new DoubleWritable(70), new DoubleWritable(80),
        });

        points.add(new ADQLPoint(coords.subList(0, 2)).serialize());
        points.add(new ADQLPoint(coords.subList(2, 4)).serialize());
        points.add(new ADQLPoint(coords.subList(4, 6)).serialize());
        points.add(new ADQLPoint(coords.subList(6, 8)).serialize());
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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
    }

    @Test
    void wrongTypeOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector, ADQLGeometry.OI, ADQLGeometry.OI,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 4)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 5)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 3, 6)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 4, 7)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 6)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 5, 8)));
    }

    @Test
    void nullPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI, ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(points.get(1)),
                new DeferredJavaObject(points.get(2))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(points.get(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(points.get(2))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(points.get(0)), new DeferredJavaObject(points.get(1)),
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void nullCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(null), new DeferredJavaObject(new DoubleWritable(0)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(new DoubleWritable(0)),
                new DeferredJavaObject(new DoubleWritable(0)), new DeferredJavaObject(null),
        }));
    }

    @Test
    void invalidPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle), new DeferredJavaObject(points.get(1)),
                new DeferredJavaObject(points.get(2)),
        }).toString());
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(points.get(0)), new DeferredJavaObject(circle),
                new DeferredJavaObject(points.get(2)),
        }).toString());
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(points.get(0)), new DeferredJavaObject(points.get(1)),
                new DeferredJavaObject(circle),
        }).toString());
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("2:[10.0, 20.0, 30.0, 40.0, 50.0, 60.0]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(points.get(0)), new DeferredJavaObject(points.get(1)),
                new DeferredJavaObject(points.get(2)),
        }).toString());
    }

    @Test
    void validCoords() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("2:[10.0, 20.0, 30.0, 40.0, 50.0, 60.0]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(coords.get(0)), new DeferredJavaObject(coords.get(1)),
                new DeferredJavaObject(coords.get(2)), new DeferredJavaObject(coords.get(3)),
                new DeferredJavaObject(coords.get(4)), new DeferredJavaObject(coords.get(5)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}