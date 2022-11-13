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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCentroid {

    UDFCentroid udf = new UDFCentroid();

    ObjectInspector outputOI = ADQLGeometry.OI;

    Object point;
    Object circle;
    Object polygon;
    Object region;

    public TestCentroid() {
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

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(10), new DoubleWritable(10), new DoubleWritable(20), new DoubleWritable(10),
                new DoubleWritable(20), new DoubleWritable(20), new DoubleWritable(10), new DoubleWritable(20),
        });
        polygon = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(polygon, coords, ADQLGeometry.Kind.POLYGON.tag);

        List<LongWritable> ranges = Arrays.asList(new LongWritable[] {
                new LongWritable(0), new LongWritable(1)
        });
        region = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(region, ranges, ADQLGeometry.Kind.REGION.tag);
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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
    }

    @Test
    void wrongTypeOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(params));
    }

    @Test
    void nullGeometry() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point),
        }));
    }

    @Test
    void validCircle() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("0:[10.0, 20.0]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle),
        }).toString());
    }

    @Test
    void validPolygon() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("0:[14.999999999999977, 15.014819855438457]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon),
        }).toString());
    }

    @Test
    void validRegion() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UnsupportedOperationException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region),
        }));
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}