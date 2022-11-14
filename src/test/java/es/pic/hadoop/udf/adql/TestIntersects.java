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
import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIntersects {

    UDFIntersects udf = new UDFIntersects();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    Object point1;
    Object point2;
    Object point3;
    Object circle1;
    Object circle2;
    Object circle3;
    Object polygon1;
    Object polygon2;
    Object polygon3;
    Object region;

    public TestIntersects() {
        List<DoubleWritable> coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(-4), new DoubleWritable(0)
        });
        point1 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(point1, coords, ADQLGeometry.Kind.POINT.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(0), new DoubleWritable(0)
        });
        point2 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(point2, coords, ADQLGeometry.Kind.POINT.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(6), new DoubleWritable(0)
        });
        point3 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(point2, coords, ADQLGeometry.Kind.POINT.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(-4), new DoubleWritable(0), new DoubleWritable(1)
        });
        circle1 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(circle1, coords, ADQLGeometry.Kind.CIRCLE.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(0), new DoubleWritable(0), new DoubleWritable(7)
        });
        circle2 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(circle2, coords, ADQLGeometry.Kind.CIRCLE.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(6), new DoubleWritable(0), new DoubleWritable(3)
        });
        circle3 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(circle3, coords, ADQLGeometry.Kind.CIRCLE.tag);

        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(-8), new DoubleWritable(-3), new DoubleWritable(2), new DoubleWritable(-3),
                new DoubleWritable(3), new DoubleWritable(3), new DoubleWritable(-8), new DoubleWritable(3),
        });
        polygon1 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(polygon1, coords, ADQLGeometry.Kind.POLYGON.tag);
        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(-1), new DoubleWritable(-1), new DoubleWritable(1), new DoubleWritable(-1),
                new DoubleWritable(1), new DoubleWritable(1), new DoubleWritable(-1), new DoubleWritable(1),
        });
        polygon2 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(polygon2, coords, ADQLGeometry.Kind.POLYGON.tag);
        coords = Arrays.asList(new DoubleWritable[] {
                new DoubleWritable(-1), new DoubleWritable(2), new DoubleWritable(1), new DoubleWritable(2),
                new DoubleWritable(1), new DoubleWritable(4), new DoubleWritable(-1), new DoubleWritable(4),
        });
        polygon3 = ADQLGeometry.OI.create();
        ADQLGeometry.OI.setFieldAndTag(polygon3, coords, ADQLGeometry.Kind.POLYGON.tag);

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
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
    }

    @Test
    void wrongTypeOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector, ADQLGeometry.OI,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 1, 3)));
    }

    @Test
    void nullGeom() throws HiveException {
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
                new DeferredJavaObject(null), new DeferredJavaObject(circle2)
        }));
    }

    @Test
    void invalidGeom() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(point2)
        }));
        assertThrows(UnsupportedOperationException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(region)
        }));
        assertThrows(UnsupportedOperationException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region), new DeferredJavaObject(circle2)
        }));
    }

    @Test
    void validGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(circle2)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(circle3)
        }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(polygon1)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(polygon3)
        }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle2), new DeferredJavaObject(circle3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(circle3)
        }).toString());
        // assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
        //         new DeferredJavaObject(circle2), new DeferredJavaObject(polygon1)
        // }).toString());
        // assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
        //         new DeferredJavaObject(circle3), new DeferredJavaObject(polygon3)
        // }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon1), new DeferredJavaObject(circle2)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon3), new DeferredJavaObject(circle1)
        }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon1), new DeferredJavaObject(polygon3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon2), new DeferredJavaObject(polygon3)
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}