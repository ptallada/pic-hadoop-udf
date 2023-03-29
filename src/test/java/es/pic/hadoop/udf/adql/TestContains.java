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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestContains {

    UDFContains udf = new UDFContains();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    ADQLRangeSet rs;
    Object point1;
    Object point2;
    Object point3;
    Object circle1;
    Object circle2;
    Object circle3;
    Object polygon1;
    Object polygon2;
    Object polygon3;
    Object region1;
    Object region2;
    Object region3;

    public TestContains() {
        point1 = new ADQLPoint(-4, 0).serialize();
        point2 = new ADQLPoint(0, 0).serialize();
        point3 = new ADQLPoint(6, 0).serialize();

        circle1 = new ADQLCircle(-4, 0, 1).serialize();
        circle2 = new ADQLCircle(0, 0, 7).serialize();
        circle3 = new ADQLCircle(6, 0, 3).serialize();

        polygon1 = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8, 3).serialize();
        polygon2 = new ADQLPolygon(-1, -1, 1, -1, 1, 1, -1, 1).serialize();
        polygon3 = new ADQLPolygon(-1, 2, 1, 2, 1, 4, -1, 4).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(3, 1, 10);
        region1 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(3, 45, 50);
        region2 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(3, 40, 750);
        region3 = new ADQLRegion(rs).serialize();
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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
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
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(point2)
        }));
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon1), new DeferredJavaObject(point2)
        }));
        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region1), new DeferredJavaObject(point2)
        }));
    }

    @Test
    void validGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(region3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point2), new DeferredJavaObject(region1)
        }).toString());

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle2), new DeferredJavaObject(region3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle3), new DeferredJavaObject(region1)
        }).toString());

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon2), new DeferredJavaObject(region3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon3), new DeferredJavaObject(region1)
        }).toString());

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region2), new DeferredJavaObject(region3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region1), new DeferredJavaObject(region2)
        }).toString());

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
                new DeferredJavaObject(circle1), new DeferredJavaObject(circle2)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(circle3)
        }).toString());

        // assertEquals("true", udf.evaluate(new DeferredJavaObject[] { //
        //         new DeferredJavaObject(circle1), new DeferredJavaObject(polygon1)
        // }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(polygon3)
        }).toString());

        // assertEquals("true", udf.evaluate(new DeferredJavaObject[] { //
        //         new DeferredJavaObject(polygon3), new DeferredJavaObject(circle2)
        // }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon3), new DeferredJavaObject(circle3)
        }).toString());

        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon2), new DeferredJavaObject(polygon1)
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