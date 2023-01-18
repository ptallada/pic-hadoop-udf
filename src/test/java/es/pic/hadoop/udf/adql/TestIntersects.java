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

import healpix.essentials.Moc;

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

    public TestIntersects() throws HiveException {
        point1 = new ADQLPoint(-4, 0).serialize();
        point2 = new ADQLPoint(0, 0).serialize();
        point3 = new ADQLPoint(6, 0).serialize();

        circle1 = new ADQLCircle(-4, 0, 1).serialize();
        circle2 = new ADQLCircle(0, 0, 7).serialize();
        circle3 = new ADQLCircle(6, 0, 3).serialize();

        polygon1 = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8, 3).serialize();
        polygon2 = new ADQLPolygon(-1, -1, 1, -1, 1, 1, -1, 1).serialize();
        polygon3 = new ADQLPolygon(-1, 2, 1, 2, 1, 4, -1, 4).serialize();

        Moc moc = new Moc();
        moc.addPixelRange(3, 23, 34);
        region = new ADQLRegion(moc).serialize();
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
                new DeferredJavaObject(point1), new DeferredJavaObject(point1)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point1), new DeferredJavaObject(point2)
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
                new DeferredJavaObject(circle2), new DeferredJavaObject(circle3)
        }).toString());
        assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle1), new DeferredJavaObject(circle3)
        }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle2), new DeferredJavaObject(polygon1)
        }).toString());
        // assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
        //         new DeferredJavaObject(circle3), new DeferredJavaObject(polygon3)
        // }).toString());
        assertEquals("true", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon1), new DeferredJavaObject(circle2)
        }).toString());
        // assertEquals("false", udf.evaluate(new DeferredJavaObject[] {
        //         new DeferredJavaObject(polygon3), new DeferredJavaObject(circle1)
        // }).toString());
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