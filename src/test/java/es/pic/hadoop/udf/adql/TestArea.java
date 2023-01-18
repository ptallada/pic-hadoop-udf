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
public class TestArea {

    UDFArea udf = new UDFArea();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    Object point;
    Object circle;
    Object polygon;
    Object region;

    public TestArea() throws HiveException {
        point = new ADQLPoint(10, 20).serialize();
        circle = new ADQLCircle(10, 20, 30).serialize();
        polygon = new ADQLPolygon(10, 10, 20, 10, 20, 20, 10, 20).serialize();

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

        assertEquals("0.0", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point),
        }).toString());
    }

    @Test
    void validCircle() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("2763.4244130435727", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle),
        }).toString());
    }

    @Test
    void validPolygon() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("96.66473395608146", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon),
        }).toString());
    }

    @Test
    void validRegion() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("590.8627262286614", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}