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
public class TestRegion {

    UDFRegion udf = new UDFRegion();

    ObjectInspector outputOI = ADQLGeometry.OI;

    Object point;
    Object circle;
    Object polygon;
    Object region;

    public TestRegion() throws HiveException {
        point = new ADQLPoint(0, 0).serialize();
        circle = new ADQLCircle(1, 1, 1).serialize();
        polygon = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8, 3).serialize();

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
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
    }

    @Test
    void wrongTypeOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
    }

    @Test
    void nullGeom() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void invalidGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(UDFArgumentTypeException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point)
        }));
    }

    @Test
    void validGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals(819556010, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle)
        }).hashCode());
        assertEquals(780344799, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon)
        }).hashCode());
        assertEquals(35731649, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region)
        }).hashCode());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}