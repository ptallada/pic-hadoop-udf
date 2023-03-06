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
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import healpix.essentials.Moc;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRegion {

    UDFRegion udf = new UDFRegion();

    Object point;
    Object circle;
    Object polygon;
    Object region;

    Object invalid_point;
    Object invalid_circle;
    Object invalid_polygon;

    public TestRegion() throws HiveException {
        point = new ADQLPoint(0, 0).serialize();
        circle = new ADQLCircle(1, 1, 1).serialize();
        polygon = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8, 3).serialize();

        invalid_point = new ADQLPoint(0, 200).serialize();
        invalid_circle = new ADQLCircle(0, 200, 400).serialize();
        invalid_polygon = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8).serialize();

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

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void nullOrder() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point), new DeferredJavaObject(null)
        }));
    }

    @Test
    void validGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals(357800518, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point)
        }).hashCode());
        assertEquals(819588683, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle)
        }).hashCode());
        assertEquals(780377470, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon)
        }).hashCode());
        assertEquals(35764324, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region)
        }).hashCode());
    }

    @Test
    void validGeomsOrder() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals(1193325843, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point), new DeferredJavaObject(new ByteWritable((byte) 3))
        }).hashCode());
        assertEquals(32705, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle), new DeferredJavaObject(new ByteWritable((byte) 3))
        }).hashCode());
        assertEquals(1193324786, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon), new DeferredJavaObject(new ByteWritable((byte) 3))
        }).hashCode());
        assertEquals(35764324, udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region), new DeferredJavaObject(new ByteWritable((byte) 3))
        }).hashCode());
    }

    @Test
    void invalidGeoms() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(invalid_point)
        }).hashCode());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}