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
public class TestComplement {

    UDFComplement udf = new UDFComplement();

    Object point;
    Object circle;
    Object polygon;
    Object region;

    ADQLRangeSet rs;
    ADQLRangeSet rs_output;

    public TestComplement() {
        point = new ADQLPoint(10, 20).serialize();
        circle = new ADQLCircle(10, 20, 30).serialize();
        polygon = new ADQLPolygon(10, 10, 20, 10, 20, 20, 10, 20).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(3, 23, 34);
        region = new ADQLRegion(rs).serialize();

        rs_output = new ADQLRangeSet();
        rs_output.addPixelRange(3, 0, 23);
        rs_output.addPixelRange(3, 34, 768);
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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
    }

    @Test
    void wrongTypeOfArguments() {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(params));
    }

    @Test
    void nullGeometry() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null)
        }));
    }

    @Test
    void validPoint() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertThrows(UnsupportedOperationException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point),
        }));
    }

    @Test
    void validCircle() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("[1, [190.0, -20.0, 150.0], null]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle),
        }).toString());
    }

    @Test
    void validPolygon() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        assertEquals("[2, [10.0, 20.0, 20.0, 20.0, 20.0, 10.0, 10.0, 10.0], null]",
                udf.evaluate(new DeferredJavaObject[] {
                        new DeferredJavaObject(polygon),
                }).toString());
    }

    @Test
    void validRegion() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        ADQLGeometry output = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region),
        }), ADQLGeometry.OI);

        assertEquals(3, output.getKind().value);
        assertNull(output.getNumCoords());
        assertEquals(rs_output, output.getRangeSet());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}