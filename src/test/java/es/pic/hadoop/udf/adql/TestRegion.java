package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.zip.Adler32;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

    ADQLGeometry geom;
    Adler32 cksum = new Adler32();

    public TestRegion() throws HiveException {
        point = new ADQLPoint(0, 0).serialize();
        circle = new ADQLCircle(1, 1, 1).serialize();
        polygon = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8, 3).serialize();

        invalid_point = new ADQLPoint(0, 200).serialize();
        invalid_circle = new ADQLCircle(0, 200, 400).serialize();
        invalid_polygon = new ADQLPolygon(-8, -3, 2, -3, 3, 3, -8).serialize();

        ADQLRangeSet rs = new ADQLRangeSet();
        rs.addPixelRange(29, 10, 90);
        region = new ADQLRegion(rs).serialize();
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
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
    }

    @Test
    void wrongTypeOfArguments() {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
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
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.javaByteObjectInspector,
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

        // POINT
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(point)
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(30998568L, cksum.getValue());

        // CIRCLE
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(circle)
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(3713698213L, cksum.getValue());

        // POLYGON
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(polygon)
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(271681799L, cksum.getValue());

        // REGION
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(region)
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(12845157L, cksum.getValue());
    }

    @Test
    void validGeomsOrder() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI, PrimitiveObjectInspectorFactory.javaByteObjectInspector,
        };

        assertEquals(udf.initialize(params), ADQLGeometry.OI);

        // POINT
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
            new DeferredJavaObject(point), new DeferredJavaObject(new Byte((byte) 3))
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(38273079L, cksum.getValue());

        // CIRCLE
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
            new DeferredJavaObject(circle), new DeferredJavaObject(new Byte((byte) 3))
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(2575827854L, cksum.getValue());

        // POLYGON
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
            new DeferredJavaObject(polygon), new DeferredJavaObject(new Byte((byte) 3))
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(3910665362L, cksum.getValue());

        // REGION
        geom = ADQLGeometry.fromBlob(udf.evaluate(new DeferredJavaObject[] {
            new DeferredJavaObject(region), new DeferredJavaObject(new Byte((byte) 3))
        }), ADQLGeometry.OI);
        assertEquals(ADQLGeometry.Kind.REGION, geom.getKind());
        assertNull(geom.getNumCoords());
        cksum.reset();
        cksum.update(geom.getRangeSet().getRangesAsBytes());
        assertEquals(12845157L, cksum.getValue());
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