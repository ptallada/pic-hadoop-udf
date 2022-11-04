package es.pic.hadoop.udf.math;
import java.lang.Math;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@Description(
    name="angdist",
    value="_FUNC_(theta1, phi1, theta2, phi2, lonlat) - Return the distance between 2 angles points",
    extended="SELECT _FUNC_(2.3, 1.2, 1.5, 1.6, true) FROM foo LIMIT 1;"
)

@UDFType(
    deterministic = true,
    stateful = false
)

public class UDFAngDist extends UDF {
    public Double evaluate(Double theta1, Double phi1, Double theta2, Double phi2) throws Exception {
        if(theta1 == null || phi1 == null || theta2 == null || phi2 == null)
            return null;
        Double vec1[] = ang2vec(theta1, phi1);
        Double vec2[] = ang2vec(theta2, phi2);
        Double vecProd = vecProduct(vec1, vec2);
        Double scalProd = scalProduct(vec1, vec2);
        return Math.atan2(vecProd, scalProd);
    }

    private Double[] ang2vec(Double theta, Double phi) { 
        Double[] vec = new Double[3];
        vec[0] = Math.sin(theta.doubleValue()) * Math.cos(phi.doubleValue());
        vec[1] = Math.sin(theta.doubleValue()) * Math.sin(phi.doubleValue());
        vec[2] = Math.cos(theta.doubleValue());
        return vec;
    }

    private Double vecProduct(Double[] vec1, Double[] vec2) {
        Double[] vec = new Double[3];
        vec[0] = vec1[1] * vec2[2] - vec1[2] * vec2[1];
        vec[1] = vec1[2] * vec2[0] - vec1[0] * vec2[2];
        vec[2] = vec1[0] * vec2[1] - vec1[1] * vec2[0];
        return vec[0] + vec[1] + vec[2];
    }

    private Double scalProduct(Double[] vec1, Double[] vec2) {
        return vec1[0] * vec2[0] + vec1[1] * vec2[1] + vec1[2] * vec2[2];
    }
}
