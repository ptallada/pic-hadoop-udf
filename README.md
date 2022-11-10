# Apache Hive UDFs

## Instructions to build
```
mvn clean jacoco:prepare-agent compile test jacoco:report package assembly:single
```

## HEALPix
```
DROP FUNCTION IF EXISTS hp_ang2pix;
DROP FUNCTION IF EXISTS hp_ang2vec;
DROP FUNCTION IF EXISTS hp_pix2ang;
DROP FUNCTION IF EXISTS hp_pix2vec;
DROP FUNCTION IF EXISTS hp_vec2ang;
DROP FUNCTION IF EXISTS hp_vec2pix;
DROP FUNCTION IF EXISTS hp_neighbours;
DROP FUNCTION IF EXISTS hp_nest2ring;
DROP FUNCTION IF EXISTS hp_ring2nest;
DROP FUNCTION IF EXISTS hp_nside2npix;
DROP FUNCTION IF EXISTS hp_npix2nside;
DROP FUNCTION IF EXISTS hp_nside2order;
DROP FUNCTION IF EXISTS hp_order2npix;
DROP FUNCTION IF EXISTS hp_maxpixrad;

CREATE FUNCTION hp_ang2pix AS 'es.pic.hadoop.udf.healpix.UDFAng2Pix';
CREATE FUNCTION hp_ang2vec AS 'es.pic.hadoop.udf.healpix.UDFAng2Vec';
CREATE FUNCTION hp_pix2ang AS 'es.pic.hadoop.udf.healpix.UDFPix2Ang';
CREATE FUNCTION hp_pix2vec AS 'es.pic.hadoop.udf.healpix.UDFPix2Vec';
CREATE FUNCTION hp_vec2ang AS 'es.pic.hadoop.udf.healpix.UDFVec2Ang';
CREATE FUNCTION hp_vec2pix AS 'es.pic.hadoop.udf.healpix.UDFVec2Pix';
CREATE FUNCTION hp_neighbours AS 'es.pic.hadoop.udf.healpix.UDFNeighbours';
CREATE FUNCTION hp_nest2ring AS 'es.pic.hadoop.udf.healpix.UDFNest2Ring';
CREATE FUNCTION hp_ring2nest AS 'es.pic.hadoop.udf.healpix.UDFRing2Nest';
CREATE FUNCTION hp_nside2npix AS 'es.pic.hadoop.udf.healpix.UDFNside2Npix';
CREATE FUNCTION hp_npix2nside AS 'es.pic.hadoop.udf.healpix.UDFNpix2Nside';
CREATE FUNCTION hp_nside2order AS 'es.pic.hadoop.udf.healpix.UDFNside2Order';
CREATE FUNCTION hp_order2npix AS 'es.pic.hadoop.udf.healpix.UDFNside2Npix';
CREATE FUNCTION hp_maxpixrad AS 'es.pic.hadoop.udf.healpix.UDFMaxPixRad';
```

## Math
```
DROP FUNCTION IF EXISTS atan2;

CREATE FUNCTION atan2 AS 'es.pic.hadoop.udf.math.UDFAtan2';
```

## Arrays
```
DROP FUNCTION IF EXISTS array_min;
DROP FUNCTION IF EXISTS array_max;
DROP FUNCTION IF EXISTS array_sum;
DROP FUNCTION IF EXISTS array_count;
DROP FUNCTION IF EXISTS array_avg;
DROP FUNCTION IF EXISTS array_stddev_pop;
DROP FUNCTION IF EXISTS array_stddev_samp;
DROP FUNCTION IF EXISTS array_variance;
DROP FUNCTION IF EXISTS array_var_pop;
DROP FUNCTION IF EXISTS array_var_samp;

CREATE FUNCTION array_min AS 'es.pic.hadoop.udf.array.UDAFArrayMin';
CREATE FUNCTION array_max AS 'es.pic.hadoop.udf.array.UDAFArrayMax';
CREATE FUNCTION array_sum AS 'es.pic.hadoop.udf.array.UDAFArraySum';
CREATE FUNCTION array_count AS 'es.pic.hadoop.udf.array.UDAFArrayCount';
CREATE FUNCTION array_avg AS 'es.pic.hadoop.udf.array.UDAFArrayAverage';
CREATE FUNCTION array_stddev_pop AS 'es.pic.hadoop.udf.array.UDAFArrayStdPop';
CREATE FUNCTION array_stddev_samp AS 'es.pic.hadoop.udf.array.UDAFArrayStdSample';
CREATE FUNCTION array_variance AS 'es.pic.hadoop.udf.array.UDAFArrayVariancePop';
CREATE FUNCTION array_var_pop AS 'es.pic.hadoop.udf.array.UDAFArrayVariancePop';
CREATE FUNCTION array_var_samp AS 'es.pic.hadoop.udf.array.UDAFArrayVarianceSample';
```
