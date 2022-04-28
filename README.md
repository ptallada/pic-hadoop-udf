# Apache Hive UDFs

## HEALPix

### Conversion from/to sky coordinates
```
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
CREATE FUNCTION atan2 AS 'es.pic.hadoop.udf.healpix.UDFAtan2';
CREATE FUNCTION angdist AS 'es.pic.hadoop.udf.healpix.UDFAngDist';
```
## Misc
``` 
CREATE FUNCTION magnified_positions AS 'es.pic.hadoop.udf.healpix.UDFMagnifiedPositions';
CREATE FUNCTION hp_mw_theta AS 'es.pic.hadoop.udf.healpix.UDFMWtheta';
```
## Arrays
```
-- array_min
-- array_max
-- array_avg
CREATE FUNCTION array_sum AS 'es.pic.hadoop.udf.array.UDAFArraySum';
-- array_count
-- array_stdev
```
