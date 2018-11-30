# UDFHealpix
 
Functions implemented

nside/npix/resolution
 - nest2ring ( int order, long pix )
 - ring2nest ( int order, long pix )
 - nside2npix ( int nSide )
 - npix2nside ( long nPix )
 - nside2order ( int nSide )
 
conversion from/to sky coordinates 
 - pix2ang (int order, long pix[, boolean nest, boolean lonlat])
 - pix2vec (int order, long pix[, boolean nest])
 - ang2pix ( int order, double theta, double phi[, boolean ring]) 
 - vec2pix (int order, float x, float y, float z[, boolean nest])
 - vec2ang (double x, double y, double z[, boolean lonlat])
 - ang2vec (double theta, double phi[, boolean lonlat])
 - neighbours (int order, long pix[, boolean nest])
 
rotator
 - dir2vec (double x, double y, double z[, boolean lonlat])
 - vec2dir (double theta, double phi[, boolean lonlat])
