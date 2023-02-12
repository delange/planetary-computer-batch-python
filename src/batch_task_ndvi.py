import argparse
import numpy
import rasterio
import planetary_computer as pc


def do_ndvi(red_href, nir_href, output_file_name):
    """
    Write a tif file to local HD. Tif contains 1 band with NDVI values [-1:1] derived from input red and nir bands.

    :param str red_href: url to blob storage for image red band.
    :param str nir_href: url to blob storage for image NIR (near infra red) band.
    :param str output_file_name: name of the output file (tif) to be stored on local HD.
    """

    red_signed_href = pc.sign(red_href)
    nir_signed_href = pc.sign(nir_href)

    red_virtual_file = rasterio.open(red_signed_href)
    nir_virtual_file = rasterio.open(nir_signed_href)
    red = red_virtual_file.read()
    nir = nir_virtual_file.read()

    # get NDVI indexes
    ndvi = numpy.zeros(red_virtual_file.shape, dtype=rasterio.float32)
    ndvi = ((nir.astype('float32') - red.astype('float32')) / (nir.astype('float32') + red.astype('float32')))[0]

    kwargs = red_virtual_file.meta
    kwargs.update(
        dtype=rasterio.float32,
        count=1,
        compress='lzw')

    output_file_name_tif = output_file_name + ".tif"
    with rasterio.open(output_file_name_tif, 'w', **kwargs) as dst:
        dst.write_band(1,ndvi.astype(rasterio.float32))


if __name__ == '__main__':
    # Construct the argument parser
    ap = argparse.ArgumentParser()

    ap.add_argument("-r", "--red", required=True,
    help="url reference to red band, string")

    ap.add_argument("-n", "--nir", required=True,
    help="url reference to red band, string")

    # Add the arguments to the parser
    ap.add_argument("-o", "--outputfilename", required=True,
    help="output file name without extention, string")

    args = vars(ap.parse_args())

    red_href = args['red']
    nir_href = args['nir']
    output_file_name = args['outputfilename']


    output_file_name_tif = output_file_name + ".tif"

    do_ndvi(red_href, nir_href, output_file_name)
  