# Import astroquery.mast (http://astroquery.readthedocs.io/en/latest/mast/mast.html)
# Note, you may need to build from source to access the HST data on AWS.
import time

from astroquery.mast import Observations
import boto3
import dask
import generate_catalog
import json



def find_and_process(
        obs_collection='HST',
        dataproduct_type = ('image'),
        instrument_name= 'ACS/WFC',
        filters='F814W'
):
    # Use AWS S3 URLs for the MAST records (rather than the ones at http://mast.stsci.edu)
    Observations.enable_cloud_dataset(profile='ndmiles_admin')

    # Query MAST for some ACS/WFC data
    query_parameters = {
        'obs_collection':obs_collection,
        'dataproduct_type': dataproduct_type,
        'instrument_name': instrument_name,
        'filters': filters
    }
    obsTable = Observations.query_criteria(**query_parameters)

    # Grab 100 products:
    # http://astroquery.readthedocs.io/en/latest/mast/mast.html#getting-product-lists
    products = Observations.get_product_list(obsTable['obsid'])

    # Filter out just the drizzled FITS files
    filtered_products = Observations.filter_products(products,
                                                     mrp_only=False,
                                                     productSubGroupDescription=['FLT'])

    # We want URLs like this: s3://stpubdata/hst/public/ibg7/ibg705080/ibg705081_drz.fits
    s3_urls = Observations.get_cloud_uris(filtered_products)

    # Auth to create a Lambda function
    session = boto3.Session(profile_name='ndmiles_admin')
    client = session.client('lambda', region_name='us-east-1')

    st = time.time()
    for url in s3_urls:
        fits_s3_key = url.replace("s3://stpubdata/", "")
        print(fits_s3_key)
        event = {
            'fits_s3_key': fits_s3_key,
            'fits_s3_bucket': 'stpubdata',
            's3_output_bucket': 'compute-sky-lambda'

        }
        Payload = json.dumps(event)
        lambda_inputs= {
            'FunctionName':'compute_sky',
            'InvocationType': 'Event',
            'LogType':'Tail',
            'Payload': Payload
        }
        response = client.invoke(**lambda_inputs)

    et = time.time()
    print(f"Duration: {et - st:0.2f}")


def process_catalog(catalog_name):
    path_urls = [val.strip('\n') for val in open(catalog_name).readlines()]
    # Auth to create a Lambda function
    session = boto3.Session(profile_name='ndmiles_admin')
    client = session.client('lambda', region_name='us-east-1')
    delayed_objs = []
    st = time.time()
    for url in path_urls:
        event = {
            'fits_s3_key': url,
            'fits_s3_bucket': 'stpubdata',
            's3_output_bucket': 'compute-sky-lambda'
        }
        Payload = json.dumps(event)
        lambda_inputs = {
            'FunctionName': 'compute_sky',
            'InvocationType': 'Event',
            'LogType': 'Tail',
            'Payload': Payload
        }
        delayed_objs.append(dask.delayed(client.invoke)(**lambda_inputs))
    dask.compute(*delayed_objs, scheduler='threads', num_workers=2)
    et = time.time()
    print(f"Duration: {et - st:0.2f}")
