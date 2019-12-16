# Import astroquery.mast (http://astroquery.readthedocs.io/en/latest/mast/mast.html)
# Note, you may need to build from source to access the HST data on AWS.
import time

from astroquery.mast import Observations
import boto3
import json


# Use AWS S3 URLs for the MAST records (rather than the ones at http://mast.stsci.edu)
Observations.enable_cloud_dataset(provider='AWS', profile='ndmiles_admin')

# Query MAST for some ACS/WFC data
query_parameters = {
    'obs_collection':'HST',
    'dataproduct_type': ['image'],
    'instrument_name': 'ACS/WFC',
    'filters': 'F814W'
}
obsTable = Observations.query_criteria(**query_parameters)

# Grab 100 products:
# http://astroquery.readthedocs.io/en/latest/mast/mast.html#getting-product-lists
products = Observations.get_product_list(obsTable[:10])

# Filter out just the drizzled FITS files
filtered_products = Observations.filter_products(products,
                                                 mrp_only=False,
                                                 productSubGroupDescription='FLC')

# We want URLs like this: s3://stpubdata/hst/public/ibg7/ibg705080/ibg705081_drz.fits
s3_urls = Observations.get_cloud_uris(filtered_products)

# Auth to create a Lambda function
session = boto3.Session(profile_name='ndmiles_admin')
client = session.client('s3', region_name='us-east-1')

st = time.time()
for url in s3_urls:
    fits_s3_key = url.replace("s3://stpubdata/", "")
    event = {
        'fits_s3_key': fits_s3_key,
        'fits_s3_bucket': 'stpubdata',
        's3_output_bucket': 'compute-sky-lambda'

    }

    # Invoke Lambda function
    response = client.invoke(
        FunctionName='compute_sky',
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(event)
    )
et = time.time()
print(f"Duration: {st - et:0.2f}")