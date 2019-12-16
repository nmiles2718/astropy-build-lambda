# This script queries MAST for WFC3 IR data and downloads the data from 
# the AWS public dataset rather than from MAST servers.

# Working with http://astroquery.readthedocs.io/en/latest/mast/mast.html
from astroquery.mast import Observations
import boto3
import os
import datetime

t_start = datetime.datetime.now()

# Read in the AWS credentials
import configparser
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.aws/credentials'))
os.environ["AWS_ACCESS_KEY_ID"] = config.get('default','aws_access_key_id')
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get('default','aws_secret_access_key')

# Create a directory for downloaded files
if not os.path.exists('DATA'):
  os.makedirs('DATA')

# This downloads all the F160W DRZ images from CANDELS in the GOODS-South field
print('\nQuerying the MAST archive.\n')
obsTable = Observations.query_criteria(obs_collection='HST',
                                       filters='F160W',
                                       instrument_name='WFC3/IR',
                                       proposal_id=['12062','12061','12062'])

# Grab the list of available data products for these observations
products = Observations.get_product_list(obsTable)

# Select only drizzled (DRZ) files
filtered = Observations.filter_products(products,
                                        mrp_only=False,
                                        productSubGroupDescription='DRZ')

# Enable 'S3 mode' for module which will return S3-like URLs for FITs files
# e.g. s3://stpubdata/hst/public/icde/icde43l0q/icde43l0q_drz.fits
Observations.enable_s3_hst_dataset()

# Grab the S3 URLs for each of the observations
s3_urls = Observations.get_hst_s3_uris(filtered)

print('Query returned {} entries.\n'.format(len(filtered)))

print('Downloading data from S3.\n')

s3 = boto3.resource('s3')

# Create an authenticated S3 session. Note, download within US-East is free
# e.g. to a node on EC2.
s3_client = boto3.client('s3')

bucket = s3.Bucket('stpubdata')

# For brevity, only download the first three images
for url in s3_urls[:3]:
  # Extract the S3 key from the S3 URL
  fits_s3_key = url.replace("s3://stpubdata/", "")
  root = url.split('/')[-1]
  if not os.path.exists('DATA/{}'.format(root)):
    bucket.download_file(fits_s3_key, 'DATA/{}'.format(root), ExtraArgs={"RequestPayer": "requester"})
    print('{} ---> DATA/{}'.format(fits_s3_key, root))
  else:
    print('DATA/{} already exists.'.format(root))
print('\n')

t_end = datetime.datetime.now()
duration = t_end - t_start

print('Done.\nDuration: {:6.2f} seconds.\n'.format(duration.total_seconds()))
