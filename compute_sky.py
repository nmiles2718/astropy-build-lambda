#!/usr/bin/env python


import glob

from astropy.table import Table
from astropy.io import fits
from astropy.stats import sigma_clipped_stats
from fits_handler import FitsHandler
# from acstools import calacs
import boto3


def download_file(event):
    fname = event['fits_s3_key']
    bucket_name = event['fits_s3_bucket']

    session = boto3.session(profile_name='ndmiles_admin')
    client = session.client('s3', region_name='us-east-1')
    client.download_file(bucket_name, fname, f"/tmp/{fname}" )

    return f"/tmp/{fname}"

def get_image_metadata(fitsobj):
    metadata = {}
    keys = ['targname', 'exptime', 'filter1','filter2', 'expstart']
    for key in keys:
        metadata[key] = fitsobj.prhdr[key]
    return metadata

def process_event(event):
    fname = download_file(event)
    basename = fname.split('/')[-1].split('_')[0]
    fitsobj = FitsHandler(fname=fname)
    fitsobj.get_data(ext='sci')
    fitsobj.get_data(ext='dq')
    metadata = get_image_metadata(fitsobj)
    units = fitsobj.chip1['sci1_hdr']['BUNIT']
    med = 0
    for chip in [fitsobj.chip1, fitsobj.chip2]:
        _, chip_med, _ = sigma_clipped_stats(
            chip['sci'][chip['dq']==0],
            sigma=5,
            maxiters=5
        )
        med += chip_med

    avg_bkg = med/2.0
    metadata[f"bkg_{units}"] = avg_bkg
    tb = Table(metadata)
    tb.write(f"/tmp/{basename}_sky.dat", format='ascii')

    session = boto3.session(profile_name='ndmiles_admin')
    client = session.client('s3', region_name='us-east-1')
    client.upload_file(
        f"/tmp/{basename}_sky.dat",
        event['s3_output_bucket'],
        f"/results/{basename}_sky.dat"
    )



def handler(event, context):
    print(event['s3_output_bucket'])
    print(event['fits_s3_key'])
    print(event['fits_s3_bucket'])
    process_event(event)



