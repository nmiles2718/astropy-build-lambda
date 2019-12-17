#!/usr/bin/env python
import os

from astropy.table import Table
from astropy.stats import sigma_clipped_stats
import boto3
from fits_handler import FitsHandler


def download_file(event):
    fname = event['fits_s3_key']
    bucket_name = event['fits_s3_bucket']
    s3 = boto3.resource('s3')
    bkt = s3.Bucket(bucket_name)
    bkt.download_file(
        fname,
        f"/tmp/{os.path.basename(fname)}",
        ExtraArgs={"RequestPayer": "requester"}
    )
    return f"/tmp/{os.path.basename(fname)}"

def get_image_metadata(fitsobj):
    metadata = {}
    keys = ['targname', 'exptime', 'filter1','filter2', 'expstart', 'aperture']
    for key in keys:
        if key == 'filter1' and 'clear' not in fitsobj.prhdr[key].lower():
            metadata['filter'] = [fitsobj.prhdr[key]]
        elif key == 'filter2' and 'clear' not in fitsobj.prhdr[key].lower():
            metadata['filter'] = [fitsobj.prhdr[key]]

    return metadata

def clean_up(img, output):
    os.remove(img)
    os.remove(output)


def process_event(event):
    fname = download_file(event)
    basename = os.path.basename(fname).split('_')[0]
    fitsobj = FitsHandler(fname=fname)
    fitsobj.get_data(ext='sci')
    fitsobj.get_data(ext='dq')
    metadata = get_image_metadata(fitsobj)
    try:
        units = fitsobj.chip1['sci1_hdr']['BUNIT']
    except KeyError as e:
        try:
            units = fitsobj.chip1['sci2_hdr']['BUNIT']
        except KeyError as e:
            units = 'unknown'
    med = 0
    for chip in [fitsobj.chip1, fitsobj.chip2]:
        if chip['sci'] is None:
            continue

        if chip['dq'] is not None:
            _, chip_med, _ = sigma_clipped_stats(
                chip['sci'][chip['dq']==0],
                sigma=5,
                maxiters=5
            )
        else:
            _, chip_med, _ = sigma_clipped_stats(
                chip['sci'],
                sigma=5,
                maxiters=5
            )

        med += chip_med

    avg_bkg = med/2.0
    metadata[f"bkg_{units}"] = [avg_bkg]
    tb = Table(metadata)
    tb.write(f"/tmp/{basename}_sky.dat", format='ascii')

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(f"/tmp/{basename}_sky.dat",
                               event['s3_output_bucket'],
                               f"results/{basename}_sky.dat")


    # Now we delete the file we downloaded to ensure if memory is persisted
    # between Lambdas, we won't fill up our disk space.
    # https://stackoverflow.com/questions/48347350/aws-lambda-no-space-left-on-device-error
    clean_up(fname, f"/tmp/{basename}_sky.dat")



def handler(event, context):
    print(event['s3_output_bucket'])
    print(event['fits_s3_key'])
    print(event['fits_s3_bucket'])
    process_event(event)



