#!/usr/bin/env python
import argparse
import re
import os
import logging
import boto3
from datetime import datetime


def main(isl_bucket, isl_prefix, count=10):
    s3_client = boto3.client("s3")
    source_dat_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake.dat")
    source_emd_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake.emd")
    with open(source_emd_file) as f:
        emd_body = f.read()
    apid = 980  # 980 or 990
    t = 734432789.000
    for i in range(count):
        dt = datetime.fromtimestamp(t)
        sclk_seconds = f"{int(dt.timestamp()):010}"
        sclk_subseconds = f"{int(dt.microsecond):06}"[0:5]
        fake_dat_file = f"{apid:04}_{sclk_seconds}_{sclk_subseconds}-1.dat"
        fake_emd_file = f"{apid:04}_{sclk_seconds}_{sclk_subseconds}-1.emd"
        new_emd_body = emd_body.replace("__SCLK_SECONDS__", sclk_seconds).replace("__SCLK_SUBSECONDS__", sclk_subseconds)    
        with open(fake_emd_file, "w") as f:
            f.write(new_emd_body)
        try:
            resp = s3_client.upload_file(fake_emd_file, isl_bucket, os.path.join(isl_prefix, fake_emd_file)) 
        except ClientError as e:
            logging.error(e)
        os.unlink(fake_emd_file)
        try:
            resp = s3_client.upload_file(source_dat_file, isl_bucket, os.path.join(isl_prefix, fake_dat_file)) 
        except ClientError as e:
            logging.error(e)
        t += .001


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage fake EDRgen inputs to S3 bucket to kick off processing."
    )
    parser.add_argument("isl_bucket", help="ISL bucket name")
    parser.add_argument("isl_prefix", help="ISL bucket prefix")
    parser.add_argument(
        "-c", "--count", type=int, default=10, help="number of fake products to stage"
    )
    parser.add_argument(
        "-l", "--loglevel", default="warning", help="Provide logging level"
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    main(args.isl_bucket, args.isl_prefix, args.count)
