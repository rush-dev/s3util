import sys, os
from boto3.session import Session, Config
from botocore.exceptions import ClientError
import logging
import logging.config
import boto3
import argparse

# Credentials
region = 'us-east-1'
bucket = 'bucket_name'
key_prefix = 'glacier/frozen/'
user = 'ecs_s3_user'
host = 'http://hostname.com'
secret = 'secret_here'

# argeparser arguments
p = argparse.ArgumentParser(description="s3 utility to list, upload, delete, and move objects.")
p.add_argument(
    '-l',
    '--list',
    action='store',
    help="list all of the objects in a s3 bucket",
    dest="list",
    metavar="bucket_name",
    type=str,
    required=False
)

p.add_argument(
    '-d',
    '--download',
    action='store',
    help="downloads a single file from a s3 bucket locally",
    dest="download",
    metavar="file_name",
    type=str,
    required=False
)

p.add_argument(
    '-del',
    '--delete',
    action='store',
    help="deletes a single file from s3",
    dest="delete",
    metavar="file_name",
    type=str,
    required=False
)

p.add_argument(
    '-u',
    '--upload',
    action='store',
    help="uploads a single file from s3",
    dest="upload",
    metavar="file_name",
    type=str,
    required=False
)

args = p.parse_args()

# Creating s3 client objects
client = Session(
    aws_access_key_id = user,
    aws_secret_access_key=secret,
)

# Connecting to s3
s3 = client.resource('s3', endpoint_url=host, verify=False)
bucket_name = s3.Bucket(bucket)

# List all objects in a bucket
def list_objects():
    list_bucket = args.list
    try:
        bucket_name = s3.Bucket(list_bucket)
        for line in bucket_name.objects.all():
            print(line.key)

    except ClientError as e:
        print(f"\n{e}")

# Single Download of file
def download_file(file_name):
    s3_file = args.download.split("/")
    s3_file = s3_file[-1]
    dl_location = input("Enter full path of download location: ")
    dl_location = dl_location.replace("\\", "\\\\")

    if dl_location[-2:] != "\\\\":
        dl_location = dl_location + "\\\\"

    bucket_name.download_file(args.download, f'{dl_location}{s3_file}')

# Single deletion of a file
def delete_file(file_name):
    s3.Object(bucket, args.delete).delete()

# Upload a single file to ESC/S3
def single_upload(file_name):
    local_file = file_name.split("/")
    local_file = local_file[-1]
    s3.meta.client.upload_file(file_name, bucket, key_prefix + local_file)

# Recursive downloading of all directories in a bucket
def download_all_files():
    for s3_object in bucket_name.objects.all():
        path, filename = os.path.splt(s3_object.key)
        bucket_name.download_file(s3.object.key, filename)

# Recursive Download of specific directories
def download_specific_dir(dir_nane):
    for s3_object in bucket_name.objects.filter(Prefix='dir_prefix_here'):
        path, filename = os.path.split(s3_object.key)
        bucket_name.download_file(s3_object.key, 'directory\\name\\here\\%s' % filename)

# Main
def main():
    if args.list:
        print(f'Objects found in {bucket}:')
        list_objects()

    elif args.download:
        try:
            print("Downloading...")
            download_file(args.download)
            print("Download Complete")
        except ClientError as e:
            if e.response['Error']['Message'] == "Not Found":
                print("ERROR!")
                print("Filename/path entered did not return a valid object.")

    elif args.delete:
        try:
            print("Deleting from s3 bucket...")
            delete_file(args.delete)
            print("Delete Complete")
        except ClientError as e:
            if e.response['Error']['Message'] == "Not Found":
                print("ERROR!")
                print("Filename/path entered did not return a valid object.")


if __name__ == '__main__':
    main()

















