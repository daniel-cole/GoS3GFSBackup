# GoS3GFSBackup
This is a custom take on the GFS backup strategy adopted for AWS S3 which is intended to be run on a daily basis to backup objects in S3.

The implementation uploads backups to S3 in the following way:
1. A monthly backup is taken on the first day of each month. A lifecycle policy to transition monthly objects should be implemented for objects with the 'monthly_' prefix. This utility does not handle rotation of monthly backups.
2. A weekly backup is taken every Monday (unless it's a monthly backup) with the prefix 'weekly_'. The maximum number of weekly backups kept by default is 4. When another weekly backup is created the oldest weekly backup is rotated.
3. A daily backup is taken once a day (unless it's a monthly or weekly backup) with the prefix 'daily_'. The maximum number of daily backups kept by default is 6. This ensures that 7 daily backups are kept as a weekly backup taken on Monday.

## CLI Arguments
./GoS3GFSBackup -h
```sh
Options:
  --credfile (required)     The full path to the AWS CLI credential file
  --region   (required)     The AWS region to upload the specified file to
  --bucket   (required)     The S3 bucket to upload the specified file to
  --bucketdir               The directory in the bucket in which to upload the S3 object to. Must include a trailing slash
  --pathtofile              The full path to the file to upload to the specified S3 bucket. Must be specified unless --rotateonly=true
  --s3filename              The name of the file as it should appear in the S3 bucket. Must be specified unless --rotateonly=true
  --timeout                 The timeout to upload the specified file (seconds). Defaults to 3600 (1 hour) [default: 3600]
  --profile                 The profile to use for the AWS CLI credential file. If none specified the default value will be used [default: default]
  --justuploadit            If this option is specified the file will be uploaded as is without the GFS backup strategy
  --rotateonly              If enabled then only GFS rotation will occur with no file upload. This is disabled by default
  --dryrun                  If enabled then no upload or rotation actions will be executed.
  --concurrentworkers       The number of threads to use when uploading the file to S3. Defaults to 5 [default: 5]
  --enforceretentionperiod  If enabled then objects in the S3 bucket will only be rotated if they are older then the retention period. Enabled by default [default: true]
  --dailyretentioncount     The number of daily objects to keep in S3. Defaults to 6 [default: 6]
  --dailyretentionperiod    The retention period (hours) that a daily object should be kept in S3. Defaults to 168 (7 days) [default: 168]
  --weeklyretentioncount    The number of weekly objects to keep in S3. Defaults to 4 [default: 4]
  --weeklyretentionperiod   The retention period (hours) that a weekly object should be kept in S3. Defaults to 672 (28 days) [default: 672]
```

## Examples
### Basic Usage (Upload and Rotate)
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=portfolioAlbum --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar
```

### Usage Custom Rotation Policy (10 daily backups, 5 weekly backups with enforced retention period applied)
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=portfolioAlbum --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar --enforceretentionperiod=true --dailyretentioncount=10 --dailyretentionperiod=240 --weeklyretentioncount=5 --weeklyretentionperiod=120
```

### Usage with 5 hour timeout
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=portfolioAlbum --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar --timeout=18000
```

### Upload Only (This does not alter the name of the upload)
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=myFileNameThatWontChangeInBucket --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar
```

### Rotation Only
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=portfolioAlbum --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar --rotateonly=true
```

### Dry run
```sh
./GoS3GFSBackup --credfile=~/.aws_creds --region=us-east-1 --bucket=mybucket --s3filename=portfolioAlbum --pathtofile=/var/tmp/uploads/portfolioAlbum2007.tar --dryrun=true
```


## Recommendations
1. This tool should be used with a lifecycle policy which moves objects to IA/Glacier to reduce costs of infrequently accessed objects. i.e. move to Glacier after 30 days
2. Replication between another bucket should be enabled for a greater level of redundancy. This is only if you are not constrained to a particular geographic location.


## Notes About Behaviour
1. An incomplete multipart upload object will be left in the S3 bucket if the upload fails due to a timeout. A policy should be set on the bucket to remove multipart upload objects after a certain period of time.

## Limitations
1. The progress tracking implemented for uploads is only to provide a rough idea of how the upload is progressing. There seems to be limitations around tracking the progress of a multipart upload using the AWS SDK for Go. 

## Testing

Run test suite with `go test -v ./...` in base directory of repository. Testing requires the following environment variables to be set:

```sh
AWS_REGION=<AWS Region>
AWS_PROFILE=<AWS Profile>
AWS_BUCKET=<AWS Bucket>
AWS_CRED_FILE=<Path to AWS credential file>
AWS_FORBIDDEN_BUCKET=<AWS bucket that user running tests does not have permission to access>
```