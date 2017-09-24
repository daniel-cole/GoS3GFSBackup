# GoS3GFSBackup
This is a custom take on the GFS backup strategy adopted for AWS S3 which is intended to be run on a daily basis.

The implementation uploads backups to S3 in the following way:
1. A monthly backup is taken on the first day of each month. A lifecycle policy for Glacier should be implemented to transition backups with the 'monthly_' prefix. The utility does not handle rotation of monthly backups.
2. A weekly backup is taken every Monday (unless it's a monthly backup) with the prefix 'weekly_'. The maximum number of weekly backups kept is 4. When another weekly backup is created the oldest weekly backup is rotated.
3. A daily backup is taken once a day (unless it's a monthly or weekly backup) with the prefix 'daily_'. This maximum number of daily backups kept is 6. The 7th daily backup is the weekly backup taken on Monday. 

## CLI Arguments
| Argument | Description |
| --------------- | ------ |
| -\-credfile | full path to the AWS CLI credential file |
| -\-region | The AWS region to upload the specified file to |
| -\-bucket | The S3 bucket to upload the specified file to |
| -\-pathtofile | The full path to the file to upload to the specified S3 bucket |
| -\-s3filename | The name of the file as it should appear in the S3 bucket |
| -\-timeout | The timeout to upload the specified file (seconds) |
| -\-profile | The profile to use for the AWS CLI credential file. If none specified the default value will be used |
| -\-justuploadit | If this option is specified the file will be uploaded as is without the GFS backup strategy. --justuploadit=true
