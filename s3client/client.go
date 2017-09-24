package s3client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

//creates an S3 client given a file which contains the
//access key and secret access key.
//credsFile: location to the file with AWS credentials
//profile: the user to create the S3 client with
func CreateS3Client(credsFile string, profile string, region string) *s3.S3 {
	sess := session.Must(session.NewSession())
	creds := credentials.NewSharedCredentials(credsFile, profile);
	return s3.New(sess, &aws.Config{Region: aws.String(region), Credentials: creds})
}