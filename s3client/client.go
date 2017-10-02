package s3client

import (
	"os"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

// Creates an S3 client using:
// 1. Environment variables if present
// 2. Use the specified credential file
func CreateS3Client(credFile string, profile string, region string) (*s3.S3, error) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	session := session.Must(session.NewSession())

	var creds *credentials.Credentials

	log.Info.Println("")

	if accessKey == "" && secretAccessKey == "" {
		// Missing both of the required environment variables
		log.Info.Println("Environment variables missing to create client: 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'")
	} else if accessKey == "" {
		log.Info.Println("Environment variables missing: 'AWS_ACCESS_KEY_ID'")
	} else if secretAccessKey == "" {
		log.Info.Println("Environment variables missing: 'AWS_SECRET_ACCESS_KEY'")

	} else {
		log.Info.Println("Loaded AWS credentials from environment variables")
		creds = credentials.NewEnvCredentials()
	}

	if creds == nil {
		log.Info.Printf("Attempting to create S3 client with specified credential file and profile: [%s | %s]\n", credFile, profile)
		creds = credentials.NewSharedCredentials(credFile, profile);
	}

	if creds == nil {
		return nil, errors.New("failed to retrieve S3 client access key id and access key secret")
	}

	return s3.New(session, &aws.Config{Region: aws.String(region), Credentials: creds}), nil
}
