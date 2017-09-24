package actions

import (
	"os"
	"fmt"
	"time"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
)

func UploadFile(pathToFile string, svc *s3.S3, s3FileName string, timeout time.Duration, bucket string, policy rpolicy.RotationPolicy, keyTime time.Time) (string, error) {
	fmt.Println(`
	######################################
	#         File Upload Started        #
	######################################
	`)

	//context provides a timeout with AWS SDK calls 'WithContext'
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	defer cancelFn()

	file, err := os.Open(pathToFile)

	if err != nil {
		return "", err
	}

	log.Info(fmt.Sprintf("uploading %s to s3", pathToFile))

	prefix := getKeyType(policy, keyTime)

	//mutate the file name into a manageable one
	s3FileName = fmt.Sprintf("%s%s_%s", prefix, s3FileName, time.Now().Format("20060102T150405"))

	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    aws.String(s3FileName),
		Bucket: aws.String(bucket),
		Body:   file,
	})

	if err != nil {
		return "", err
	}

	log.Info(fmt.Sprintf("successfully uploaded '%s' to s3 bucket: '%s' as '%s'", pathToFile, bucket, s3FileName))

	return s3FileName, nil

}

// This function is only to be used in a dire situation! i.e. This script isn't functioning as expected
func JustUploadFile(fileToUpload string, svc *s3.S3, s3FileName string, timeout time.Duration, bucket string) {
	fmt.Println(`
	######################################
	#     Just Upload File Started!      #
	######################################
	`)

	//context provides a timeout with AWS SDK calls 'WithContext'
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	defer cancelFn()

	file, err := os.Open(fileToUpload)

	if err != nil {
		log.Fatal(fmt.Sprintf("failed to read backup file %v", err))
	}

	log.Info(fmt.Sprintf("uploading %s to s3", fileToUpload))

	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    aws.String(s3FileName),
		Bucket: aws.String(bucket),
		Body:   file,
	})

	if err != nil {
		log.Fatal(fmt.Printf("failed to upload object, %v\n", err))
	}

	log.Info("successfully uploaded file")

}
