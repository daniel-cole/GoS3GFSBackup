package util

import (
	"regexp"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"time"
	"github.com/jinzhu/now"
	"github.com/aws/aws-sdk-go/aws"
	"errors"
	"os"
)

func CheckPrefix(key string, prefix string) bool {
	re := regexp.MustCompile("^" + prefix)
	return re.Match([]byte(key))
}

// Helpful function to get rid of all abandoned multipart uploads
func CleanUpMultiPartUploads(svc *s3.S3, bucket string) error {
	multiPartUploads, err := s3client.GetAllMultiPartUploads(svc, bucket)
	if err != nil {
		return err
	}
	for key, uploadId := range multiPartUploads {
		s3client.AbortAllMultiPartUploads(svc, bucket, key, uploadId)
	}
	return nil
}

// Helper function to get all sorted keys
func RetrieveSortedKeysByTime(svc *s3.S3, bucket string, prefix string) ([]s3client.BucketEntry, error) {
	keys, err := s3client.GetKeysByPrefix(svc, bucket, prefix)
	if err != nil {
		return nil, err
	}

	numKeys := len(keys)
	if numKeys == 0 {
		return nil, nil
	}
	return s3client.SortKeysByTime(keys), nil
}

func GetKeyType(policy rpolicy.RotationPolicy, keyTime time.Time) string {
	monthlyYear, monthlyMonth, monthlyDay := now.New(keyTime).BeginningOfMonth().Date()

	keyTimeYear, keyTimeMonth, keyTimeDay := keyTime.Date()

	if keyTimeYear == monthlyYear && monthlyMonth == keyTimeMonth && monthlyDay == keyTimeDay {
		// This is a monthly backup as it falls on the first day of the month
		return policy.MonthlyPrefix
	}

	if keyTime.Weekday() == time.Monday {
		// This is a weekly backup as it falls on a Monday
		return policy.WeeklyPrefix
	}

	// Every other backup will be daily
	return policy.DailyPrefix
}

// Given a specified key in the target bucket returns true if it exists; otherwise false
func FindKeyInBucket(keyToFind string, bucketContents *s3.ListObjectsOutput) bool {
	for _, key := range bucketContents.Contents {
		if *key.Key == keyToFind {
			return true
		}
	}
	return false
}

func FindKeysInBucketByPrefix(prefix string, bucketContents *s3.ListObjectsOutput) []string {
	keys := []string{}
	for _, key := range bucketContents.Contents {
		if CheckPrefix(*key.Key, prefix) {
			keys = append(keys, *key.Key)
		}
	}
	return keys
}

func EmptyBucket(svc *s3.S3, bucket string) error {
	result, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		return err
	}
	for _, key := range result.Contents {
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(*key.Key),
		})
		if err != nil {
			return err
		}
	}
	result, err = s3client.GetBucketContents(svc, bucket)

	if err != nil {
		return err
	}
	if len(result.Contents) > 0 {
		return errors.New("expected bucket contents to be 0 after emptying")
	}

	return nil
}

func CheckBucketSize(bucketContents *s3.ListObjectsOutput, expectedContentSize int) bool {

	bucketContentsLength := len(bucketContents.Contents)

	if bucketContentsLength != expectedContentSize {
		return false
	}
	return true

}

func CreateBigFile(pathToBigFile string, size int64) error {
	fd, err := os.Create(pathToBigFile)
	defer fd.Close()

	if err != nil {
		return err
	}
	_, err = fd.Seek(size-1, 0)
	if err != nil {
		return err
	}
	_, err = fd.Write([]byte{0}) // Write 500MB worth of null bits to the file
	if err != nil {
		return err
	}
	return nil
}

func CreateFile(pathToFile string, contents []byte) error {
	fd, err := os.Create(pathToFile)
	defer fd.Close()

	fd.Write(contents)

	if err != nil {
		return err
	}
	return nil
}
