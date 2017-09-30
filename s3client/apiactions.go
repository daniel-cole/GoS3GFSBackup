package s3client

import (
	"time"
	"sort"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type BucketEntry struct {
	Key          string
	ModifiedTime time.Time
}

// Sorts the bucket keys by the last modified time
// Returns a KeyTime array with the newest values first
func SortKeysByTime(keys map[string]time.Time) []BucketEntry {
	var sortedBucketEntry []BucketEntry
	for k, v := range keys {
		sortedBucketEntry = append(sortedBucketEntry, BucketEntry{k, v})
	}

	sort.Slice(sortedBucketEntry, func(i, j int) bool {
		return sortedBucketEntry[i].ModifiedTime.After(sortedBucketEntry[j].ModifiedTime)
	})

	return sortedBucketEntry
}

// Returns a map of keys in the bucket along with the LastModified attribute
// The map consists of Map[AWS Bucket Key] -> LastModifiedTime
func GetKeysByPrefix(svc *s3.S3, bucket string, prefix string) (map[string]time.Time, error) {
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	keys := make(map[string]time.Time)

	// Loop over each object found in the bucket with the specified prefix
	for _, key := range result.Contents {
		keys[*key.Key] = *key.LastModified
	}

	return keys, nil
}

func DeleteKey(svc *s3.S3, bucket string, key string) (string, error) {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", err
	}

	return key, nil
}

func GetAllMultiPartUploads(svc *s3.S3, bucket string) (map[string]string, error) {
	resp, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return nil, err
	}

	multiPartUploadKeys := make(map[string]string)

	for _, multiPartUpload := range resp.Uploads {
		multiPartUploadKeys[*multiPartUpload.Key] = *multiPartUpload.UploadId
	}

	return multiPartUploadKeys, nil
}

func GetMultiPartUploadIDByKey(svc *s3.S3, bucket string, key string) (string, error) {
	resp, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key), // Prefix is the entire key
	})

	if err != nil {
		return "", err
	}

	if len(resp.Uploads) != 1 {
		return "", errors.New("expected no more than one return value when getting multipart uploadId by key")
	}

	return *resp.Uploads[0].UploadId, nil
}

func AbortAllMultiPartUploads(svc *s3.S3, bucket string, key string, uploadId string) error {
	_, err := svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})

	if err != nil {
		return err
	}

	return nil

}

func GetNumMultiPartsById(svc *s3.S3, bucket string, key string, uploadId string) (int64, error) {
	resp, err := svc.ListParts(&s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})
	if err != nil {
		return -1, err
	}
	numParts := len(resp.Parts)

	return int64(numParts), nil
}


// Returns the entire contents of the target bucket
func GetBucketContents(svc *s3.S3, bucket string) (*s3.ListObjectsOutput, error) {
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
