package actions

import (
	"fmt"
	"time"
	"sort"
	"github.com/jinzhu/now"
	"github.com/aws/aws-sdk-go/aws"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"

)

type BucketEntry struct {
	Key          string
	ModifiedTime time.Time
}

func RotateFiles(svc *s3.S3, bucket string, policy rpolicy.RotationPolicy) {
	fmt.Println(`
	######################################
	#  GoS3GFSBackup Rotation Started!   #
	######################################
	`)

	log.Info("starting GFS rotation")

	fmt.Println(`
	######################################
	#   Starting Daily Key Rotation!     #
	######################################
	`)

	// Daily rotation
	rotate(svc, bucket, policy.DailyRetentionPeriod, policy.DailyRetentionCount, policy.DailyPrefix)


	fmt.Println(`
	######################################
	#   Starting Weekly Key Rotation!    #
	######################################
	`)

	// Weekly rotation
	rotate(svc, bucket, policy.WeeklyRetentionPeriod, policy.WeeklyRetentionCount, policy.WeeklyPrefix)


	log.Info("finished GFS rotation")
}

func getKeyType(policy rpolicy.RotationPolicy, keyTime time.Time) string {
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

func retrieveSortedKeys(svc *s3.S3, bucket string, prefix string) ([]BucketEntry, error) {
	keys, err := getKeysByPrefix(svc, bucket, prefix)
	if err != nil {
		return nil, err
	}

	numKeys := len(keys)
	if numKeys == 0 {
		return nil, nil
	}
	return sortKeysByTime(keys), nil
}

// Sorts the bucket keys by the last modified time
// Returns a KeyTime array with the newest values first
func sortKeysByTime(keys map[string]time.Time) []BucketEntry {
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
func getKeysByPrefix(svc *s3.S3, bucket string, prefix string) (map[string]time.Time, error) {
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

func deleteKey(svc *s3.S3, bucket string, key string) (string, error) {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", err
	}

	return key, nil
}


