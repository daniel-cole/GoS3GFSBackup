package actions

import (
	"fmt"
	"regexp"
	"time"
	"testing"
	"path/filepath"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"os"
)

// variables for all tests
var svc *s3.S3
var bucket string
var s3FileName string
var policy rpolicy.RotationPolicy
var timeout time.Duration


func init() {
	aws_creds := os.Getenv("AWS_CRED_FILE")
	aws_profile := os.Getenv("AWS_PROFILE")
	aws_region := os.Getenv("AWS_REGION")
	aws_bucket := os.Getenv("AWS_BUCKET")
	svc = s3client.CreateS3Client(aws_creds, aws_profile, aws_region)
	bucket = aws_bucket

	policy = rpolicy.RotationPolicy{
		DailyRetentionPeriod: time.Second * 140,
		DailyRetentionCount:  6,
		DailyPrefix:          "daily_",

		WeeklyRetentionPeriod: time.Second * 280,
		WeeklyRetentionCount:  4,
		WeeklyPrefix:          "weekly_",

		MonthlyPrefix: "monthly_",
	}
	s3FileName = "test_file"
	timeout = time.Second * 3600
}



//----------------------------------------------
//
//                  Tests
//
//----------------------------------------------

// Upload one file on a Tuesday
// This should result in the file being prefixed with 'daily_'
func TestFirstDailyUpload(t *testing.T) {
	emptyBucket(t)

	// Tuesday - should result in a daily backup
	uploadDate := time.Date(2017, time.September, 19, 01, 0, 0, 0, time.UTC)

	dailyKey := runMockBackup(t, uploadDate, "")

	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 1) // the first backup will create a daily, weekly and monthly base

	if !findKeyInBucket(dailyKey, bucketContents){
		t.Error(fmt.Sprintf("expected key: '%s' to exist in bucket", dailyKey))
	}

	if !checkPrefix(dailyKey, policy.DailyPrefix){
		t.Error(fmt.Sprintf("expected key '%s' to be prefixed with: '%s'", dailyKey, policy.DailyPrefix))
	}

}


// Upload one file on a Monday
// This should result in the file being prefixed with 'weekly_'
func TestFirstWeeklyUpload(t *testing.T){
	emptyBucket(t)

	//Monday - should be a weekly backup
	uploadDate := time.Date(2017, time.September, 18, 01, 0, 0, 0, time.UTC)

	weeklyKey := runMockBackup(t, uploadDate, "")

	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 1)

	if !findKeyInBucket(weeklyKey, bucketContents){
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", weeklyKey))
	}

	if !checkPrefix(weeklyKey, policy.WeeklyPrefix){
		t.Error(fmt.Sprintf("expected key: '%s' to be prefixed with: '%s'", weeklyKey, policy.WeeklyPrefix))
	}
}

// Upload one file on the first of the month
// This should result in it being prefixed with 'monthly_'
func TestFirstMonthlyUpload(t *testing.T){
	emptyBucket(t)

	//First of the month - should be a monthly backup
	uploadDate := time.Date(2017, time.September, 01, 01, 0, 0, 0, time.UTC)

	monthlyKey := runMockBackup(t, uploadDate, "")

	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 1)

	if !findKeyInBucket(monthlyKey, bucketContents){
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", monthlyKey))
	}

	if !checkPrefix(monthlyKey, policy.MonthlyPrefix){
		t.Error(fmt.Sprintf("expected key: '%s' to be prefixed with: '%s'", monthlyKey, policy.MonthlyPrefix))
	}
}


// Test a full week of backups from Monday to Sunday
func TestFullWeekUpload(t *testing.T){
	emptyBucket(t)

	uploadDate := time.Date(2017, time.September, 04, 01, 0, 0, 0, time.UTC) // Monday September 4 2017
	weeklyKey := runMockBackup(t, uploadDate, "")

	dailyKeys := []string{}

	uploadDate = uploadDate.Add(time.Hour*24) //Tuesday 5th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	uploadDate = uploadDate.Add(time.Hour*24) //Wednesday 6th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	uploadDate = uploadDate.Add(time.Hour*24) //Thursday 7th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	uploadDate = uploadDate.Add(time.Hour*24) //Friday 8th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	uploadDate = uploadDate.Add(time.Hour*24) // Saturday 9th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	uploadDate = uploadDate.Add(time.Hour*24) // Sunday 10th
	dailyKeys = append(dailyKeys, runMockBackup(t, uploadDate, ""))

	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 7)

	for _, dailyKey := range dailyKeys {
		if !findKeyInBucket(dailyKey, bucketContents){
			t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", dailyKey))
		}
	}

	if !findKeyInBucket(weeklyKey, bucketContents){
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", weeklyKey))
	}

}

// The weekly backup will be rotated as soon as a 5th weekly backup exists
// This can be delayed if the weekly backup falls on the first of the month
// Where a monthly backup will be taken instead
func TestWeeklyRotation(t *testing.T){
	emptyBucket(t)

	uploadDate := time.Date(2017, time.September, 01, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, uploadDate.Format("02-Jan-06"))

	weeklyBackupKeys := make(map[int]string)

	day := 2
	for day <= 25 { // Ends at September 25 2017
		// Once this loop has completed, 4 weekly backups should exist
		uploadDate = uploadDate.Add(time.Hour*24)
		d := uploadDate.Format("02-Jan-06")
		// These days represent each Monday in September
		if day == 4 || day == 11 || day == 18 || day == 25 {
			weeklyBackupKeys[day] = runMockBackup(t, uploadDate, d)
		} else {
			runMockBackup(t, uploadDate, d)
		}
		day++
	}

	// Verify that the 4 weekly backups have been taken and exist in the bucket
	bucketContents := getBucketContents(t)
	if len(findKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for _, key := range weeklyBackupKeys {
		if !findKeyInBucket(key, bucketContents){
			t.Error("expected to find key in bucket: " + key)
		}
	}

	// This will trigger oldest weekly backup to be removed from the bucket
	for day <= 32 { // Ends at October 2 2017
		uploadDate = uploadDate.Add(time.Hour*24)
		d := uploadDate.Format("02-Jan-06")
		// These days represent each Monday in September
		if day == 32 {
			weeklyBackupKeys[day] = runMockBackup(t, uploadDate, d)
		} else {
			runMockBackup(t, uploadDate, d)
		}
		day++
	}

	bucketContents = getBucketContents(t)
	if len(findKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for index, key := range weeklyBackupKeys {
		if index == 4 {
			if findKeyInBucket(key, bucketContents){
				t.Error("found unexpected key in bucket: " + key)
			}
		} else {
			if !findKeyInBucket(key, bucketContents) {
				t.Error("expected to find key in bucket: " + key)
			}
		}
	}

	for day <= 39 { // Ends at October 9 2017
		uploadDate = uploadDate.Add(time.Hour*24)
		d := uploadDate.Format("02-Jan-06")
		// These days represent each Monday in September
		if day == 39 {
			weeklyBackupKeys[day] = runMockBackup(t, uploadDate, d)
		} else {
			runMockBackup(t, uploadDate, d)
		}
		day++
	}

	bucketContents = getBucketContents(t)
	if len(findKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for index, key := range weeklyBackupKeys {
		if index == 11 || index == 4 { // Should have been moved after rotation
			if findKeyInBucket(key, bucketContents){
				t.Error("found unexpected key in bucket: " + key)
			}
		} else {
			if !findKeyInBucket(key, bucketContents) {
				t.Error("expected to find key in bucket: " + key)
			}
		}
	}

}

// Test a full week of backups from Monday to Sunday
func TestFullThirtyDaysUpload(t *testing.T){
	emptyBucket(t)

	uploadDate := time.Date(2017, time.September, 01, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, uploadDate.Format("02-Jan-06"))

	for day := 2; day <= 30; day++ { // September 30
		uploadDate = uploadDate.Add(time.Hour*24)
		d := uploadDate.Format("02-Jan-06")
		runMockBackup(t, uploadDate, d)
	}


	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 11) // 6 daily, 4 weekly, 1 monthly

}

// Test a full week of backups from Monday to Sunday
func TestFullNinetyDaysUpload(t *testing.T){
	emptyBucket(t)

	uploadDate := time.Date(2017, time.September, 01, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, uploadDate.Format("02-Jan-06"))

	for day := 2; day <= 90; day++ { // November 30 2017
		uploadDate = uploadDate.Add(time.Hour*24)
		d := uploadDate.Format("02-Jan-06")
		runMockBackup(t, uploadDate, d)
	}


	bucketContents := getBucketContents(t)

	checkBucketSize(t, bucketContents, 13) // 6 daily, 4 weekly, 3 monthly

}

//----------------------------------------------
//
//      Helper functions for testing below
//
//----------------------------------------------


func checkBucketSize(t *testing.T, bucketContents *s3.ListObjectsOutput, expectedContentSize int) {

	bucketContentsLength := len(bucketContents.Contents)

	if bucketContentsLength != expectedContentSize {
		t.Error(fmt.Sprintf("expected bucket to have: %d keys but got %d keys", expectedContentSize, bucketContentsLength))
	}

}

func checkPrefix(key string, prefix string) bool {
	re := regexp.MustCompile("^"+prefix)
	return re.Match([]byte(key))
}

// Uploads the 'test_backup_file' file in the repository
// This also initiates a GFS rotation
func runMockBackup(t *testing.T, uploadDate time.Time, suffix string) string {
	testFileAbsPath, _ := filepath.Abs("../test_backup_file")
	s3FileName, err := UploadFile(testFileAbsPath, svc, s3FileName + suffix, timeout, bucket, policy, uploadDate)
	if err != nil {
		t.Fatal(fmt.Sprintf("failed to upload file: %v", err))
	}

	RotateFiles(svc, bucket, policy)

	// Add a small delay to deal with bulk uploads
	// time.Sleep(30*time.Second)

	return s3FileName
}


// Returns the entire contents of the target bucket
func getBucketContents(t *testing.T) *s3.ListObjectsOutput {
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatal(fmt.Sprintf("failed to list bucket contents: %v", err))
	}
	return result
}

// Given a specified key in the target bucket returns true if it exists; otherwise false
func findKeyInBucket(keyToFind string, bucketContents *s3.ListObjectsOutput) bool {
	for _, key := range bucketContents.Contents {
		if *key.Key == keyToFind {
			return true
		}
	}
	return false
}

func findKeysInBucketByPrefix(prefix string, bucketContents *s3.ListObjectsOutput) []string {
	keys := []string{}
	for _, key := range bucketContents.Contents {
		if checkPrefix(*key.Key, prefix) {
			keys = append(keys, *key.Key)
		}
	}
	return keys
}

func emptyBucket(t *testing.T) {
	fmt.Println("emptying bucket")

	result := getBucketContents(t)

	for _, key := range result.Contents {
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(*key.Key),
		})
		if err != nil {
			t.Error(fmt.Sprintf("failed to delete key from bucket: %s", key))
		}
	}
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		t.Error("failed to check bucket contents before executing tests")
	}
	if len(result.Contents) > 0 {
		t.Error("expected bucket contents to be empty after clearing")
	}
}
