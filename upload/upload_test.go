package upload

import (
	"os"
	"fmt"
	"time"
	"testing"
	"strconv"
	"strings"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
)

// Test variables
var svc *s3.S3
var bucket string
var s3FileName string
var policy rpolicy.RotationPolicy
var timeout time.Duration

var dailyRetentionCount int
var dailyRetentionPeriod time.Duration

var weeklyRetentionCount int
var weeklyRetentionPeriod time.Duration

var testFileName string
var pathToTestFile string
var testUploadObject UploadObject

var bigS3FileName string
var pathToBigFile string
var bigFileSize int64
var bigTestUploadObject UploadObject

var aws_forbidden_bucket string

// Setup testing
func init() {
	log.Init(os.Stdout, os.Stdout, os.Stderr)

	aws_credentials := os.Getenv("AWS_CRED_FILE")
	aws_profile := os.Getenv("AWS_PROFILE")
	aws_region := os.Getenv("AWS_REGION")
	aws_bucket := os.Getenv("AWS_BUCKET")
	aws_forbidden_bucket = os.Getenv("AWS_FORBIDDEN_BUCKET")

	s3svc, err := s3client.CreateS3Client(aws_credentials, aws_profile, aws_region)
	if err != nil {
		log.Error.Println(err)
		os.Exit(1)
	}

	svc = s3svc

	bucket = aws_bucket

	dailyRetentionCount = 6
	dailyRetentionPeriod = 140

	weeklyRetentionCount = 4
	weeklyRetentionPeriod = 280

	s3FileName = "test_file"
	timeout = time.Second * 3600

	testFileName = "testBackupFile"
	pathToTestFile = "../" + testFileName

	testUploadObject = UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	err = util.CreateFile(pathToTestFile, []byte("this is just a little test file"))
	if err != nil {
		log.Error.Println("failed to create file required for testing")
		os.Exit(1)
	}

	bigFileSize = int64(250 * 1024 * 1024) // 250MiB
	bigS3FileName = "bigS3File250MB"
	pathToBigFile = "../" + bigS3FileName

	bigTestUploadObject = UploadObject{
		PathToFile: pathToBigFile,
		S3FileName: bigS3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	err = util.CreateBigFile(pathToBigFile, bigFileSize)
	if err != nil {
		log.Error.Println("failed to create file required for testing")
		os.Exit(1)
	}

	// Not critical to run this up but can get costly if no lifecycle policy in place to clean up dead multiparts
	util.CleanUpMultiPartUploads(svc, bucket)

	policy = rpolicy.RotationPolicy{
		DailyRetentionPeriod:   time.Second * dailyRetentionPeriod,
		DailyRetentionCount:    dailyRetentionCount,
		DailyPrefix:            "daily_",
		WeeklyRetentionPeriod:  time.Second * weeklyRetentionPeriod,
		WeeklyRetentionCount:   weeklyRetentionCount,
		WeeklyPrefix:           "weekly_",
		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: false,
	}
}

//----------------------------------------------
//
//                Upload Tests
//
//----------------------------------------------

//----------------------------------------------
//
// Positive Upload Testing
//	1: Upload a single file
//	2: Upload a single file with justUploadIt set to true
//	3: Upload 50 files
//	4: Upload a Significantly Large File (250MiB)
//	5: Attempt to upload a file with dry run set to true
//	6: Upload file with bucket dir specified
//
//----------------------------------------------

// Test 1 - Positive Upload Testing
//	Upload a Single File
func TestUploadSingleFile(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	s3FileName, err := UploadFile(svc, testUploadObject, policy, time.Now(), false, false)
	if err != nil {
		t.Error(fmt.Sprintf("expected to upload single file without any error: %v", err))
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(s3FileName, bucketContents) {
		t.Error("exepected to find key in bucket: " + s3FileName)
	}

}

// Test 2 - Positive Upload Testing
//	Upload a single file with justUploadIt set to true
func TestJustUploadIt(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	_, err = UploadFile(svc, testUploadObject, policy, time.Now(), true, false) // Set justUploadIt to true
	if err != nil {
		t.Error("expected to upload single file without any error")
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(s3FileName, bucketContents) { // Notice no modification to file name for just upload it
		t.Error("exepected to find key in bucket: " + s3FileName)
	}
}

// Test 3 - Positive Upload Testing
//	Upload 50 Files
func TestUpload50Files(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	testUploadMultipleObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	bucketKeys := []string{}

	for i := 0; i < 50; i++ {
		testUploadMultipleObject.S3FileName = s3FileName + strconv.Itoa(i)
		bucketKey, err := UploadFile(svc, testUploadMultipleObject, policy, time.Now(), false, false)
		if err != nil {
			t.Error(fmt.Sprintf("expected to successfully upload file '%d' in bulk upload of 50 files", i))
		}
		bucketKeys = append(bucketKeys, bucketKey)
		time.Sleep(1) // Ensure there is a small delay between uploading files
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 50) {
		t.Error("expected bucket size to be 50")
	}

	for _, bucketKey := range bucketKeys {
		if !util.FindKeyInBucket(bucketKey, bucketContents){
			t.Error("expected to find key in bucket: " + bucketKey)
		}
	}
}

// Test 4 - Positive Upload Testing
//	Upload a Significantly Large File (250MiB)
func TestUpload250MBFile(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	bucketKey, err := UploadFile(svc, bigTestUploadObject, policy, time.Now(), false, false)
	if err != nil {
		t.Error(fmt.Sprintf("failed to upload big file of size: %v bytes", bigFileSize))
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(bucketKey, bucketContents){
		t.Error("expected to find key in bucket: " + bucketKey)
	}
}

// Test 5 - Positive Upload Testing
//	Upload a file with dry run set to run
func TestUploadWithDryRun(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	_, err = UploadFile(svc, testUploadObject, policy, time.Now(), false, true)
	if err != nil {
		t.Error(fmt.Sprintf("failed to upload big file of size: %v bytes", bigFileSize))
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 0) {
		t.Error("expected bucket size to be 0")
	}
}

// Test 6 - Positive Upload Testing
//	Upload a file with bucket dir specified
func TestUploadBucketDir(t *testing.T){
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	testUploadBucketDirObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "testdir/",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	bucketKey, err := UploadFile(svc, testUploadBucketDirObject, policy, time.Now(), false, false)

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(bucketKey, bucketContents){
		t.Error("expected to find key in bucket: " + bucketKey)
	}
}

func TestJustUploadItWithBucket(t *testing.T){

}

//----------------------------------------------
// Negative Testing
// 	1: Upload a file where the bucket has not been specified
//	2. Upload a file where the bucket has an invalid name
//	3: Upload a file that does not exist
//	4: Upload a file to a bucket without the appropriate permissions
//	5: Upload a file that exceeds the specified timeout period (60 seconds)
//
//----------------------------------------------

// Test 1 - Negative Upload Testing
//	Upload a file where the bucket has not been specified
func TestUploadInvalidBucketNotSpecified(t *testing.T) {
	expectedErrString := "invalid bucket specified, bucket must be specified"

	testUploadNoBucketObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     "",
		Timeout:    timeout,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadNoBucketObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected upload to fail with system unable to find specified file")
	}
}

// Test 2 - Negative Upload Testing
//	Upload a file where the bucket has an invalid name
func TestUploadInvalidBucketBadName(t *testing.T) {
	expectedErrString := "status code: 400"

	testUploadInvalidBucketObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     "badbucket*?",
		Timeout:    timeout,
		NumWorkers: 5,
	}
	_, err := UploadFile(svc, testUploadInvalidBucketObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected upload to fail with system unable to find specified file")
	}
}

// Test 3 - Negative Upload Testing
//	Upload a file that does not exist
func TestUploadInvalidFile(t *testing.T) {
	expectedErrString := "The system cannot find the file specified"

	testUploadInvalidPathObject := UploadObject{
		PathToFile: "../this/should/../definitely/../../notexist",
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadInvalidPathObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected upload to fail with system unable to find specified file")
	}
}

// Test 4 - Negative Upload Testing
//	Upload a file to a bucket without the appropriate permissions
func TestUploadForbiddenBucket(t *testing.T) {
	expectedErrString := "status code: 403"

	testUploadBadPermissionObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     aws_forbidden_bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadBadPermissionObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected upload to fail with status code 403")
	}
}

// Test 5 - Negative Upload Testing
//	Upload a file that exceeds the specified timeout period (60 seconds)
func TestUploadExceedTimeout(t *testing.T) {

	testUploadTimeoutObject := UploadObject{
		PathToFile: pathToBigFile,
		S3FileName: bigS3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    time.Second * 60,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadTimeoutObject, policy, time.Now(), false, false)

	if err == nil && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Error(fmt.Sprintf("expected file upload to timeout. timeout specified was: %d seconds", timeout))
	}

}

// Test 6 - Negative Upload Testing
//	Upload a file with an invalid bucket directory
func TestUploadInvalidBucketDir(t *testing.T){
	expectedErrString := "expected bucket dir to have trailing slash"

	testUploadBadObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "badbucketdir",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadBadObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected error due to bucket dir not including a trailing slash")
	}
}

// Test 7 - Negative Upload Testing
//	Upload a file with negative workers

func TestUploadInvalidWorkers(t *testing.T){
	expectedErrString := "concurrent workers should not be less than 1"

	testUploadBadObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 0,
	}

	_, err := UploadFile(svc, testUploadBadObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected error due to invalid number of workers specified")
	}
}

// Test 8 - Negative Upload Testing
//	Upload a file with no path specified
func TestUploadNoPathToFile(t *testing.T){
	expectedErrString := "path to file should not be empty and must include the full path to the file"

	testUploadBadObject := UploadObject{
		PathToFile: "",
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadBadObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected error as no path to file specified")
	}
}

// Test 9 - Negative Upload Testing
//	Upload a file with negative timeout
func TestUploadNegativeTimeout(t *testing.T){
	expectedErrString := "timeout must not be less than 0"

	testUploadBadObject := UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    time.Second * time.Duration(-1),
		NumWorkers: 5,
	}

	_, err := UploadFile(svc, testUploadBadObject, policy, time.Now(), false, false)
	if err != nil && strings.Contains(err.Error(), expectedErrString) {
		// Pass
	} else {
		t.Error("expected error when timeout less than 0")
	}
}
