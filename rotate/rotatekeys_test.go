package rotate

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/upload"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

// Test variables
var svc *s3.S3
var bucket string

var policy rpolicy.RotationPolicy
var timeout time.Duration

var dailyRetentionCount int
var dailyRetentionPeriod time.Duration

var weeklyRetentionCount int
var weeklyRetentionPeriod time.Duration

var testFileName string
var pathToTestFile string

// Setup testing
func init() {
	log.Init(ioutil.Discard, ioutil.Discard, ioutil.Discard)

	awsCredentials := os.Getenv("AWS_CRED_FILE")
	awsProfile := os.Getenv("AWS_PROFILE")
	awsRegion := os.Getenv("AWS_REGION")
	awsBucket := os.Getenv("AWS_BUCKET_ROTATION")
	s3svc, err := s3client.CreateS3Client(awsCredentials, awsProfile, awsRegion)

	if err != nil {
		log.Error.Println(err)
	}

	svc = s3svc

	bucket = awsBucket

	dailyRetentionCount = 6
	dailyRetentionPeriod = 140

	weeklyRetentionCount = 4
	weeklyRetentionPeriod = 280

	timeout = time.Second * 3600

	testFileName = "testBackupFile"
	pathToTestFile = "../" + testFileName

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

	err = util.CreateFile(pathToTestFile, []byte("this is just a little test file"))
	if err != nil {
		log.Error.Println("failed to create file required for testing: " + err.Error())
		os.Exit(1)
	}

}

//----------------------------------------------
//
//                  Tests
//
//----------------------------------------------

//----------------------------------------------
//
// Positive Testing
//		Basic Rotation Testing
//			Tests:
//				1: 1 daily upload
//				2: 1 weekly upload
//				3: 1 monthly upload
//
//
// These tests are to ensure that when a file is uploaded on a
// particular day of the month it is given the appropriate prefix
//
//----------------------------------------------

// Test 1 -Basic Rotation Testing
// 	Upload one file on a Tuesday
// 	This should result in the file being prefixed with 'daily_'
func TestFirstDailyUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_19 := 19
	testMonth := time.September
	testYear := 2017

	// Tuesday - should result in a daily backup
	uploadDate := time.Date(testYear, testMonth, SEP_19, 01, 0, 0, 0, time.UTC)

	dailyKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) { // the first backup will create a daily, weekly and monthly base
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(dailyKey, bucketContents) {
		t.Error(fmt.Sprintf("expected key: '%s' to exist in bucket", dailyKey))
	}

	if !util.CheckPrefix(dailyKey, policy.DailyPrefix) {
		t.Error(fmt.Sprintf("expected key '%s' to be prefixed with: '%s'", dailyKey, policy.DailyPrefix))
	}

}

// Test 2 - Basic Rotation Testing
// 	Upload one file on a Monday
// 	This should result in the file being prefixed with 'weekly_'
func TestFirstWeeklyUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_18 := 18
	testMonth := time.September
	testYear := 2017

	//Monday - should be a weekly backup
	uploadDate := time.Date(testYear, testMonth, SEP_18, 01, 0, 0, 0, time.UTC)

	weeklyKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket size to be 1")
	}

	if !util.FindKeyInBucket(weeklyKey, bucketContents) {
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", weeklyKey))
	}

	if !util.CheckPrefix(weeklyKey, policy.WeeklyPrefix) {
		t.Error(fmt.Sprintf("expected key: '%s' to be prefixed with: '%s'", weeklyKey, policy.WeeklyPrefix))
	}
}

// Test 3 - Basic Rotation Testing
// 	Upload one file on the first of the month
// 	This should result in it being prefixed with 'monthly_'
func TestFirstMonthlyUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_01 := 1
	testMonth := time.September
	testYear := 2017

	//First of the month - should be a monthly backup
	uploadDate := time.Date(testYear, testMonth, SEP_01, 01, 0, 0, 0, time.UTC)

	monthlyKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 1) {
		t.Error("expected bucket contents to be 1")
	}

	if !util.FindKeyInBucket(monthlyKey, bucketContents) {
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", monthlyKey))
	}

	if !util.CheckPrefix(monthlyKey, policy.MonthlyPrefix) {
		t.Error(fmt.Sprintf("expected key: '%s' to be prefixed with: '%s'", monthlyKey, policy.MonthlyPrefix))
	}
}

//----------------------------------------------
//
// Positive Testing
//		Full Rotation Testing
//			No Enforced Retention Period
//				Tests:
//					1: One week rotation
//					2: 6 week gradual rotation
//					3: 30 day rotation
//					4: 90 day rotation
//
//
// These tests are designed to simulate the GFS
// rotation completely without enforcing the retention period
//
//----------------------------------------------

// Test 1 - Full Rotation Testing - No Enforced Retention Period
// 	Test a full week of backups from Monday to Sunday
func TestFullWeekUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_04 := 4
	currentDay := SEP_04
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 4 2017
	weeklyKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

	dailyKeys := []string{}

	SEP_10 := 9

	for currentDay <= SEP_10 {
		uploadDate = uploadDate.Add(time.Hour * 24)
		currentKey, _ := runMockBackup(t, uploadDate, 0, policy, false)
		dailyKeys = append(dailyKeys, currentKey)
		currentDay++
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	log.Info.Println(len(bucketContents.Contents))

	if !util.CheckBucketSize(bucketContents, 7) {
		t.Error("expected bucket contents to be 7 but got: " + string(len(bucketContents.Contents)))
	}

	for _, dailyKey := range dailyKeys {
		if !util.FindKeyInBucket(dailyKey, bucketContents) {
			t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", dailyKey))
		}
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if !util.FindKeyInBucket(weeklyKey, bucketContents) {
		t.Error(fmt.Sprintf("expected key '%s' to exist in bucket", weeklyKey))
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 1 {
		t.Error("expected to find 1 weekly keys in bucket")
	}

}

// Test 2 - Full Rotation Testing - No Enforced Retention Period
// 	The weekly backup will be rotated as soon as a 5th weekly backup exists
// 	This can be delayed if the weekly backup falls on the first of the month
// 	Where a monthly backup will be taken instead
func TestGradualWeeklyRotation(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_01 := 1
	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, 0, policy, false)

	weeklyBackupKeys := make(map[int]string)

	SEP_02 := 2
	SEP_04 := 4
	SEP_11 := 11
	SEP_18 := 18
	SEP_25 := 25

	currentDay = SEP_02
	for currentDay <= SEP_25 { // Ends at September 25 2017
		// Once this loop has completed, 4 weekly backups should exist
		uploadDate = uploadDate.Add(time.Hour * 24)
		// These days represent each Monday in September
		if currentDay == SEP_04 || currentDay == SEP_11 || currentDay == SEP_18 || currentDay == SEP_25 {
			currentKey, _ := runMockBackup(t, uploadDate, 0, policy, false)
			weeklyBackupKeys[currentDay] = currentKey
		} else {
			runMockBackup(t, uploadDate, 0, policy, false)
		}
		currentDay++
	}

	// Verify that the 4 weekly backups have been taken and exist in the bucket
	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}
	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != SEP_04 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for _, key := range weeklyBackupKeys {
		if !util.FindKeyInBucket(key, bucketContents) {
			t.Error("expected to find key in bucket: " + key)
		}
	}

	OCTOBER_02 := 32

	// This will trigger oldest weekly backup to be removed from the bucket
	for currentDay <= OCTOBER_02 { // Ends at October 2 2017
		uploadDate = uploadDate.Add(time.Hour * 24)
		// These days represent each Monday in September
		if currentDay == OCTOBER_02 {
			currentKey, _ := runMockBackup(t, uploadDate, 0, policy, false)
			weeklyBackupKeys[currentDay] = currentKey
		} else {
			runMockBackup(t, uploadDate, 0, policy, false)
		}
		currentDay++
	}

	bucketContents, err = s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}
	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for day, key := range weeklyBackupKeys {
		if day == SEP_04 {
			if util.FindKeyInBucket(key, bucketContents) { // Should not find first rotated weekly key
				t.Error("found unexpected key in bucket: " + key)
			}
		} else {
			if !util.FindKeyInBucket(key, bucketContents) {
				t.Error("expected to find key in bucket: " + key)
			}
		}
	}

	OCTOBER_09 := 39

	for currentDay <= OCTOBER_09 { // Ends at October 9 2017
		uploadDate = uploadDate.Add(time.Hour * 24)
		// These days represent each Monday in September
		if currentDay == OCTOBER_09 {
			currentKey, _ := runMockBackup(t, uploadDate, 0, policy, false)
			weeklyBackupKeys[currentDay] = currentKey
		} else {
			runMockBackup(t, uploadDate, 0, policy, false)
		}
		currentDay++
	}

	bucketContents, err = s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}
	// Ensure that there are only 4 weekly keys after 6 weeks
	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error(fmt.Sprintf("expected to find %d '%s' keys in bucket", 4, policy.WeeklyPrefix))
	}

	for day, key := range weeklyBackupKeys {
		// Ensure that the first two weekly keys no longer exist as they should have been rotated
		if day == SEP_04 || day == SEP_11 { // Should not find first or second rotated weekly key
			if util.FindKeyInBucket(key, bucketContents) {
				t.Error("found unexpected key in bucket: " + key)
			}
		} else {
			if !util.FindKeyInBucket(key, bucketContents) {
				t.Error("expected to find key in bucket: " + key)
			}
		}
	}

}

// Test 3 - Full Rotation Testing - No Enforced Retention Period
// 	Test a full 30 days of backups starting 2017 September 01
func TestFullThirtyDaysUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_01 := 1
	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, SEP_01, currentDay, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, 0, policy, false)

	SEP_02 := 2

	// Expected daily backups
	SEP_24 := 24
	SEP_26 := 26
	SEP_27 := 27
	SEP_28 := 28
	SEP_29 := 29
	SEP_30 := 30

	// Expected weekly backups
	SEP_04 := 04
	SEP_11 := 11
	SEP_18 := 18
	SEP_25 := 25

	expectedDailyBackupDays := []int{SEP_24, SEP_26, SEP_27, SEP_28, SEP_29, SEP_30}
	expectedWeeklyBackupDays := []int{SEP_04, SEP_11, SEP_18, SEP_25}

	mostRecentDailyBackups := []string{}
	mostRecentWeeklyBackups := []string{}

	for currentDay = SEP_02; currentDay <= SEP_30; currentDay++ { // September 30
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

		for _, expectedDay := range expectedDailyBackupDays {
			if expectedDay == currentDay {
				mostRecentDailyBackups = append(mostRecentDailyBackups, backupKey)
			}
		}

		for _, expectedDay := range expectedWeeklyBackupDays {
			if expectedDay == currentDay {
				mostRecentWeeklyBackups = append(mostRecentWeeklyBackups, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 11) {
		t.Error("expected bucket size to be 11") // 6 daily, 4 weekly, 1 monthly
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error("expected to find 3 weekly keys in bucket")
	}

	for _, dailyBackup := range mostRecentDailyBackups {
		if !util.FindKeyInBucket(dailyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + dailyBackup)
		}
	}

	for _, weeklyBackup := range mostRecentWeeklyBackups {
		if !util.FindKeyInBucket(weeklyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + weeklyBackup)
		}
	}

}

// Test 4 - Full Rotation Testing - No Enforced Retention Period
// 	Test a full 90 days of backups starting 2017 September 01
func TestFullNinetyDaysUpload(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	SEP_01 := 01
	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC)
	runMockBackup(t, uploadDate, 0, policy, false)

	// Most recently daily backups at end of rotation
	NOV_24 := 85
	NOV_25 := 86
	NOV_26 := 87
	NOV_28 := 89
	NOV_29 := 90

	// Most recent weekly backups at end of rotation
	NOV_06 := 67
	NOV_13 := 74
	NOV_20 := 81
	NOV_27 := 88

	NOV_30 := 91 // Also should be a daily

	SEP_02 := 2
	currentDay = SEP_02

	expectedDailyBackupDays := []int{NOV_24, NOV_25, NOV_26, NOV_28, NOV_29, NOV_30}
	expectedWeeklyBackupDays := []int{NOV_06, NOV_13, NOV_20, NOV_27}

	mostRecentDailyBackups := []string{}
	mostRecentWeeklyBackups := []string{}

	for currentDay = SEP_02; currentDay <= NOV_30; currentDay++ {
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

		for _, expectedDay := range expectedDailyBackupDays {
			if expectedDay == currentDay {
				mostRecentDailyBackups = append(mostRecentDailyBackups, backupKey)
			}
		}

		for _, expectedDay := range expectedWeeklyBackupDays {
			if expectedDay == currentDay {
				mostRecentWeeklyBackups = append(mostRecentWeeklyBackups, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 13) { // 6 daily, 4 weekly, 3 monthly
		t.Error("expected bucket size to be 13")
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error("expected to find 3 weekly keys in bucket")
	}

	for _, dailyBackup := range mostRecentDailyBackups {
		if !util.FindKeyInBucket(dailyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + dailyBackup)
		}
	}

	for _, weeklyBackup := range mostRecentWeeklyBackups {
		if !util.FindKeyInBucket(weeklyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + weeklyBackup)
		}
	}

}

//----------------------------------------------
//
// Positive Testing
//		Partial Rotation Testing
//			With Enforced Retention Period
//				Tests:
//					1: Three Weeks
//					2: Two Months
//
// These tests are to ensure that the behaviour
// for GFS is the same even when the enforced
// retention period is applied.
//
// Each mock backup is sufficiently spaced apart in time
// to ensure that the retention period policy is not triggered
//
//----------------------------------------------

func TestThreeWeeksEnforcedRetentionPeriodPositive(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	enforcedRetentionPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod:   time.Second * 20,
		DailyRetentionCount:    dailyRetentionCount,
		DailyPrefix:            "daily_",
		WeeklyRetentionPeriod:  time.Second * 60,
		WeeklyRetentionCount:   weeklyRetentionCount,
		WeeklyPrefix:           "weekly_",
		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: true,
	}

	delayBetweenUploads := 5

	SEP_04 := 04 // Monday
	currentDay := SEP_04
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

	// Next day in iteration
	SEP_05 := 5

	// Most recent daily backups at end of rotation
	SEP_12 := 12
	SEP_13 := 13
	SEP_14 := 14
	SEP_15 := 15
	SEP_16 := 16
	SEP_17 := 17
	SEP_18 := 18

	// Most recent weekly backups that are not covered by most recent daily backups
	SEP_11 := 11

	expectedDailyBackupDays := []int{SEP_12, SEP_13, SEP_14, SEP_15, SEP_16, SEP_17}
	expectedWeeklyBackupDays := []int{SEP_04, SEP_11, SEP_18}

	mostRecentDailyBackups := []string{}
	mostRecentWeeklyBackups := []string{}

	for currentDay = SEP_05; currentDay <= SEP_18; currentDay++ { // September 30
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

		for _, expectedDay := range expectedDailyBackupDays {
			if expectedDay == currentDay {
				mostRecentDailyBackups = append(mostRecentDailyBackups, backupKey)
			}
		}

		for _, expectedDay := range expectedWeeklyBackupDays {
			if expectedDay == currentDay {
				mostRecentWeeklyBackups = append(mostRecentWeeklyBackups, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 9) {
		t.Error("expected bucket size to be 9") // 6 daily, 3 weekly
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 3 {
		t.Error("expected to find 3 weekly keys in bucket")
	}

	for _, dailyBackup := range mostRecentDailyBackups {
		if !util.FindKeyInBucket(dailyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + dailyBackup)
		}
	}

	for _, weeklyBackup := range mostRecentWeeklyBackups {
		if !util.FindKeyInBucket(weeklyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + weeklyBackup)
		}
	}
}

func TestTwoMonthsEnforcedRetentionPeriodPositive(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	enforcedRetentionPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod:   time.Second * 18,
		DailyRetentionCount:    dailyRetentionCount,
		DailyPrefix:            "daily_",
		WeeklyRetentionPeriod:  time.Second * 60,
		WeeklyRetentionCount:   weeklyRetentionCount,
		WeeklyPrefix:           "weekly_",
		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: true,
	}

	delayBetweenUploads := 3

	SEP_01 := 01
	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

	// Next day in iteration
	SEP_02 := 2

	// Most recently daily backups at end of rotation
	OCT_24 := 54
	OCT_25 := 55
	OCT_26 := 56
	OCT_27 := 57
	OCT_28 := 58
	OCT_29 := 59

	// Most recent weekly backups at end of rotation
	OCT_09 := 39
	OCT_16 := 46
	OCT_23 := 53
	OCT_30 := 60

	expectedDailyBackupDays := []int{OCT_24, OCT_25, OCT_26, OCT_27, OCT_28, OCT_29}
	expectedWeeklyBackupDays := []int{OCT_09, OCT_16, OCT_23, OCT_30}

	mostRecentDailyBackups := []string{}
	mostRecentWeeklyBackups := []string{}

	for currentDay = SEP_02; currentDay <= OCT_30; currentDay++ { // September 30
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

		for _, expectedDay := range expectedDailyBackupDays {
			if expectedDay == currentDay {
				mostRecentDailyBackups = append(mostRecentDailyBackups, backupKey)
			}
		}

		for _, expectedDay := range expectedWeeklyBackupDays {
			if expectedDay == currentDay {
				mostRecentWeeklyBackups = append(mostRecentWeeklyBackups, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 12) {
		t.Error("expected bucket size to be 12") // 6 daily, 4 weekly, 2 monthly
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error("expected to find 4 weekly keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.MonthlyPrefix, bucketContents)) != 2 {
		t.Error("expected to find 2 monthly keys in bucket")
	}

	for _, dailyBackup := range mostRecentDailyBackups {
		if !util.FindKeyInBucket(dailyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + dailyBackup)
		}
	}

	for _, weeklyBackup := range mostRecentWeeklyBackups {
		if !util.FindKeyInBucket(weeklyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + weeklyBackup)
		}
	}
}

//----------------------------------------------
// Negative Testing
//		Partial Rotation Testing
//			With Enforced Retention Period
//				Tests:
//					1: Three Weeks
//					2: Two Months
//
// This test is to ensure that objects are not rotated even
// when another object is uploaded that will trigger rotation
// prior to the retention period
//
//----------------------------------------------

func TestDailyRotationEnforcedRetentionPeriodNegative(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	enforcedRetentionPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod:   time.Second * 120, // Increase retention period
		DailyRetentionCount:    dailyRetentionCount,
		DailyPrefix:            "daily_",
		WeeklyRetentionPeriod:  time.Second * 60,
		WeeklyRetentionCount:   weeklyRetentionCount,
		WeeklyPrefix:           "weekly_",
		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: true,
	}

	delayBetweenUploads := 3

	SEP_01 := 1
	SEP_02 := 2
	SEP_03 := 3
	SEP_04 := 4
	SEP_05 := 5
	SEP_06 := 6
	SEP_07 := 7
	SEP_08 := 8

	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

	expectedBackupDays := []int{SEP_01, SEP_02, SEP_03, SEP_04, SEP_05, SEP_06, SEP_07, SEP_08}
	expectedBackupKeys := []string{}

	for currentDay = SEP_02; currentDay <= SEP_08; currentDay++ { // Next upload will cause a rotation
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)
		for _, expectedDay := range expectedBackupDays {
			if expectedDay == currentDay {
				expectedBackupKeys = append(expectedBackupKeys, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 8) { // 1 monthly, 1 weekly, 6 daily
		t.Error("expected bucket size to be 8")
	}

	// Run mock that causes rotation
	uploadDate = uploadDate.Add(time.Hour * 24)
	backupKey, deletedKeys := runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)
	if len(deletedKeys) > 0 {
		t.Error("expected no keys to be deleted")
	}
	expectedBackupKeys = append(expectedBackupKeys, backupKey)

	bucketContents, err = s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	// Additional daily key should not be rotated due to old key be within retention period
	if !util.CheckBucketSize(bucketContents, 9) {
		t.Error("expected bucket size to be 9")
	}

	for _, backupKey := range expectedBackupKeys {
		if !util.FindKeyInBucket(backupKey, bucketContents) {
			t.Error("expected to find key in bucket: " + backupKey)
		}
	}

}

func TestTwoMonthsEnforcedRetentionPeriodNegative(t *testing.T) {

}

//----------------------------------------------
// Positive Testing
//		Partial Rotation Testing
//			Dry Run
//
// An initial set of objects is created which is eligible for rotation.
// No objects should be created or deleted
//----------------------------------------------

func TestDryRunRotation(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	enforcedRetentionPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod:   time.Second * 120, // Increase retention period
		DailyRetentionCount:    dailyRetentionCount,
		DailyPrefix:            "daily_",
		WeeklyRetentionPeriod:  time.Second * 60,
		WeeklyRetentionCount:   weeklyRetentionCount,
		WeeklyPrefix:           "weekly_",
		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: false,
	}

	delayBetweenUploads := 1

	SEP_01 := 1
	SEP_02 := 2
	SEP_03 := 3
	SEP_04 := 4
	SEP_05 := 5
	SEP_06 := 6
	SEP_07 := 7
	SEP_08 := 8

	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC) // Monday September 1 2017
	runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)

	expectedBackupDays := []int{SEP_01, SEP_02, SEP_03, SEP_04, SEP_05, SEP_06, SEP_07, SEP_08}
	expectedBackupKeys := []string{}

	for currentDay = SEP_02; currentDay <= SEP_08; currentDay++ { // Next upload will cause a rotation
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, false)
		for _, expectedDay := range expectedBackupDays {
			if expectedDay == currentDay {
				expectedBackupKeys = append(expectedBackupKeys, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 8) { // 1 monthly, 1 weekly, 6 daily
		t.Error("expected bucket size to be 8")
	}

	// Enable dry run
	uploadDate = uploadDate.Add(time.Hour * 24)
	runMockBackup(t, uploadDate, delayBetweenUploads, enforcedRetentionPolicy, true)

	bucketContents, err = s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	// Bucket should not have changed in size nor the contents changed since dry run was enabled for last backup
	if !util.CheckBucketSize(bucketContents, 8) {
		t.Error("expected bucket size to be 8")
	}

	for _, backupKey := range expectedBackupKeys {
		if !util.FindKeyInBucket(backupKey, bucketContents) {
			t.Error("expected to find key in bucket: " + backupKey)
		}
	}
}

//----------------------------------------------
// Positive Testing
//		Partial Rotation Testing
//			Test rotation with other objects
//
// It's likely that there will be other objects in the bucket.
// This test is to ensure that no other objects are affected during the rotation.
// To test this an initial set of objects will be created in the bucket and then a full month rotation will run
//----------------------------------------------

func TestRotationWithOtherObjects(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	randomKeys := []string{}

	// Upload test file as some random names
	i := 0
	for i < 5 {
		// Generate 5 random keys in a random bucket
		randomKey, err := justUploadIt("herewego"+strconv.Itoa(i), "test123"+"/")
		if err != nil {
			t.Error("failed to upload random key")
		}
		randomKeys = append(randomKeys, randomKey)
		i++
	}

	for i < 10 {
		// Generate 5 random keys in a random bucket
		randomKey, err := justUploadIt("testinganother1"+strconv.Itoa(i), "test123"+strconv.Itoa(i)+"/")
		if err != nil {
			t.Error("failed to upload random key")
		}
		randomKeys = append(randomKeys, randomKey)
		i++
	}

	for i < 15 {
		// Generate 5 random keys in the main bucket
		randomKey, err := justUploadIt("gowehere"+strconv.Itoa(i), "")
		if err != nil {
			t.Error("failed to upload random key")
		}
		randomKeys = append(randomKeys, randomKey)
		i++
	}

	// Run through full 90 day rotation

	SEP_01 := 01
	currentDay := SEP_01
	testMonth := time.September
	testYear := 2017

	uploadDate := time.Date(testYear, testMonth, currentDay, 01, 0, 0, 0, time.UTC)
	runMockBackup(t, uploadDate, 0, policy, false)

	// Most recently daily backups at end of rotation
	NOV_24 := 85
	NOV_25 := 86
	NOV_26 := 87
	NOV_28 := 89
	NOV_29 := 90

	// Most recent weekly backups at end of rotation
	NOV_06 := 67
	NOV_13 := 74
	NOV_20 := 81
	NOV_27 := 88

	NOV_30 := 91 // Also should be a daily

	SEP_02 := 2
	currentDay = SEP_02

	expectedDailyBackupDays := []int{NOV_24, NOV_25, NOV_26, NOV_28, NOV_29, NOV_30}
	expectedWeeklyBackupDays := []int{NOV_06, NOV_13, NOV_20, NOV_27}

	mostRecentDailyBackups := []string{}
	mostRecentWeeklyBackups := []string{}

	for currentDay = SEP_02; currentDay <= NOV_30; currentDay++ {
		uploadDate = uploadDate.Add(time.Hour * 24)
		backupKey, _ := runMockBackup(t, uploadDate, 0, policy, false)

		for _, expectedDay := range expectedDailyBackupDays {
			if expectedDay == currentDay {
				mostRecentDailyBackups = append(mostRecentDailyBackups, backupKey)
			}
		}

		for _, expectedDay := range expectedWeeklyBackupDays {
			if expectedDay == currentDay {
				mostRecentWeeklyBackups = append(mostRecentWeeklyBackups, backupKey)
			}
		}
	}

	bucketContents, err := s3client.GetBucketContents(svc, bucket)
	if err != nil {
		t.Error("failed to retrieve bucket contents")
	}

	if !util.CheckBucketSize(bucketContents, 28) { // 6 daily, 4 weekly, 3 monthly, 15 randomly generated keys
		t.Error("expected bucket size to be 13")
	}

	if len(util.FindKeysInBucketByPrefix(policy.DailyPrefix, bucketContents)) != 6 {
		t.Error("expected to find 6 daily keys in bucket")
	}

	if len(util.FindKeysInBucketByPrefix(policy.WeeklyPrefix, bucketContents)) != 4 {
		t.Error("expected to find 3 weekly keys in bucket")
	}

	for _, dailyBackup := range mostRecentDailyBackups {
		if !util.FindKeyInBucket(dailyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + dailyBackup)
		}
	}

	for _, weeklyBackup := range mostRecentWeeklyBackups {
		if !util.FindKeyInBucket(weeklyBackup, bucketContents) {
			t.Error("expected to find key in bucket: " + weeklyBackup)
		}
	}

	for _, randomKey := range randomKeys {
		if !util.FindKeyInBucket(randomKey, bucketContents) {
			t.Error("expected to find key in bucket: " + randomKey)
		}
	}
}

//----------------------------------------------
//
//      Helper functions for testing below
//
//----------------------------------------------

// Uploads the 'test_backup_file' file in the repository and initiates a GFS rotation
// If delay > 0 then the thread will sleep for the delay specified (seconds) before rotating the files
func runMockBackup(t *testing.T, uploadDate time.Time, delay int, providedPolicy rpolicy.RotationPolicy, dryRun bool) (string, []string) {

	testUploadObject := upload.UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: testFileName + "_" + uploadDate.Format("02-Jan-06"),
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
		PartSize:   50,
		Manipulate: true,
	}

	prefix := util.GetKeyType(providedPolicy, uploadDate)
	s3FileName, err := upload.UploadFile(svc, testUploadObject, prefix, dryRun)
	if err != nil {
		t.Fatal(fmt.Sprintf("failed to upload file: %v", err))
	}

	// If no minimum delay is specified then it's more likely a file will be uploaded
	// out of order and break the test
	if delay == 0 {
		delay = 1
	}

	time.Sleep(time.Second * time.Duration(delay))

	return s3FileName, StartRotation(svc, bucket, providedPolicy, dryRun)
}

func justUploadIt(s3FileName string, s3BucketDir string) (string, error) {
	testUploadObject := upload.UploadObject{
		PathToFile: pathToTestFile,
		S3FileName: s3FileName,
		BucketDir:  s3BucketDir,
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
		PartSize:   50,
		Manipulate: false,
	}

	prefix := util.GetKeyType(policy, time.Now())
	backupKey, err := upload.UploadFile(svc, testUploadObject, prefix, false)
	if err != nil {
		return "", err
	}
	return backupKey, nil
}
