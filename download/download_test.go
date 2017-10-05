package download

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/upload"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// Test variables
var svc *s3.S3
var bucket string

var testFileName string
var fullPathToTestFile string

var bigTestFileName string
var fullPathToBigTestFile string

var timeout time.Duration

func init() {
	log.Init(ioutil.Discard, ioutil.Discard, ioutil.Discard)

	awsCredentials := os.Getenv("AWS_CRED_FILE")
	awsProfile := os.Getenv("AWS_PROFILE")
	awsRegion := os.Getenv("AWS_REGION")
	awsBucket := os.Getenv("AWS_BUCKET_DOWNLOAD")
	s3svc, err := s3client.CreateS3Client(awsCredentials, awsProfile, awsRegion)

	if err != nil {
		log.Error.Println(err)
		os.Exit(1)
	}

	svc = s3svc

	timeout = time.Second * 3600

	bucket = awsBucket

	testFileName = "localTestFile"
	fullPathToTestFile = "../" + testFileName

	err = util.CreateFile(fullPathToTestFile, []byte("this is just a little test file"))
	if err != nil {
		log.Error.Println("failed to create file required for testing: " + err.Error())
	}

	bigFileSize := int64(250 * 1024 * 1024) // 250MiB
	bigTestFileName = "localBigTestFile"
	fullPathToBigTestFile = "../" + bigTestFileName

	err = util.CreateBigFile(fullPathToBigTestFile, bigFileSize)
	if err != nil {
		log.Error.Println("failed to create file required for testing: " + err.Error())
		os.Exit(1)
	}

}

func TestDownloadFile(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	createdMD5, err := util.ComputeMD5Sum(fullPathToTestFile)
	if err != nil {
		t.Error("expected to be able to generated md5sum on existing file")
	}

	testUploadObjectNotManipulated := upload.UploadObject{
		PathToFile: fullPathToTestFile,
		S3FileName: testFileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
		PartSize:   50,
		Manipulate: false,
	}

	s3FileName, err := upload.UploadFile(svc, testUploadObjectNotManipulated, "", false)
	if err != nil {
		t.Error(fmt.Sprintf("expected to upload single file without any error: %v", err))
	}

	downloadLocation := "../mySmallTestDownload"

	downloadObject := DownloadObject{
		DownloadLocation: downloadLocation,
		S3FileKey:        s3FileName,
		Bucket:           bucket,
		BucketDir:        "",
		NumWorkers:       5,
		PartSize:         50,
	}

	err = DownloadFile(svc, downloadObject)
	if err != nil {
		t.Error("failed to download s3 file: " + err.Error())
	}

	downloadedMD5, err := util.ComputeMD5Sum(downloadLocation)
	if err != nil {
		t.Error("expected to be able to generated md5sum on downloaded file")
	}

	if string(createdMD5) != string(downloadedMD5) {
		t.Error("expected md5s to match")
	}

}

func TestDownloadFileInDir(t *testing.T) {
	err := util.EmptyBucket(svc, bucket)
	if err != nil {
		t.Error("failed to empty bucket")
	}

	createdMD5, err := util.ComputeMD5Sum(fullPathToBigTestFile)
	if err != nil {
		t.Error("expected to be able to generated md5sum on existing file")
	}

	testUploadObjectNotManipulated := upload.UploadObject{
		PathToFile: fullPathToBigTestFile,
		S3FileName: bigTestFileName,
		BucketDir:  "",
		Bucket:     bucket,
		Timeout:    timeout,
		NumWorkers: 5,
		PartSize:   50,
		Manipulate: false,
	}

	s3FileName, err := upload.UploadFile(svc, testUploadObjectNotManipulated, "", false)
	if err != nil {
		t.Error(fmt.Sprintf("expected to upload single file without any error: %v", err))
	}

	downloadLocation := "../myBigTestDownload"

	downloadObject := DownloadObject{
		DownloadLocation: downloadLocation,
		S3FileKey:        s3FileName,
		Bucket:           bucket,
		BucketDir:        "",
		NumWorkers:       5,
		PartSize:         50,
	}

	err = DownloadFile(svc, downloadObject)
	if err != nil {
		t.Error("failed to download s3 file: " + err.Error())
	}

	downloadedMD5, err := util.ComputeMD5Sum(downloadLocation)
	if err != nil {
		t.Error("expected to be able to generated md5sum on downloaded file")
	}

	if string(createdMD5) != string(downloadedMD5) {
		t.Error("expected md5s to match")
	}
}
