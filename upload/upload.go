package upload

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

// UploadFile returns the name of the file that was uploaded to S3
func UploadFile(svc *s3.S3, uploadObject UploadObject, policy rpolicy.RotationPolicy,
	uploadTime time.Time, justUploadIt bool, dryRun bool) (string, error) {

	if svc == nil {
		return "", errors.New("svc must not be nil")
	}

	err := validationCheck(uploadObject)
	if err != nil {
		return "", err
	}

	log.Info.Println(`
	######################################
	#         File Upload Started        #
	######################################
	`)

	// Context provides a timeout with AWS SDK calls 'WithContext'
	ctx := context.Background()
	var cancelFn func()
	if uploadObject.Timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, uploadObject.Timeout)
	}
	defer cancelFn()

	file, err := os.Open(uploadObject.PathToFile)
	defer file.Close()

	if err != nil {
		log.Error.Printf("Failed to open file '%s', %v\n", uploadObject.PathToFile, err)
		return "", err
	}

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	log.Info.Printf("Uploading '%s' (%d bytes) to s3 bucket '%s'\n", uploadObject.PathToFile, fileSize, uploadObject.Bucket)

	prefix := util.GetKeyType(policy, uploadTime)

	s3FileName := uploadObject.S3FileName

	if !justUploadIt { // Mutate the file name to comply with GFS
		s3FileName = fmt.Sprintf("%s%s%s_%s", uploadObject.BucketDir, prefix, uploadObject.S3FileName, time.Now().Format("20060102T150405"))
	} else {
		s3FileName = uploadObject.BucketDir + s3FileName
	}

	uploadParams := &s3manager.UploadInput{
		Bucket: aws.String(uploadObject.Bucket),
		Key:    aws.String(s3FileName),
		Body:   file,
	}

	partSize := int64(50 * 1024 * 1024) // 50 MiB

	log.Info.Printf("Upload part size is: %d bytes\n", partSize)

	finishedCh := make(chan bool)

	go func() {
		if fileSize < partSize { // Don't bother checking progress if file size is < 50MiB
			<-finishedCh
		} else {
			totalParts := int64(math.Ceil(float64(fileSize) / float64(partSize))) // Round up
			log.Info.Printf("Upload is larger than %d bytes and therefore will be uploaded in %d chunks\n", partSize, totalParts)
			checkUploadProgress(svc, s3FileName, uploadObject.Bucket, partSize, totalParts, finishedCh) // Attempt to track progress of file upload
		}
	}()

	log.Info.Printf("Uploading is about to begin with a maximum of %d workers\n", uploadObject.NumWorkers)

	uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = partSize                   // 50MiB part size. Limit of 10,000 parts. http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html
		u.Concurrency = uploadObject.NumWorkers // The total number of workers to upload the file
		u.LeavePartsOnError = false
	})

	startTime := time.Now()

	if dryRun {
		log.Info.Printf("Skipping upload of key: '%s' as dry run has been enabled\n", s3FileName)
	} else {
		_, err = uploader.UploadWithContext(ctx, uploadParams) // Upload file
	}
	elapsedTime := time.Since(startTime).Seconds()

	log.Info.Printf("Total time spent processing upload: %0.2f seconds\n", elapsedTime)

	finishedCh <- true // Stop checking for upload

	if err != nil {
		if strings.Contains(err.Error(), "context deadline exceeded") {
			log.Error.Printf("failed to upload file due to upload time exceeding specified timeout %v\n", err)
		} else {
			log.Error.Printf("Failed to upload file: %v\n", err)
		}

		return "", err
	}

	return s3FileName, nil

}

// This function attempts to track the progress of an S3 multipart upload
// It will only work if there are no other multipart uploads running at the same time with the same key
// This function provides better feedback when the file size is sufficiently large or the number of workers relative
// To the file size is low. i.e. 1 worker for 200MiB. 5 workers for 5GiB
func checkUploadProgress(svc *s3.S3, s3FileName string, bucket string, partSize int64, totalParts int64, uploadFinishedCh <-chan bool) {
	log.Info.Println("Attempting to display progress of upload. This will give a very rough estimate of progress, " +
		"especially if the upload is being handled by multiple workers")
	for { // Loop will only exit once channel has been updated
		time.Sleep(time.Second * 30) // Sleep first to allow time for multi-part upload to start
		select {
		case <-uploadFinishedCh:
			log.Info.Println("Stopping upload checks as upload has finished processing")
			// Received a value from the channel which means that the upload has finished
			return
		default:
			uploadId, err := s3client.GetMultiPartUploadIDByKey(svc, bucket, s3FileName)
			if err != nil {
				log.Warn.Printf("Failed to retrieve upload id: %v\n", err)
			}

			partsCompleted, err := s3client.GetCountMultiPartsById(svc, bucket, s3FileName, uploadId)
			if err != nil {
				log.Warn.Printf("Failed to retrieve uploaded parts: %v\n", err)
			}
			// Display the current estimated upload progress
			log.Info.Printf("Upload progress: parts uploaded: %d/%d (%d bytes)\n", partsCompleted, totalParts, partsCompleted*partSize)
		}
	}

}

func validationCheck(uploadObject UploadObject) error {
	if uploadObject.BucketDir != "" {
		matched, _ := regexp.MatchString("^.*/$", uploadObject.BucketDir)
		if !matched {
			return errors.New("expected bucket dir to have trailing slash")
		}
	}

	if uploadObject.S3FileName == "" {
		return errors.New("s3FileName should not be empty")
	}

	if uploadObject.NumWorkers < 1 {
		return errors.New("concurrent workers should not be less than 1")
	}

	if uploadObject.PathToFile == "" {
		return errors.New("path to file should not be empty and must include the full path to the file")
	}

	if uploadObject.Bucket == "" {
		return errors.New("invalid bucket specified, bucket must be specified")
	}

	if uploadObject.Timeout < 0 {
		return errors.New("timeout must not be less than 0")
	}

	return nil
}
