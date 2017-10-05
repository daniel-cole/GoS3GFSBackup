package download

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"os"
)

// DownloadFile downloads a file from s3 given a bucket and key
func DownloadFile(svc *s3.S3, downloadObject DownloadObject) error {

	log.Info.Println(`
	######################################
	#       File Download Started        #
	######################################
	`)

	partSize := int64(downloadObject.PartSize * 1024 * 1024)

	downloader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
		d.PartSize = partSize
		d.Concurrency = downloadObject.NumWorkers
	})

	file, err := os.Create(downloadObject.DownloadLocation)
	if err != nil {
		return err
	}

	log.Info.Println("Attempting to download file from S3: " + downloadObject.S3FileKey)

	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(downloadObject.Bucket),
		Key:    aws.String(downloadObject.S3FileKey),
	})

	if err != nil {
		log.Error.Printf("Failed to download '%s' from S3: %v\n", downloadObject.S3FileKey, err)
		return err
	}

	return nil

}
