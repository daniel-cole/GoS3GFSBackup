package download

import (
	//"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	//"io"
)

func DownloadFile(svc *s3.S3, bucket string, key string) error {

	//partSize := int64(50 * 1024 * 1024) // 50 MiB

	//downloader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
	//	d.PartSize = partSize
	//	d.Concurrency = 5
	//})

	//_, err := downloader.Download(io.WriterAt([]byte("C:\\Users\\danny\\uwat9")), &s3.GetObjectInput{
	//	Bucket: aws.String(bucket),
	//	Key:    aws.String(key),
	//})
	//
	//if err != nil {
	//	return err
	//}
	return nil

}
