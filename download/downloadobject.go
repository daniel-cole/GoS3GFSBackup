package download

// DownloadObject represents an object to download from S3
type DownloadObject struct {
	DownloadLocation string
	S3FileKey        string
	Bucket           string
	BucketDir        string
	NumWorkers       int
	PartSize         int
}
