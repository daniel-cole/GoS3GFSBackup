package download

// DownloadObject represents an object to download from S3
type DownloadObject struct {
	DownloadTarget   string
	FullPathToS3File string
	Bucket           string
	BucketDir        string
	NumWorkers       int
	PartSize         int
}
