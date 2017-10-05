package download

type DownloadObject struct {
	DownloadTarget   string
	FullPathToS3File string
	Bucket           string
	BucketDir        string
}
