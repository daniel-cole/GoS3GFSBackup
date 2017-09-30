package upload

import "time"

type UploadObject struct {
	PathToFile string
	S3FileName string
	Bucket     string
	BucketDir  string
	Timeout    time.Duration
	NumWorkers int
}
