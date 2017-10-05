package main

import (
	"github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rotate"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/upload"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"os"
	"strconv"
	"time"
)

type args struct {
	Region                 string `arg:"required,help:The AWS region to upload the specified file to"`
	Bucket                 string `arg:"required,help:The S3 bucket to upload the specified file to"`
	CredFile               string `arg:"help:The full path to the AWS CLI credential file if environment variables are not being used to provide the access id and key"`
	Profile                string `arg:"help:The profile to use for the AWS CLI credential file"`
	Action                 string `arg:"help:The intended action for the tool to run [backup|upload|download|rotate]"`
	PathToFile             string `arg:"help:The full path to the file to upload to the specified S3 bucket. Must be specified unless --rotateonly=true"`
	S3FileName             string `arg:"help:The name of the file as it should appear in the S3 bucket. Must be specified unless --rotateonly=true"`
	BucketDir              string `arg:"help:The directory in the bucket in which to upload the S3 object to. Must include the trailing slash"`
	Timeout                int    `arg:"help:The timeout to upload the specified file (seconds)"`
	DryRun                 bool   `arg:"help:If enabled then no upload or rotation actions will be executed [default: false]"`
	ConcurrentWorkers      int    `arg:"help:The number of threads to use when uploading the file to S3"`
	PartSize               int    `arg:"help:The part size to use when performing a multipart upload or download (MB)"`
	EnforceRetentionPeriod bool   `arg:"help:If enabled then objects in the S3 bucket will only be rotated if they are older then the retention period"`
	DailyRetentionCount    int    `arg:"help:The number of daily objects to keep in S3"`
	DailyRetentionPeriod   int    `arg:"help:The retention period (hours) that a daily object should be kept in S3"`
	WeeklyRetentionCount   int    `arg:"help:The number of weekly objects to keep in S3"`
	WeeklyRetentionPeriod  int    `arg:"help:The retention period (hours) that a weekly object should be kept in S3"`
}

func init() {
	log.Init(os.Stdout, os.Stdout, os.Stderr)
}

func main() {
	// Set default args
	args := args{}
	args.Timeout = 3600 // Default timeout to 1 hour for file upload
	args.CredFile = ""
	args.Profile = "default"
	args.BucketDir = ""
	args.EnforceRetentionPeriod = true
	args.DryRun = false
	args.ConcurrentWorkers = 5
	args.PartSize = 50
	args.DailyRetentionCount = 6
	args.DailyRetentionPeriod = 168
	args.WeeklyRetentionCount = 4
	args.WeeklyRetentionPeriod = 672

	// Parse args from command line
	arg.MustParse(&args)

	logArgs(args)

	log.Info.Println(`
	######################################
	#        GoS3GFSBackup Started       #
	######################################
	`)

	svc, err := s3client.CreateS3Client(args.CredFile, args.Profile, args.Region)
	if err != nil {
		log.Error.Println(err)
		os.Exit(1)
	}

	runAction(svc, args)

	log.Info.Println("Finished GoS3GFSBackup!")

	log.Info.Println(`
	######################################
	#      GoS3GFSBackup Finished        #
	######################################
	`)

}

func runAction(svc *s3.S3, args args) {
	switch args.Action {
	case "backup":
		runBackupAction(svc, args)
	case "upload":
		runUploadAction(svc, args)
	case "download":
		runDownloadAction(svc, args)
	case "rotate":
		runRotateAction(svc, args)
	default:
		log.Error.Println("unexpected action specified: " + args.Action)
	}
}

func runBackupAction(svc *s3.S3, arguments args) {

	rotationPolicy := getRotationPolicy(arguments)

	log.Info.Println("Starting standard GFS upload and rotation")
	prefix := util.GetKeyType(rotationPolicy, time.Now())
	_, err := upload.UploadFile(svc, getUploadObject(arguments, true), prefix, arguments.DryRun)
	if err != nil {
		log.Error.Printf("Failed to upload file. Skipping Rotation. Reason: %v\n", err)
		os.Exit(1)
	}

	rotate.StartRotation(svc, arguments.Bucket, rotationPolicy, arguments.DryRun)
	log.Info.Println("Upload and Rotation Complete!")

}

func runUploadAction(svc *s3.S3, arguments args) {
	log.Info.Println("Upload action specified, uploading file")

	_, err := upload.UploadFile(svc, getUploadObject(arguments, false), "", arguments.DryRun)
	if err != nil {
		log.Error.Printf("Failed to upload file. Reason: %v\n", err)
		os.Exit(1)
	}
}

func runRotateAction(svc *s3.S3, arguments args) {
	log.Info.Println("Rotate action specified, proceeding with rotation only")
	rotate.StartRotation(svc, arguments.Bucket, getRotationPolicy(arguments), arguments.DryRun)
}

func runDownloadAction(svc *s3.S3, args args) {

}

func getUploadObject(arguments args, manipulate bool) upload.UploadObject {
	return upload.UploadObject{
		PathToFile: arguments.PathToFile,
		S3FileName: arguments.S3FileName,
		BucketDir:  arguments.BucketDir,
		Bucket:     arguments.Bucket,
		Timeout:    time.Second * time.Duration(arguments.Timeout),
		NumWorkers: arguments.ConcurrentWorkers,
		PartSize:   arguments.PartSize,
		Manipulate: manipulate,
	}
}

func getRotationPolicy(arguments args) rpolicy.RotationPolicy {
	if !arguments.EnforceRetentionPeriod {
		log.Warn.Println("GoS3GFSBackup is running with enforce retention period disabled. " +
			"This may result in objects being deleted that which have not exceeded the retention period")
	}

	//  Standard GFS rotation policy
	return rpolicy.RotationPolicy{
		DailyRetentionPeriod: time.Hour * time.Duration(arguments.DailyRetentionPeriod),
		DailyRetentionCount:  arguments.DailyRetentionCount,
		DailyPrefix:          "daily_",

		WeeklyRetentionPeriod: time.Hour * time.Duration(arguments.WeeklyRetentionPeriod),
		WeeklyRetentionCount:  arguments.WeeklyRetentionCount,
		WeeklyPrefix:          "weekly_",

		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: arguments.EnforceRetentionPeriod,
	}

}

func logArgs(arguments args) {
	log.Info.Println("Starting GoS3GFSBackup with arguments: ")

	log.Info.Println("--credfile=" + arguments.CredFile)
	log.Info.Println("--region=" + arguments.Region)
	log.Info.Println("--bucket=" + arguments.Bucket)
	log.Info.Println("--bucketdir=" + arguments.BucketDir)
	log.Info.Println("--profile=" + arguments.Profile)
	log.Info.Println("--action=" + arguments.Action)
	log.Info.Println("--pathtofile=" + arguments.PathToFile)
	log.Info.Println("--s3filename=" + arguments.S3FileName)
	log.Info.Println("--dryrun=" + strconv.FormatBool(arguments.DryRun))
	log.Info.Println("--timeout=" + strconv.Itoa(arguments.Timeout))
	log.Info.Println("--enforceretentionperiod=" + strconv.FormatBool(arguments.EnforceRetentionPeriod))
	log.Info.Println("--concurrentworkers=" + strconv.Itoa(arguments.ConcurrentWorkers))
	log.Info.Println("--partsize=" + strconv.Itoa(arguments.PartSize))
	log.Info.Println("--dailyretentioncount=" + strconv.Itoa(arguments.DailyRetentionCount))
	log.Info.Println("--dailyretentionperiod=" + strconv.Itoa(arguments.DailyRetentionPeriod))
	log.Info.Println("--weeklyretentioncount=" + strconv.Itoa(arguments.WeeklyRetentionCount))
	log.Info.Println("--weeklyretentionperiod=" + strconv.Itoa(arguments.WeeklyRetentionPeriod))

}
