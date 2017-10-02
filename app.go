package main

import (
	"os"
	"time"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rotate"
	"github.com/daniel-cole/GoS3GFSBackup/upload"
	"strconv"
)

type args struct {
	Region                 string `arg:"required,help:The AWS region to upload the specified file to"`
	Bucket                 string `arg:"required,help:The S3 bucket to upload the specified file to"`
	CredFile               string `arg:"help:The full path to the AWS CLI credential file if environment variables are not being used to provide the access id and key"`
	Profile                string `arg:"help:The profile to use for the AWS CLI credential file"`
	PathToFile             string `arg:"help:The full path to the file to upload to the specified S3 bucket. Must be specified unless --rotateonly=true"`
	S3FileName             string `arg:"help:The name of the file as it should appear in the S3 bucket. Must be specified unless --rotateonly=true"`
	BucketDir              string `arg:"help:The directory in the bucket in which to upload the S3 object to. Must include the trailing slash"`
	Timeout                int    `arg:"help:The timeout to upload the specified file (seconds)"`
	JustUploadIt           bool   `arg:"help:If this option is specified the file will be uploaded as is without the GFS backup strategy [default: false]"`
	RotateOnly             bool   `arg:"help:If enabled then only GFS rotation will occur with no file upload [default: false]"`
	DryRun                 bool   `arg:"help:If enabled then no upload or rotation actions will be executed [default: false]"`
	ConcurrentWorkers      int    `arg:"help:The number of threads to use when uploading the file to S3"`
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
	args.RotateOnly = false
	args.JustUploadIt = false
	args.DryRun = false
	args.ConcurrentWorkers = 5
	args.DailyRetentionCount = 6
	args.DailyRetentionPeriod = 168
	args.WeeklyRetentionCount = 4
	args.WeeklyRetentionPeriod = 672

	// Parse args from command line
	arg.MustParse(&args)

	logArgs(args)

	fmt.Println(`
	######################################
	#        GoS3GFSBackup Started       #
	######################################
	`)

	//  Standard GFS rotation policy
	rotationPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod: time.Hour * time.Duration(args.DailyRetentionPeriod),
		DailyRetentionCount:  args.DailyRetentionCount,
		DailyPrefix:          "daily_",

		WeeklyRetentionPeriod: time.Hour * time.Duration(args.WeeklyRetentionPeriod),
		WeeklyRetentionCount:  args.WeeklyRetentionCount,
		WeeklyPrefix:          "weekly_",

		MonthlyPrefix:          "monthly_",
		EnforceRetentionPeriod: args.EnforceRetentionPeriod,
	}

	if !args.EnforceRetentionPeriod {
		log.Warn.Println("GoS3GFSBackup is running with enforce retention period disabled. This may result in objects being " +
			"deleted that which have not exceeded the retention period")
	}

	svc, err := s3client.CreateS3Client(args.CredFile, args.Profile, args.Region)
	if err != nil {
		log.Error.Println(err)
		os.Exit(1)
	}

	uploadObject := upload.UploadObject{
		PathToFile: args.PathToFile,
		S3FileName: args.S3FileName,
		BucketDir:  args.BucketDir,
		Bucket:     args.Bucket,
		Timeout:    time.Second * time.Duration(args.Timeout),
		NumWorkers: args.ConcurrentWorkers,
	}

	if args.RotateOnly {
		log.Info.Println("--rotateonly flag set to true, proceeding with rotation only")
		rotate.StartRotation(svc, args.Bucket, rotationPolicy, args.DryRun)

	} else if args.JustUploadIt {
		log.Info.Println("--justuploadit flag set to true, no rotation or file name manipulation will occur during upload")
		_, err := upload.UploadFile(svc, uploadObject, rotationPolicy, time.Now(), args.JustUploadIt, args.DryRun)
		if err != nil {
			log.Error.Printf("Failed to upload file. Reason: %v\n", err)
			os.Exit(1)
		}
	} else {
		log.Info.Println("Starting standard GFS upload and rotation")
		_, err := upload.UploadFile(svc, uploadObject, rotationPolicy, time.Now(), args.JustUploadIt, args.DryRun)
		if err != nil {
			log.Error.Printf("Failed to upload file. Skipping Rotation. Reason: %v\n", err)
			os.Exit(1)
		}
		rotate.StartRotation(svc, args.Bucket, rotationPolicy, args.DryRun)
		log.Info.Println("Upload and Rotation Complete!")
	}

	log.Info.Println("Finished GoS3GFSBackup!")

	fmt.Println(`
	######################################
	#      GoS3GFSBackup Finished        #
	######################################
	`)

}

func logArgs(arguments args) {
	log.Info.Println("Starting GoS3GFSBackup with arguments: ")

	log.Info.Println("--credfile=" + arguments.CredFile)
	log.Info.Println("--region=" + arguments.Region)
	log.Info.Println("--bucket=" + arguments.Bucket)
	log.Info.Println("--bucketdir=" + arguments.BucketDir)
	log.Info.Println("--profile=" + arguments.Profile)
	log.Info.Println("--pathtofile=" + arguments.PathToFile)
	log.Info.Println("--s3filename=" + arguments.S3FileName)
	log.Info.Println("--dryrun=" + strconv.FormatBool(arguments.DryRun))
	log.Info.Println("--timeout= " + strconv.Itoa(arguments.Timeout))
	log.Info.Println("--rotateonly=" + strconv.FormatBool(arguments.RotateOnly))
	log.Info.Println("--enforceretentionperiod=" + strconv.FormatBool(arguments.EnforceRetentionPeriod))
	log.Info.Println("--concurrentworkers=" + strconv.Itoa(arguments.ConcurrentWorkers))
	log.Info.Println("--justuploadit=" + strconv.FormatBool(arguments.JustUploadIt))
	log.Info.Println("--dailyretentioncount=" + strconv.Itoa(arguments.DailyRetentionCount))
	log.Info.Println("--dailyretentionperiod=" + strconv.Itoa(arguments.DailyRetentionPeriod))
	log.Info.Println("--weeklyretentioncount=" + strconv.Itoa(arguments.WeeklyRetentionCount))
	log.Info.Println("--weeklyretentionperiod=" + strconv.Itoa(arguments.WeeklyRetentionPeriod))

}
