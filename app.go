package main

import (
	"os"
	"time"
	"fmt"
	"github.com/alexflint/go-arg"
	log "github.com/Sirupsen/logrus"
	"github.com/daniel-cole/GoS3GFSBackup/actions"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
)

type args struct {
	CredFile     string `arg:"required,help:The full path to the AWS CLI credential file"`
	Region       string `arg:"required,help:The AWS region to upload the specified file to"`
	Bucket       string `arg:"required,help:The S3 bucket to upload the specified file to"`
	PathToFile   string `arg:"required,help:The full path to the file to upload to the specified S3 bucket"`
	S3FileName   string `arg:"required,help:The name of the file as it should appear in the S3 bucket"`
	Timeout      int    `arg:"help:The timeout to upload the specified file (seconds)"`
	Profile      string `arg:"help:The profile to use for the AWS CLI credential file. If none specified the default value will be used"`
	JustUploadIt bool   `arg:"help:If this option is specified the file will be uploaded as is without the GFS backup strategy. --justuploadit=true"`
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	//set default args
	args := args{}
	args.Timeout = 3600 // Default to 1 hour for file upload
	args.Profile = "default"

	//parse args from command line
	arg.MustParse(&args)

	fmt.Println(`
	######################################
	#        GoS3GFSBackup Started       #
	######################################
	`)

	log.WithFields(log.Fields{
		"credential file": args.CredFile,
		"AWS region":      args.Region,
		"file":            args.PathToFile,
		"timeout":         args.Timeout,
	}).Info("GoS3GFSBackup inputs")

	//  Standard GFS rotation policy
	rotationPolicy := rpolicy.RotationPolicy{
		DailyRetentionPeriod: time.Hour * 168, // 1 Week
		DailyRetentionCount:  6,               // 6 days to keep due to weekly (Monday) backup being the father
		DailyPrefix:          "daily_",

		WeeklyRetentionPeriod: time.Hour * 672, // 28 Days / 1 Month
		WeeklyRetentionCount:  4,
		WeeklyPrefix:          "weekly_",

		MonthlyPrefix: "monthly_",
	}

	svc := s3client.CreateS3Client(args.CredFile, args.Profile, args.Region)

	timeout := time.Second * time.Duration(args.Timeout)

	if args.JustUploadIt {
		// This should only be used in an emergency
		actions.JustUploadFile(args.PathToFile, svc, args.S3FileName, timeout, args.Bucket)
	} else {
		_, err := actions.UploadFile(args.PathToFile, svc, args.S3FileName, timeout, args.Bucket, rotationPolicy, time.Now())
		if err != nil {
			log.Fatal(fmt.Sprintf("failed to upload file: %v", err))
		}
		actions.RotateFiles(svc, args.Bucket, rotationPolicy)
	}

	log.Info("Finished GoS3GFSBackup!")

	fmt.Println(`
	######################################
	#      GoS3GFSBackup Finished        #
	######################################
	`)

}
