package rotate

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/daniel-cole/GoS3GFSBackup/log"
	"github.com/daniel-cole/GoS3GFSBackup/rpolicy"
	"github.com/daniel-cole/GoS3GFSBackup/s3client"
	"github.com/daniel-cole/GoS3GFSBackup/util"
	"time"
)

// StartRotation initiates the GFS rotation with the provided policy
func StartRotation(svc *s3.S3, bucket string, policy rpolicy.RotationPolicy, dryRun bool) []string {
	log.Info.Println(`
	######################################
	#  GoS3GFSBackup Rotation Started!   #
	######################################
	`)

	log.Info.Println("Starting GFS rotation")

	// Keys to be returned at end of both daily and weekly rotation
	deletedKeys := []string{}

	log.Info.Println(`
	######################################
	#   Starting Daily Key Rotation!     #
	######################################
	`)

	// Daily rotation
	for _, key := range keyRotation(svc, bucket, policy.DailyRetentionPeriod, policy.DailyRetentionCount, policy.DailyPrefix, policy.EnforceRetentionPeriod, dryRun) {
		deletedKeys = append(deletedKeys, key)
	}

	log.Info.Println(`
	######################################
	#   Starting Weekly Key Rotation!    #
	######################################
	`)

	// Weekly rotation
	for _, key := range keyRotation(svc, bucket, policy.WeeklyRetentionPeriod, policy.WeeklyRetentionCount, policy.WeeklyPrefix, policy.EnforceRetentionPeriod, dryRun) {
		deletedKeys = append(deletedKeys, key)
	}

	log.Info.Println(`
	######################################
	#         Key Rotation Summary       #
	######################################
	`)

	log.Info.Printf("The total number of keys deleted for this rotation was: %d\n", len(deletedKeys))
	for _, key := range deletedKeys {
		log.Info.Printf("Key deleted in rotation: '%s'\n", key)
	}

	log.Info.Println("Finished GFS rotation")

	return deletedKeys
}

// Any keys with prefix _monthly should have a life cycle policy to move into glacier after 30 days
// If enforceRetentionPeriod is set to true then no keys that are
func keyRotation(svc *s3.S3, bucket string, retentionPeriod time.Duration, retentionCount int, prefix string, enforceRetentionPeriod bool, dryRun bool) []string {
	sortedKeys, err := sortKeysAndLogInfo(svc, bucket, prefix) // Requirement that the keys are sorted before rotating

	log.Info.Println(`
	######################################
	#           Rotating Keys!           #
	######################################
	`)

	if err != nil {
		log.Error.Printf("Failed to retrieve sorted keys: %v\n", err)
		return nil
	}

	if sortedKeys == nil {
		log.Info.Printf("No '%s' key(s) found for rotation\n", prefix)
		return nil
	}

	deletedKeys := []string{}

	numKeys := len(sortedKeys)
	if numKeys > retentionCount {
		log.Info.Printf("Total number of '%s' keys (%d) exceeds retention policy of %d, purging old keys\n",
			prefix, numKeys, retentionCount)

		for _, kv := range sortedKeys[retentionCount:] {
			key := kv.Key

			keyAge := time.Since(kv.ModifiedTime)
			keyAgeHours := keyAge.Hours()
			keyAgeMinutes := keyAge.Minutes()

			log.Info.Printf("Candidate key for deletion: '%s' is %0.1f hours / %0.1f minutes old\n", key, keyAgeHours, keyAgeMinutes)

			// Safety check to ensure that candidate keys for deletion are not within the retentionPeriod
			// This will prevent any key from being deleted if the retention period is enforced
			// If the retention period is not enforced then the key will be removed and a warning logged
			if keyAge <= retentionPeriod {
				if enforceRetentionPeriod {
					log.Error.Printf("Key: '%s' is in violation of retention policy count. However, enforce "+
						"retention period is enabled and the total time elapsed since the key was last modified is "+
						"%0.1f hours / %0.1f minutes. This is less than the retention period of %0.1f hours / %0.1f minutes. "+
						"This key is not eligible for deletion until the retention period has elapsed\n", key, keyAgeHours,
						keyAgeMinutes, retentionPeriod.Hours(), retentionPeriod.Minutes())
					continue // Skip to next candidate key for deletion
				}

				log.Warn.Printf("Key: '%s' is in violation of retention policy count. However, enforce "+
					"retention period is NOT enabled. The total time elapsed since the key was last modified is "+
					"%0.1f hours / %0.1f minutes. This is less than the retention period of %0.1f hours / %0.1f minutes. "+
					"This key WILL be deleted since enforce retention period is NOT enabled\n", key, keyAgeHours,
					keyAgeMinutes, retentionPeriod.Hours(), retentionPeriod.Minutes())
			}
			if dryRun { // Do not delete any keys if dry run has been specified
				log.Info.Printf("Skipping deletion of key: '%s' as dry run has been enabled\n", key)
				deletedKeys = append(deletedKeys, key)
			} else {
				deletedKey, err := s3client.DeleteKey(svc, bucket, key)
				if err != nil {
					log.Error.Printf("Failed to delete key from bucket: '%s': %v\n", key, err)
				} else {
					log.Info.Printf("Successfully deleted key from bucket: '%s'\n", key)
					deletedKeys = append(deletedKeys, deletedKey)
				}
			}

		}

		return deletedKeys
	}

	log.Info.Printf("Skipping rotation for '%s' keys due to insufficient number of keys. "+
		"Minimum of %d keys required for rotation. Found %d key(s)\n", prefix, retentionCount+1, numKeys)
	return nil

}

// Returns an array of sorted keys by LastModified date.
// The first value in the array is the most recently modified key
func sortKeysAndLogInfo(svc *s3.S3, bucket string, prefix string) ([]s3client.BucketEntry, error) {
	log.Info.Println(`
	######################################
	#        Retrieving Key Info!        #
	######################################
	`)

	log.Info.Printf("Attempting to retrieve list of keys with prefix: '%s'\n", prefix)
	sortedKeys, err := util.RetrieveSortedKeysByTime(svc, bucket, prefix)
	if err != nil {
		log.Error.Printf("Failed to retrieve keys with prefix: '%s' from bucket: %s\n", prefix, bucket)
		return nil, err
	}

	for _, kv := range sortedKeys {
		log.Info.Printf("Found key: '%s'\n", kv.Key)
	}

	log.Info.Printf("Found %d key(s) with '%s' prefix\n", len(sortedKeys), prefix)

	if len(sortedKeys) == 0 {
		return nil, nil
	}
	return sortedKeys, nil
}
