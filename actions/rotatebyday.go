package actions

import (
	"time"
	"github.com/aws/aws-sdk-go/service/s3"
	"fmt"
	log "github.com/Sirupsen/logrus"
)

func rotate(svc *s3.S3, bucket string, retentionPeriod time.Duration, retentionCount int, prefix string){
	fmt.Println(`
	######################################
	#        Retrieving Key Info!        #
	######################################
	`)

	log.Info(fmt.Sprintf("attempting to retrieve list of keys with prefix: '%s'", prefix))
	sortedKeys, err := retrieveSortedKeys(svc, bucket, prefix)
	if err != nil {
		log.Error(fmt.Sprintf("failed to retrieve keys with prefix: '%s' from bucket: %s", prefix, bucket))
		return
	}

	log.Info(fmt.Sprintf("found %d key(s) with '%s' prefix", len(sortedKeys), prefix))
	for _, kv := range sortedKeys {
		log.Info(fmt.Sprintf("found key: '%s'", kv.Key))
	}

	fmt.Println(`
	######################################
	#           Rotating Keys!           #
	######################################
	`)
	rotateByDay(svc, bucket, sortedKeys, retentionPeriod, retentionCount, prefix)
}

// Any keys with prefix _monthly should have a life cycle policy to move into glacier after 30 days
func rotateByDay(svc *s3.S3, bucket string, sortedKeys []BucketEntry, retentionPeriod time.Duration, retentionCount int, prefix string) []string {
	if sortedKeys == nil { //no keys, no need to rotateByTime
		log.Info(fmt.Sprintf("no '%s' key(s) found for rotation", prefix))
		return nil
	}

	deletedKeys := []string{}

	numKeys := len(sortedKeys)
	if numKeys > retentionCount {
		log.Info(fmt.Sprintf("total number of keys (%d) exceeds retention policy of %d keys, purging old keys",
			numKeys, retentionCount))
		for _, kv := range sortedKeys[retentionCount:] {
			key := kv.Key

			timeDelta := time.Since(kv.ModifiedTime)

			log.Info(fmt.Sprintf("candidate for deletion: '%s' is %0.1f hours / %0.1f minutes old", key, timeDelta.Hours(), timeDelta.Minutes()))

			// Safety check to ensure that candidate keys for deletion are not within the retentionPeriod
			// This will only log a warning but should be checked to ensure that the correct behaviour is being observed
			if timeDelta <= retentionPeriod {
				log.Warn(fmt.Sprintf("key: '%s' is in violation of rentention policy count, however, the key is younger "+
					"than the retention period of %0.1f hours / %0.1f minutes. The key will still be deleted", key, retentionPeriod.Hours(), retentionPeriod.Minutes()))
			}

			deletedKey, err := deleteKey(svc, bucket, key)
			if err != nil {
				log.Error(fmt.Sprintf("failed to delete key from bucket: '%s'", key))
			} else {
				log.Info(fmt.Sprintf("successfully deleted key from bucket: '%s'", key))
				deletedKeys = append(deletedKeys, deletedKey)
			}
		}
	}

	log.Info(fmt.Sprintf("skipping rotation for %s keys due to insufficient number of keys. "+
		"minimum of %d keys required for rotation, found %d key(s) ", prefix, retentionCount+1, numKeys))
	return nil

}
