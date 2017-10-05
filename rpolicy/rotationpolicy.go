package rpolicy

import "time"

// RotationPolicy defines what rules should be applied for rotating objects in S3
type RotationPolicy struct {
	DailyRetentionPeriod   time.Duration
	DailyRetentionCount    int
	DailyPrefix            string
	WeeklyRetentionPeriod  time.Duration
	WeeklyRetentionCount   int
	WeeklyPrefix           string
	MonthlyPrefix          string
	EnforceRetentionPeriod bool
}
