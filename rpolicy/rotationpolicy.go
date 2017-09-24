package rpolicy

import "time"

type RotationPolicy struct {
	DailyRetentionPeriod time.Duration
	DailyRetentionCount int
	DailyPrefix string
	WeeklyRetentionPeriod time.Duration
	WeeklyRetentionCount int
	WeeklyPrefix string
	MonthlyPrefix string
}

