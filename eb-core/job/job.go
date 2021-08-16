package core

import "time"

type Job interface {
	GetName() string
	Call() JobReport
}

type JobReport struct {
	Name       string
	Parameters JobParameters
	Metrics    *JobMetrics
	Status     JobStatus
	//systemProperties
}

type JobParameters struct {
}

type JobMetrics struct {
	StartTime     time.Time
	EndTime       time.Time
	ReadCount     int
	WriteCount    int
	FilterCount   int
	ErrorCount    int
	CustomMetrics map[string]interface{}
}

func (jr JobReport) toString() string {
	return "Job Report"
}

/*type RecordTracker struct {
	MoreRecords bool `default:true`
}

func NewRecordTracker(moreRecords bool) *RecordTracker {
	return &RecordTracker{MoreRecords: true}
}*/
