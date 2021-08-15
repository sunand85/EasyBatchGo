package core

type JobStatus int

const (
	STARTING JobStatus = iota
	STARTED
	STOPPING
	FAILED
	COMPLETED
	ABORTED
)

func (js JobStatus) String() string {
	return [...]string{"Starting", "Started", "Stopping", "Failed", "Completed", "Aborted"}[js-1]
}
