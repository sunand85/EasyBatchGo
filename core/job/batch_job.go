package core

import (
	"core/processor"
	"core/reader"
	"core/record"
	"core/writer"
	"fmt"
	"time"
)

type BatchJob struct {
	name            string
	RecordReader    reader.RecordReader
	RecordProcessor processor.RecordProcessor
	RecordWriter    writer.RecordWriter
	Parameters      JobParameters
	Metrics         JobMetrics
	Report          JobReport
	Tracker         record.TrackRecord
}

func (b *BatchJob) GetName() string {
	return b.name
}

func (b *BatchJob) Call() JobReport {
	start(b) //init code
	b.openReader()
	b.openWriter()
	b.setStatus(STARTED)
	var batch = record.Batch{}
	for b.moreRecords() && !isInterrupted() {
		//b.readAndProcessBatch() //ToDo revisit
		//b.writeBatch(batch) //ToDo revisit
		readRecord := b.readRecord()
		if readRecord != nil {
			batch.AddRecord(readRecord)
		} else {
			b.Tracker.NoMoreRecords()
		}
	}

	b.writeBatch(&batch)
	b.setStatus(STOPPING)
	tearDown(b)
	return b.Report
}

func (b *BatchJob) openReader() {
	b.RecordReader.Open()
}

func (b *BatchJob) openWriter() {
	b.RecordWriter.Open()
}

//ToDo
func (b *BatchJob) moreRecords() bool {
	return b.Tracker.MoreRecords
}

//ToDo
func isInterrupted() bool {
	return false
}

//ToDo batch handling, right now its just a single record reader. Infact do not use this method for now
func (b BatchJob) readAndProcessBatch() record.Batch {

	var batch record.Batch
	readRecord := b.readRecord()
	if readRecord != nil {
		b.Metrics.ReadCount++
	}
	b.processRecord()

	return batch
}

//Decide if b BatchJob should be a pointer or not so that state is maintained across execution for this object
func (b *BatchJob) readRecord() record.Record {
	//fmt.Println("Reading Record")
	readRecord := b.RecordReader.ReadRecord()
	if readRecord != nil {
		b.Metrics.ReadCount++
	}
	return readRecord
}

//ToDO
func (b *BatchJob) processRecord() {
	//NO OP
}

func (b *BatchJob) writeBatch(batch *record.Batch) {
	fmt.Println("Starting to Write")
	b.RecordWriter.WriteRecords(batch)
}

func tearDown(bj *BatchJob) {
	//Account for ABORTED if the process is interrupted
	bj.Metrics.EndTime = time.Now()
	bj.Report.Metrics = bj.Metrics
	bj.setStatus(COMPLETED)
}

func start(bj *BatchJob) {
	fmt.Println("Job Name : ", bj.GetName())
	bj.setStatus(STARTING)
	bj.Metrics.StartTime = time.Now()
	bj.Tracker.MoreRecords = true
}

func (b *BatchJob) setStatus(jobStatus JobStatus) {
	b.Report.Status = jobStatus
}

func (b BatchJob) GetStatus() JobStatus {
	return b.Report.Status
}
