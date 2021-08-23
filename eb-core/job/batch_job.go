package core

import (
	"fmt"
	"github.com/sunand85/EasyBatchGo/eb-core/listener"
	"github.com/sunand85/EasyBatchGo/eb-core/processor"
	"github.com/sunand85/EasyBatchGo/eb-core/reader"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
	"time"
)

type BatchJob struct {
	name                string
	RecordReader        reader.RecordReader
	RecordProcessors    []processor.RecordProcessor
	RecordWriter        writer.RecordWriter
	ReadRecordListener  listener.ReadRecordListener
	WriteRecordListener listener.WriteRecordListener
	Parameters          JobParameters
	Metrics             JobMetrics
	Report              JobReport
	Tracker             record.TrackRecord
}

func (b *BatchJob) GetName() string {
	return b.name
}

func (b *BatchJob) Call() JobReport {
	start(b) //init code
	defer tearDown(b)
	b.openReader()
	b.openWriter()
	b.setStatus(STARTED)
	var batch = record.Batch{}
	for b.moreRecords() && !isInterrupted() {
		//b.readAndProcessBatch() //ToDo revisit
		//b.writeBatch(batch) //ToDo revisit
		readRecord := b.readRecord()
		if readRecord != nil && readRecord.GetPayload() != nil {
			//Adding list of processors which needs to be executed in sequence
			if len(b.RecordProcessors) > 0 {
				readRecord = b.processRecord(readRecord)
				//Chance of record becoming nil post processing. Especially when processors are used as Filters.
				if readRecord == nil {
					b.Metrics.FilterCount++
					continue
				}
			}
			batch.AddRecord(readRecord)
		} else {
			b.Tracker.NoMoreRecords()
		}
	}

	if b.RecordWriter != nil {
		if b.WriteRecordListener != nil {
			b.WriteRecordListener.BeforeRecordWriting()
			b.writeBatch(&batch)
			b.WriteRecordListener.AfterRecordWriting()
		} else {
			b.writeBatch(&batch)
		}
	}

	b.setStatus(STOPPING)
	finish(b)

	return b.Report
}

func (b *BatchJob) openReader() {
	b.RecordReader.Open()
}

func (b *BatchJob) openWriter() {
	if b.RecordWriter != nil {
		b.RecordWriter.Open()
	}
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
	b.processRecord(nil)

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
func (b *BatchJob) processRecord(rec record.Record) record.Record {
	var processedRecord record.Record
	for _, recordProcessor := range b.RecordProcessors {
		processedRecord = recordProcessor.ProcessRecord(rec)
		if processedRecord == nil {
			return nil
		}
		rec = processedRecord //Feeding the processed record to the next processor in line
	}

	return rec
}

func (b *BatchJob) writeBatch(batch *record.Batch) {
	fmt.Println("Starting to Write")
	if batch != nil {
		b.RecordWriter.WriteRecords(batch)
		b.Metrics.WriteCount = batch.Size()
	}
}

func tearDown(bj *BatchJob) {
	//Account for ABORTED if the process is interrupted
	//Close all the Readers, Writers
	if bj.RecordReader != nil {
		bj.RecordReader.Close()
	}
	if bj.RecordWriter != nil {
		bj.RecordWriter.Close()
	}
}

func finish(bj *BatchJob) {
	//Capturing End Time, Metrics
	bj.Metrics.EndTime = time.Now()
	//bj.Report.Metrics = bj.Metrics
	bj.setStatus(COMPLETED)
}

func start(bj *BatchJob) {
	fmt.Println("Job Name : ", bj.GetName())
	bj.setStatus(STARTING)
	bj.Metrics.StartTime = time.Now()
	bj.Tracker.MoreRecords = true
	bj.Report.Metrics = &bj.Metrics
}

func (b *BatchJob) setStatus(jobStatus JobStatus) {
	b.Report.Status = jobStatus
}

func (b BatchJob) GetStatus() JobStatus {
	return b.Report.Status
}
