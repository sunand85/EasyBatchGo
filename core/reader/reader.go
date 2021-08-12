package reader

import (
	"core/record"
	"strings"
	"time"
)

type RecordReader interface {
	Open()
	ReadRecord() record.Record
	Close()
}

// This does nothing
type NoOpRecordReader struct {
}

func (n NoOpRecordReader) Open() {
	//NO OP
}

func (n NoOpRecordReader) ReadRecord() record.Record {
	return nil
}

func (n NoOpRecordReader) Close() {
	panic("implement me")
}

// Underway
type FileRecordReader struct {
	Directory string
}

// Split String Record Reader Implementation, splits by space
type SplitStringRecordReader struct {
	CurrentRecordNumber int
	DataSource          string
	splitString         []string
	count               int
	//Scanner bufio.Scanner
}

func (srr *SplitStringRecordReader) Open() {
	println("I am in Split String Record Reader")
	println("Data - ", srr.DataSource)
	srr.CurrentRecordNumber = 0
	srr.splitString = strings.Split(srr.DataSource, " ")
	srr.count = len(srr.splitString)
}

func (srr *SplitStringRecordReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        srr.CurrentRecordNumber,
		Source:        srr.DataSource,
		LocalDateTime: time.Time{},
	}

	if srr.CurrentRecordNumber < srr.count {
		value := srr.splitString[srr.CurrentRecordNumber]
		srr.CurrentRecordNumber++
		return record.NewStringRecord(header, value)
	} else {
		//return record.NewStringRecord(header, nil)
		return nil
	}
	//split := strings.Split(srr.DataSource, " ")
	//split.
	//if srr.Scanner.Scan() {
	//	payload := srr.Scanner.Text()
	//	return record.NewStringRecord(header, payload)
	//} else {
	//	return record.NewStringRecord(header, nil)
	//}

	//return record.NewStringRecord(header, srr.DataSource)
}

func (srr *SplitStringRecordReader) Close() {
	//panic("implement me")
	//NO OP here
}
