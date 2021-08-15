package reader

import (
	"bufio"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
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

//String Record Reader
type StringRecordReader struct {
	CurrentRecordNumber int
	DataSource          string
	splitString         []string
	count               int
	StringReader        *bufio.Reader
}

func (s *StringRecordReader) Open() {
	s.CurrentRecordNumber = 0
	s.StringReader = bufio.NewReader(strings.NewReader(s.DataSource))
}

func (s *StringRecordReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        s.CurrentRecordNumber,
		LocalDateTime: time.Time{},
	}

	line, _, err := s.StringReader.ReadLine()
	if err != nil {
		return nil
	} else {
		return record.NewStringRecord(header, string(line))
	}
}

func (s *StringRecordReader) Close() {
	//NO OP
}

// Split String Record Reader Implementation, splits by space
type SplitStringRecordReader struct {
	CurrentRecordNumber int
	DataSource          string
	splitString         []string
	count               int
}

func (srr *SplitStringRecordReader) Open() {
	println("Data - ", srr.DataSource)
	srr.CurrentRecordNumber = 0
	srr.splitString = strings.Split(srr.DataSource, " ")
	srr.count = len(srr.splitString)
}

func (srr *SplitStringRecordReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        srr.CurrentRecordNumber,
		LocalDateTime: time.Time{},
	}

	if srr.CurrentRecordNumber < srr.count {
		value := srr.splitString[srr.CurrentRecordNumber]
		srr.CurrentRecordNumber++
		return record.NewStringRecord(header, value)
	} else {
		return nil
	}
}

func (srr *SplitStringRecordReader) Close() {
	//NO OP here
}
