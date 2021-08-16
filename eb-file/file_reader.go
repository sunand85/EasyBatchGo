package eb_file

import (
	"bufio"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"log"
	"os"
	"time"
)

type FlatFileRecordReader struct {
	Path                string
	Charset             string
	FlatFile            *os.File
	FlatFileReader      *bufio.Reader
	CurrentRecordNumber int
}

func NewFlatFileRecordReader(path string) *FlatFileRecordReader {
	return &FlatFileRecordReader{Path: path}
}

func (ffr *FlatFileRecordReader) Open() {
	file, err := os.Open(ffr.Path)
	if err != nil {
		log.Fatal("[FILE] ", err)
	}
	ffr.FlatFile = file
	ffr.FlatFileReader = bufio.NewReader(ffr.FlatFile)
	ffr.CurrentRecordNumber = 1
}

func (ffr *FlatFileRecordReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        ffr.CurrentRecordNumber,
		Source:        ffr.Path,
		LocalDateTime: time.Now(),
	}

	line, _, err := ffr.FlatFileReader.ReadLine()

	if err != nil {
		return nil
	} else {
		ffr.CurrentRecordNumber++
		return record.NewStringRecord(header, string(line))
	}
}

func (ffr *FlatFileRecordReader) Close() {
	ffr.FlatFile.Close()
}
