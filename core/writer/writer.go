package writer

import (
	"core/record"
	"fmt"
)

type RecordWriter interface {
	Open()
	WriteRecords(batch *record.Batch)
	Close()
}

//This does nothing
type NoOpWriter struct {
}

func (n NoOpWriter) Open() {
	//NO OP
}

func (n NoOpWriter) WriteRecords(batch *record.Batch) {
	fmt.Println("Doing Nothing - NO_OP")
}

func (n NoOpWriter) Close() {
	//NO OP
}

//Console Writer
type StandardOutputRecordWriter struct {
}

func (s StandardOutputRecordWriter) Open() {
	//panic("implement me")
	//No Op
}

func (s StandardOutputRecordWriter) WriteRecords(batch *record.Batch) {
	for _, val := range batch.Records {
		fmt.Println("Result : ", val.GetPayload())
	}
}

func (s StandardOutputRecordWriter) Close() {
	//NO OP
}
