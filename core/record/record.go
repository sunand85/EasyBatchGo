package record

import "time"

type Record interface {
	GetHeader() Header
	GetPayload() interface{}
}

type Header struct {
	Number        int
	Source        string
	LocalDateTime time.Time
	//Scanned bool

}

//GenericRecord implementing Record Interface
type GenericRecord struct {
	Header  Header
	Payload interface{}
}

func (gr GenericRecord) GetHeader() Header {
	return gr.Header
}

func (gr GenericRecord) GetPayload() interface{} {
	return gr.Payload
}

//TBD
type Batch struct {
	Records []Record
}

func (b *Batch) AddRecord(record Record) {
	b.Records = append(b.Records, record)
}

func (b *Batch) RemoveRecord(record Record) { //Need to test this below
	index := SliceIndex(len(b.Records), func(i int) bool { return b.Records[i] == record })
	b.Records = append(b.Records[:index], b.Records[index+1:]...)
}

func (b *Batch) Size() int {
	return len(b.Records)
}

//Move this to utility functions
func SliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}

//========String Record ===========//
type StringRecord struct {
	Header  Header
	Payload interface{}
}

func NewStringRecord(header Header, payload interface{}) *StringRecord {
	return &StringRecord{Header: header, Payload: payload}
}

func (s StringRecord) GetHeader() Header {
	panic("implement me")
}

func (s *StringRecord) GetPayload() interface{} {
	return s.Payload
}

//======= Record Tracker =========
type TrackRecord struct {
	MoreRecords bool
}

func NewTrackRecord() *TrackRecord {
	return &TrackRecord{MoreRecords: true}
}

func (tr *TrackRecord) NoMoreRecords() {
	tr.MoreRecords = false
}
