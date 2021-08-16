package eb_json

import "github.com/sunand85/EasyBatchGo/eb-core/record"

type JsonRecord struct {
	Header  record.Header
	Payload string
}

func (j *JsonRecord) GetHeader() record.Header {
	return j.Header
}

func (j *JsonRecord) GetPayload() interface{} {
	return j.Payload
}

func NewJsonRecord(header record.Header, payload string) *JsonRecord {
	return &JsonRecord{Header: header, Payload: payload}
}
