package filter

import (
	"github.com/sunand85/EasyBatchGo/eb-core/record"
)

type EmptyStringRecordFilter struct {
}

func NewEmptyStringRecordFilter() *EmptyStringRecordFilter {
	return &EmptyStringRecordFilter{}
}

func (esr *EmptyStringRecordFilter) ProcessRecord(r record.Record) record.Record {
	if r.GetPayload().(string) == "" {
		return nil
	} else {
		return r
	}
}
