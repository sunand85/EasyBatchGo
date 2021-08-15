package eb_file

import "github.com/sunand85/EasyBatchGo/eb-core/record"

type HeaderRecordFilter struct {
}

func NewHeaderRecordFilter() *HeaderRecordFilter {
	return &HeaderRecordFilter{}
}

func (h *HeaderRecordFilter) ProcessRecord(r record.Record) record.Record {
	if r.GetHeader().Number == 1 { //Skipping this record
		return nil
	}
	return r
}
