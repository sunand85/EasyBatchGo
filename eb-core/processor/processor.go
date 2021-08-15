package processor

import "github.com/sunand85/EasyBatchGo/eb-core/record"

type RecordProcessor interface {
	ProcessRecord(r record.Record) record.Record
}
