package processor

import "core/record"

type RecordProcessor interface {
	ProcessRecord(record record.Record) record.Record
}
