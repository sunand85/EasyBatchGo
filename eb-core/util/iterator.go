package util

import "github.com/sunand85/EasyBatchGo/eb-core/record"

type StatefulIterator interface {
	Value() int
	Next() bool
}

type RecordIterator struct {
	current int
	data    []*record.Record
}

func NewRecordIterator(data []*record.Record) *RecordIterator {
	return &RecordIterator{current: -1, data: data}
}

func NewRecordStatefulIteratorInterface(data []*record.Record) StatefulIterator {
	return &RecordIterator{data: data, current: -1}
}

func (it *RecordIterator) Value() int {
	rec := it.data[it.current]
	if rec != nil {
		return it.current
	} else {
		return 0
	}

}
func (it *RecordIterator) Next() bool {
	it.current++
	if it.current >= len(it.data) {
		return false
	}
	return true
}
