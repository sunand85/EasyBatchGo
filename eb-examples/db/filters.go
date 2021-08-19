package main

import (
	"fmt"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
)

type AgeFilter struct {
	key   string
	value int64
	op    string
}

func NewAgeFilter(key string, op string, value int64) *AgeFilter {
	return &AgeFilter{key: key, value: value, op: op}
}

func (a *AgeFilter) ProcessRecord(r record.Record) record.Record {
	valueMap := r.GetPayload().(map[string]interface{})
	v := valueMap[a.key]
	var handled bool
	if v != nil {
		lhs, ok := v.(int64)
		if !ok {
			fmt.Println("[Filter] Type Conversion Failed")
			return nil
		}
		switch a.op {
		case ">":
			if lhs > a.value {
				return record.NewGenericRecord(r.GetHeader(), r.GetPayload())
			}
			handled = true
		case "<":
			if lhs < a.value {
				return record.NewGenericRecord(r.GetHeader(), r.GetPayload())
			}
			handled = true
		default:
			fmt.Println("[Filter] Operator is not valid : ", a.op)
			return nil
		}
	}

	if !handled {
		fmt.Println("[Filter] Operator is not valid : ", a.key)
	}
	return nil
}
