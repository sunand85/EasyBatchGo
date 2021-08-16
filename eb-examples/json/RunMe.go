package main

import (
	"fmt"
	core "github.com/sunand85/EasyBatchGo/eb-core/job"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	writer "github.com/sunand85/EasyBatchGo/eb-core/writer"
	eb_json "github.com/sunand85/EasyBatchGo/eb-json"
)

func main() {

	jsonFileReader := eb_json.NewJsonFileReader("./eb-examples/json/resources/abc.json")
	job := core.NewJobBuilder().
		Name("Json File Job").
		Reader(jsonFileReader).
		Processor(NewAgeFilter("age", "<", 21)).
		Writer(writer.StandardOutputRecordWriter{}).
		Build()

	report := job.Call()
	fmt.Println("Metrics ", report.Metrics)
}

type AgeFilter struct {
	key   string
	value float64
	op    string
}

func NewAgeFilter(key string, op string, value float64) *AgeFilter {
	return &AgeFilter{key: key, value: value, op: op}
}

func (a *AgeFilter) ProcessRecord(r record.Record) record.Record {
	valueMap := r.GetPayload().(map[string]interface{})
	v := valueMap[a.key]
	var handled bool
	if v != nil {
		lhs, ok := v.(float64)
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

//Sticking with map[string]interface{} for now
type User struct {
	Name string `json:"name"`
	Role string `json:"role"`
	Age  string `json:"age"`
}
