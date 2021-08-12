package main

import (
	core "core/job"
	"core/reader"
	"core/writer"
	"fmt"
)

func main() {
	//stringRecord1 := record.StringRecord{Header: nil, Payload: "sunand 1"}
	//stringRecord2 := record.StringRecord{Header: nil, Payload: "sunand 2"}
	recordReader := reader.SplitStringRecordReader{DataSource: "sunand is a good boy and razorpay uses golang"}
	recordWriter := writer.StandardOutputRecordWriter{}
	job1 := core.NewJobBuilder().Name("first job").Reader(&recordReader).Writer(&recordWriter).Build()
	job1.Call()

	println("============")

	job2 := core.NewJobBuilder().Name("second job").Reader(&recordReader).Writer(&recordWriter).Build()
	report := job2.Call()
	fmt.Println("Read Count = ", report.Metrics.ReadCount)
}
