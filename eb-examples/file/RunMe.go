package main

import (
	"fmt"
	core "github.com/sunand85/EasyBatchGo/eb-core/job"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
	file "github.com/sunand85/EasyBatchGo/eb-file"
)

func main() {

	fileRecordReader := file.NewFlatFileRecordReader("./eb-examples/file/resources/abc.csv")

	var Dummy interface{}

	//names := file.WithFieldNames("id", "handle", "tweet")
	delimitedRecordMapper := file.NewDelimitedRecordMapper(Dummy).WithFieldNames("id", "handle", "tweet")
	//delimitedRecordMapper := file.NewDelimitedRecordMapper(Dummy)//.WithFieldNames("id", "handle", "tweet")
	job := core.NewJobBuilder().Name("Read CSV").
		Reader(fileRecordReader).
		Processor(delimitedRecordMapper).
		Filter(file.NewHeaderRecordFilter()).
		Writer(writer.StandardOutputRecordWriter{}).
		Build()

	report := job.Call()

	fmt.Println("Read Count : ", report.Metrics.ReadCount)
	fmt.Println("Write Count : ", report.Metrics.WriteCount)
	fmt.Println("===================================")
	fmt.Println("The End")
}

type Dummy interface {
}
