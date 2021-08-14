package main

import (
	core "core/job"
	"core/reader"
	"core/record"
	"core/writer"
	"fmt"
	"strings"
)

func main() {
	println("========================")

	recordReader := reader.SplitStringRecordReader{DataSource: "sunand is a good boy and razorpay uses golang"}
	recordWriter := writer.StandardOutputRecordWriter{}
	job1 := core.NewJobBuilder().Name("first job").Reader(&recordReader).Writer(&recordWriter).Build()
	job1.Call()

	println("========================")

	job2 := core.NewJobBuilder().Name("second job").Reader(&recordReader).Writer(&recordWriter).Build()
	report := job2.Call()
	fmt.Println("Read Count = ", report.Metrics.ReadCount)

	println("========================")

	stringRecordReader := reader.StringRecordReader{DataSource: "sunand is a good boy and sunand is learning golang"}
	wordCounter := NewWordCounter()
	job3 := core.NewJobBuilder().Name("Third Job").
		Reader(&stringRecordReader).
		Processor(NewLineTokenizer()).
		Processor(wordCounter).Build()

	jobReport := job3.Call()
	fmt.Println("Read Count = ", jobReport.Metrics.ReadCount)

	fmt.Println("Word Counter = ", wordCounter)
}

type LineTokenizer struct {
}

func NewLineTokenizer() *LineTokenizer {
	return &LineTokenizer{}
}

func (l *LineTokenizer) ProcessRecord(rec record.Record) record.Record {
	value := rec.GetPayload().(string)
	//value := string(rec.GetPayload())
	splitString := strings.Split(value, " ")
	return record.NewGenericRecord(rec.GetHeader(), splitString)
}

type WordCounter struct {
	words map[string]int
}

func NewWordCounter() *WordCounter {
	w := make(map[string]int)
	return &WordCounter{words: w}
}

func (w *WordCounter) ProcessRecord(record record.Record) record.Record {
	tokens := record.GetPayload().([]string)
	for _, val := range tokens {
		i := w.words[val]
		if i == 0 {
			w.words[val] = 1
		} else {
			w.words[val]++
		}
	}
	return record
}

func (w *WordCounter) count() map[string]int {
	return w.words
}
