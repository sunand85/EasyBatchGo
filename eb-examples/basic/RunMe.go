package main

import (
	"fmt"
	core "github.com/sunand85/EasyBatchGo/eb-core/job"
	"github.com/sunand85/EasyBatchGo/eb-core/reader"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
	"strings"
)

func main() {
	println("========================")

	recordReader := reader.SplitStringRecordReader{DataSource: "sunand is a good boy and learning golang"}
	recordWriter := writer.StandardOutputRecordWriter{}
	job1 := core.NewJobBuilder().Name("Split String Job").Reader(&recordReader).Writer(&recordWriter).Build()
	report := job1.Call()
	fmt.Println("Metrics : ", report.Metrics)
	println("========================")
	/*
		job2 := core.NewJobBuilder().Name("Second Job").Reader(&recordReader).Writer(&recordWriter).Build()
		report = job2.Call()
		fmt.Println("Metrics : ", report.Metrics)
		println("========================")*/

	stringRecordReader := reader.StringRecordReader{DataSource: "Appu is sleeping and Amma is sleeping and Appa is sleeping"}
	lineTokenizer := NewLineTokenizer()
	wordCounter := NewWordCounter()
	job3 := core.NewJobBuilder().Name("Word Count Job").
		Reader(&stringRecordReader).
		Processor(lineTokenizer).
		Processor(wordCounter).Build()

	report = job3.Call()
	fmt.Println("Read Count = ", report.Metrics.ReadCount)
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
		w.words[val]++

		/*i := w.words[val]
		if i == 0 {
			w.words[val] = 1
		} else {
			w.words[val]++
		}*/
	}
	return record
}

func (w *WordCounter) count() map[string]int {
	return w.words
}

//ToDo write a JSON example
type AgeValidator struct {
}

/*
func (a *AgeValidator) ProcessRecord(r record.Record) record.Record {
	//r.GetPayload()
}*/
