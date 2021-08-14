package core

import (
	"core/processor"
	"core/reader"
	"core/writer"
)

type JobBuilder struct {
	BatchJob BatchJob
}

func NewJobBuilder() *JobBuilder {
	return &JobBuilder{}
}

func (jb *JobBuilder) Name(name string) *JobBuilder {
	jb.BatchJob.name = name
	return jb
}

func (jb *JobBuilder) Reader(reader reader.RecordReader) *JobBuilder {
	jb.BatchJob.RecordReader = reader
	return jb
}

func (jb *JobBuilder) Processor(processor processor.RecordProcessor) *JobBuilder {
	jb.BatchJob.RecordProcessors = append(jb.BatchJob.RecordProcessors, processor)
	return jb
}

func (jb *JobBuilder) Writer(writer writer.RecordWriter) *JobBuilder {
	jb.BatchJob.RecordWriter = writer
	return jb
}

func (jb *JobBuilder) Build() BatchJob {
	return jb.BatchJob
}
