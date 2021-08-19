package core

import (
	"github.com/sunand85/EasyBatchGo/eb-core/listener"
	"github.com/sunand85/EasyBatchGo/eb-core/processor"
	"github.com/sunand85/EasyBatchGo/eb-core/reader"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
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

func (jb *JobBuilder) Validator(processor processor.RecordProcessor) *JobBuilder {
	jb.BatchJob.RecordProcessors = append(jb.BatchJob.RecordProcessors, processor)
	return jb
}

func (jb *JobBuilder) Filter(processor processor.RecordProcessor) *JobBuilder {
	jb.BatchJob.RecordProcessors = append(jb.BatchJob.RecordProcessors, processor)
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

func (jb *JobBuilder) WriteListener(listener listener.WriteRecordListener) *JobBuilder {
	jb.BatchJob.WriteRecordListener = listener
	return jb
}

func (jb *JobBuilder) ReadListener(listener listener.WriteRecordListener) *JobBuilder {
	jb.BatchJob.WriteRecordListener = listener
	return jb
}

func (jb *JobBuilder) Build() BatchJob {
	return jb.BatchJob
}
