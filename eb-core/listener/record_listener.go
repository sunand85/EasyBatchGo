package listener

type ReadRecordListener interface {
	BeforeRecordReading()
	AfterRecordReading()
}

type WriteRecordListener interface {
	BeforeRecordWriting()
	AfterRecordWriting()
}
