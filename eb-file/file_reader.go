package eb_file

import (
	"bufio"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"log"
	"os"
	"time"
)

type FlatFileRecordReader struct {
	Path                string
	Charset             string
	FileReader          *bufio.Reader
	CurrentRecordNumber int
}

func NewFlatFileRecordReader(path string) *FlatFileRecordReader {
	return &FlatFileRecordReader{Path: path}
}

func (ffr *FlatFileRecordReader) Open() {
	file, err := os.Open(ffr.Path)
	if err != nil {
		log.Fatal("[FILE] ", err)
	}
	ffr.FileReader = bufio.NewReader(file)
	ffr.CurrentRecordNumber = 1
}

func (ffr *FlatFileRecordReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        ffr.CurrentRecordNumber,
		Source:        ffr.Path,
		LocalDateTime: time.Now(),
	}

	line, _, err := ffr.FileReader.ReadLine()

	if err != nil {
		return nil
	} else {
		ffr.CurrentRecordNumber++
		return record.NewStringRecord(header, string(line))
	}
}

func (ffr *FlatFileRecordReader) Close() {
	//NO OP
}

/*func (ffr *FlatFileRecordReader) ReadFile(path string) {
	//file, err := ioutil.ReadFile("abc.csv")
	file, err := os.Open("./eb-file/abc.csv")
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Println("Record = " + string(line))
	}
}
*/
