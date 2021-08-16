package eb_json

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"io/ioutil"
	"log"
	"os"
	"reflect"
)

type JsonFileReader struct {
	Path                string
	File                *os.File
	DataSource          []byte
	TargetType          interface{}
	data                []interface{}
	isArray             bool
	count               int
	CurrentRecordNumber int
}

func NewJsonFileReader(path string) *JsonFileReader {
	return &JsonFileReader{Path: path}
}

func (j *JsonFileReader) Open() {
	jsonFile, fErr := os.Open(j.Path)
	if fErr != nil {
		log.Fatal("[JSON] Failed to Open file ", fErr)
	}
	j.File = jsonFile

	all, err := ioutil.ReadAll(j.File)
	if err != nil {
		log.Fatal("[JSON] Failed to Read file ", err)
	} else {
		j.DataSource = all
	}

	var json = jsoniter.ConfigFastest
	//Since the target type is declared as interface it will be either converted to map[string]interface{} or []interface{}
	jsonErr := json.Unmarshal(j.DataSource, &j.TargetType)
	if err != nil {
		log.Fatal("[JSON] Unmarshall Failed ", jsonErr)
	}

	rt := reflect.TypeOf(j.TargetType)
	switch rt.Kind() {
	case reflect.Slice:
		j.data = j.TargetType.([]interface{})
	default:
		j.data = append(j.data, j.TargetType)
	}

	j.count = len(j.data)
}

func (j *JsonFileReader) ReadRecord() record.Record {
	header := record.Header{
		Number:        j.CurrentRecordNumber + 1,
		Source:        j.Path,
		LocalDateTime: time.Now(),
	}
	if j.CurrentRecordNumber < j.count {
		value := j.data[j.CurrentRecordNumber]
		j.CurrentRecordNumber++
		return record.NewGenericRecord(header, value)
	} else {
		return nil
	}
}

func (j *JsonFileReader) Close() {
	j.File.Close()
}
