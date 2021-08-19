package eb_file

import (
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"strconv"
	"strings"
)

const (
	DEFAULT_DELIMITER           = ","
	DEFAULT_QUALIFIER           = ""
	DEFAULT_WHITESPACE_TRIMMING = false
)

type DelimitedRecordMapper struct {
	targetType interface{}
	fieldNames []string
	//fieldPositions []int

	defaultDelimiter              string
	defaultQualifier              string
	trimWhiteSpaces               bool
	fieldNamesRetrievedFromHeader bool

	recordExpectedLength int
}

func NewDelimitedRecordMapper(targetType interface{}) *DelimitedRecordMapper {
	return &DelimitedRecordMapper{
		targetType:       targetType,
		defaultDelimiter: DEFAULT_DELIMITER,
		defaultQualifier: DEFAULT_QUALIFIER,
	}
}

//type Option func(DelimitedRecordMapper) DelimitedRecordMapper

func (d *DelimitedRecordMapper) WithFieldNames(fields ...string) *DelimitedRecordMapper {
	d.fieldNames = fields
	return d
}

func (d *DelimitedRecordMapper) WithExpectedRecordLength(recordLength int) *DelimitedRecordMapper {
	d.recordExpectedLength = recordLength
	return d
}

func (d *DelimitedRecordMapper) WithDelimiter(delimiter string) *DelimitedRecordMapper {
	d.defaultDelimiter = delimiter
	return d
}

func (d *DelimitedRecordMapper) WithQualifier(qualifier string) *DelimitedRecordMapper {
	d.defaultQualifier = qualifier
	return d
}

func (d *DelimitedRecordMapper) ProcessRecord(r record.Record) record.Record {
	//panic("implement me")
	fieldContents := make(map[string]interface{})
	fields := d.parseRecord(r)
	for i, field := range fields {
		var fieldName string
		if d.fieldNamesRetrievedFromHeader {
			fieldName = d.fieldNames[field.index]
		} else {
			fieldName = d.fieldNames[i]
		}
		fieldContents[fieldName] = field.rawContent
	}

	return record.NewGenericRecord(r.GetHeader(), fieldContents)
}

func (d *DelimitedRecordMapper) parseRecord(r record.Record) []*Field {
	payload := r.GetPayload().(string)
	tokens := strings.Split(payload, d.defaultDelimiter)
	d.setRecordExpectedLength(tokens)
	d.setFieldNames(tokens)
	d.checkRecordLength(tokens)
	d.checkQualifier(tokens)

	fields := make([]*Field, d.recordExpectedLength)
	for index, token := range tokens {
		token = strings.TrimSpace(token)
		token = d.removeQualifier(token)
		fields[index] = NewField(index, token)
	}

	return fields
}

func (d *DelimitedRecordMapper) setRecordExpectedLength(tokens []string) {
	if d.recordExpectedLength == 0 {
		d.recordExpectedLength = len(tokens)
	}
}

func (d *DelimitedRecordMapper) setFieldNames(tokens []string) {
	if len(d.fieldNames) == 0 {
		d.fieldNames = make([]string, d.recordExpectedLength)
		d.fieldNamesRetrievedFromHeader = true
		for i, token := range tokens {
			token = strings.TrimSpace(token)
			token = d.removeQualifier(token)
			d.fieldNames[i] = token
		}
	}
}

func (d *DelimitedRecordMapper) checkRecordLength(tokens []string) {
	if len(tokens) != d.recordExpectedLength {
		var errMessage string = "Record Length (" + strconv.Itoa(len(tokens)) + ") fieldNames not equal to expected length of " + strconv.Itoa(d.recordExpectedLength) + " fieldNames"
		panic(errMessage)
	}
}

func (d *DelimitedRecordMapper) checkQualifier(tokens []string) {
	if len(d.defaultQualifier) > 0 {
		for _, token := range tokens {
			if !strings.HasPrefix(token, d.defaultQualifier) || !strings.HasSuffix(token, d.defaultQualifier) {
				errMessage := "Field [" + token + "] is not enclosed as expected with '" + d.defaultQualifier + "'"
				panic(errMessage)
			}
		}
	}
}

func (d *DelimitedRecordMapper) removeQualifier(token string) string {
	qLen := len(d.defaultQualifier)
	if qLen > 0 {
		return token[qLen:(len(token) - qLen)]
	}
	return token
}
