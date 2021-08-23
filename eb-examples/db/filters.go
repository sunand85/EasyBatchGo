package main

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"log"
)

type AgeFilter struct {
	key   string
	value int64
	op    string
}

func NewAgeFilter(key string, op string, value int64) *AgeFilter {
	return &AgeFilter{key: key, value: value, op: op}
}

func (a *AgeFilter) ProcessRecord(r record.Record) record.Record {
	valueMap := r.GetPayload().(map[string]interface{})
	v := valueMap[a.key]
	var handled bool
	if v != nil {
		lhs, ok := v.(int64)
		if !ok {
			fmt.Println("[Filter] Type Conversion Failed")
			return nil
		}
		switch a.op {
		case ">":
			if lhs > a.value {
				return record.NewGenericRecord(r.GetHeader(), r.GetPayload())
			}
			handled = true
		case "<":
			if lhs < a.value {
				return record.NewGenericRecord(r.GetHeader(), r.GetPayload())
			}
			handled = true
		default:
			fmt.Println("[Filter] Operator is not valid : ", a.op)
			return nil
		}
	}

	if !handled {
		fmt.Println("[Filter] Operator is not valid : ", a.key)
	}
	return nil
}

type TweetFilter struct {
	Tweet Tweet
}

func NewTweetFilter(tweet Tweet) *TweetFilter {
	return &TweetFilter{Tweet: tweet}
}

func (t *TweetFilter) ProcessRecord(r record.Record) record.Record {
	json := jsoniter.ConfigFastest
	toJson, err := json.MarshalToString(r.GetPayload())
	if err != nil {
		log.Fatal("[JSON] Map to Json String Marshalling Failed")
	} else {
		err := json.UnmarshalFromString(toJson, &t.Tweet)
		if err != nil {
			log.Fatal("[JSON] Json String to Object UnMarshalling Failed")
		}
	}

	fmt.Println("Tweet : ", t.Tweet)
	if t.isValid() {
		return r
	} else {
		return nil
	}
}

func (t *TweetFilter) isValid() bool {
	if t.Tweet.Handle == "sunand" {
		return false
	} else {
		return true
	}
}
