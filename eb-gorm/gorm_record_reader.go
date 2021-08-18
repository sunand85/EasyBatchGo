package eb_gorm

import (
	"database/sql"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"gorm.io/gorm"
	"log"
	"time"
)

const DEFAULT_LIMIT = 1000

type GormRecordReader struct {
	Db                  *gorm.DB
	sqlDB               *sql.DB
	Table               string
	Query               string
	Params              []string
	Offset              int
	Limit               int
	CurrentRecordNumber int
	TargetType          interface{}

	records []interface{}
	rows    *sql.Rows
}

func NewGormRecordReader(db *gorm.DB, query string, params []string, targetType interface{}) *GormRecordReader {
	return &GormRecordReader{Db: db, Query: query, Params: params, TargetType: targetType}
}

func (g *GormRecordReader) Open() {
	var err error // I can also add (err error) in the function definition
	g.sqlDB, err = g.Db.DB()
	if err != nil {
		log.Fatal("[SQL DB] Couldn't get instance of DB")
		return
	}
	err = g.sqlDB.Ping()
	if err != nil {
		log.Fatal("[SQL DB] Ping failed : ", g.Db)
	}

	//ToDo to set the connection pool size here, may be take them as params

	g.rows, err = g.Db.Raw(g.Query, g.Params).Rows() //.Offset(g.Offset).Limit(g.Limit)
	if err != nil {
		log.Fatal("[SQL DB] Failed to execute query : ", g.Query)
	}

}

func (g *GormRecordReader) ReadRecord() record.Record {

	if g.rows.Next() {
		result := make(map[string]interface{})
		//var result = g.TargetType
		err := g.Db.ScanRows(g.rows, result)
		//err := g.Db.ScanRows(g.rows, g.TargetType)
		if err != nil {
			log.Fatal("[SQL DB] Unable To Read record in the right format ", err)
		}
		g.CurrentRecordNumber++
		header := g.createHeader()
		//return record.NewGenericRecord(header, g.TargetType)
		return record.NewGenericRecord(header, result)
	} else {
		return nil
	}
}

func (g *GormRecordReader) createHeader() record.Header {
	header := record.Header{
		Number:        g.CurrentRecordNumber,
		Source:        g.Query,
		LocalDateTime: time.Now(),
		TargetType:    g.TargetType,
	}
	return header
}

func (g *GormRecordReader) Close() {
	g.sqlDB.Close()
}
