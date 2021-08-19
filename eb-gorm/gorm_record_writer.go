package eb_gorm

import (
	"database/sql"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"gorm.io/gorm"
	"log"
)

type GormRecordWriter struct {
	Db         *gorm.DB
	sqlDB      *sql.DB
	TargetType interface{}
}

func NewGormRecordWriter(db *gorm.DB, targetType interface{}) *GormRecordWriter {
	return &GormRecordWriter{Db: db, TargetType: targetType}
}

func (g *GormRecordWriter) Open() {
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

}

func (g *GormRecordWriter) WriteRecords(batch *record.Batch) {
	//var result = make(map[string]interface{})

	for _, rec := range batch.Records {
		if rec != nil {
			//Payload has to be map[string]interface{} or else gorm will not understand the input

			//g.Db.Model(rec.GetHeader().TargetType).Create(rec.GetPayload())
			//payload := rec.GetPayload().(map[string]string)
			//for key := range payload {
			//	result[key] = payload[key]
			//}

			g.Db.Model(g.TargetType).Create(rec.GetPayload())
		} else {
			log.Fatal("[Sql DB] Found a nil record in batch")
			continue
		}
	}
}

func (g *GormRecordWriter) Close() {
	g.sqlDB.Close()
}
