package main

import (
	"fmt"
	core "github.com/sunand85/EasyBatchGo/eb-core/job"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
	eb_gorm "github.com/sunand85/EasyBatchGo/eb-gorm"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"time"
)

func main() {
	//ToDo add proper create table syntax to run the program smoothly
	dsn := "root:@tcp(127.0.0.1:3306)/demo?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("[SQL DB] Gorm Db Connection Open failed ", dsn)
	}
	var params []string
	userLogs := UserLogs{}
	gormRecordReader := eb_gorm.NewGormRecordReader(db, "select * from userslogs_test", params, &userLogs)
	job := core.NewJobBuilder().
		Name("Database Read Job").
		Reader(gormRecordReader).
		Writer(writer.NewStandardOutputRecordWriter()).
		Build()

	report := job.Call()
	fmt.Println("Metrics Read Count : ", report.Metrics.ReadCount)

	/*gormRecordReader.Open()
	for {
		record := gormRecordReader.ReadRecord()
		if record != nil {
			targetType := record.GetHeader().TargetType
			fmt.Println("Target Type :", reflect.TypeOf(targetType))
			fmt.Println("Data : ", record.GetPayload())
		} else {
			break
		}
	}*/
}

//Gorm Model
type UserLogs struct {
	Username string
	Logdata  string
	Created  time.Time
}
