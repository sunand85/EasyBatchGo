package main

import (
	"fmt"
	core "github.com/sunand85/EasyBatchGo/eb-core/job"
	"github.com/sunand85/EasyBatchGo/eb-core/record"
	"github.com/sunand85/EasyBatchGo/eb-core/writer"
	eb_gorm "github.com/sunand85/EasyBatchGo/eb-gorm"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

func main() {
	//ToDo add proper create table syntax to run the program smoothly
	mysqlDataSource := "root:@tcp(127.0.0.1:3306)/demo?charset=utf8mb4&parseTime=True&loc=Local"
	mysqldb, err := gorm.Open(mysql.Open(mysqlDataSource), &gorm.Config{})
	if err != nil {
		log.Fatal("[SQL DB] Gorm MySQL DB Connection Open failed ", mysqlDataSource)
	}
	loadDataIntoMySqlAppLogsTable(mysqldb)

	var params []string
	appLogs := AppLogs{}
	gormRecordReader := eb_gorm.NewGormRecordReader(mysqldb, "select * from app_logs", params, &appLogs)
	mysqlJob := core.NewJobBuilder().
		Name("MySQL Database Read Job").
		Reader(gormRecordReader).
		Writer(writer.NewStandardOutputRecordWriter()).
		Build()

	report := mysqlJob.Call()
	fmt.Println("Metrics Read Count : ", report.Metrics.ReadCount)

	fmt.Println("#############################################")
	//Postgresql Job
	pgsqlDataSource := "host=localhost dbname=postgres port=5432 sslmode=disable"
	pgdb, err := gorm.Open(postgres.Open(pgsqlDataSource), &gorm.Config{})
	if err != nil {
		log.Fatal("[SQL DB] Gorm PgSQL DB Connection Open failed ", pgsqlDataSource)
	}
	loadDataIntoPostgresUsersTable(pgdb)

	recordReader := eb_gorm.NewGormRecordReader(pgdb, "select * from users", params, &User{})
	pgsqlJob := core.NewJobBuilder().
		Name("Postgresql Database Reader Job").
		Reader(recordReader).
		//Filter(NewAgeFilter("age", ">", 20)).
		Writer(writer.NewStandardOutputRecordWriter()).
		Build()

	pgJobReport := pgsqlJob.Call()

	fmt.Println("[PG] Read Count : ", pgJobReport.Metrics.ReadCount)
	fmt.Println("[PG] Write Count : ", pgJobReport.Metrics.WriteCount)

	//Checking type of payload.
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

func loadDataIntoMySqlAppLogsTable(mysqldb *gorm.DB) {
	mysqldb.AutoMigrate(&AppLogs{})

	app1 := &AppLogs{
		AppName: "Nginx App",
		Log:     "All Clients are Hitting me",
		Model:   gorm.Model{},
	}
	app2 := &AppLogs{
		AppName: "ServiceDiscovery App",
		Log:     "Helping Discover Endpoints",
		Model:   gorm.Model{},
	}
	app3 := &AppLogs{
		AppName: "Feature App",
		Log:     "All Business Logic Logs",
		Model:   gorm.Model{},
	}

	input := make([]*AppLogs, 3)
	input[0] = app1
	input[1] = app2
	input[2] = app3

	mysqldb.Exec("Truncate Table app_logs")
	mysqldb.Create(input)
}

func loadDataIntoPostgresUsersTable(pgdb *gorm.DB) {
	pgdb.AutoMigrate(&User{})

	sunand := &User{
		Name:  "sunand",
		Age:   30,
		Email: "samosachat at the rate gmail dot com",
		Phone: "9876543210",
	}
	noodles := &User{
		Name:  "noodles",
		Age:   30,
		Email: "noodles at the rate gmail dot com",
		Phone: "7890123456",
	}
	appu := &User{
		Name:  "appu",
		Age:   3,
		Email: "appu at the rate gmail dot com",
		Phone: "0123456789",
	}

	input := make([]*User, 3)
	input[0] = sunand
	input[1] = noodles
	input[2] = appu

	pgdb.Exec("Truncate Table Users")
	pgdb.Create(input)
}

//============= Gorm Model's =================
type AppLogs struct {
	AppName string
	Log     string
	gorm.Model
}

type User struct {
	Name  string
	Age   uint
	Email string
	Phone string
	gorm.Model
}

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
