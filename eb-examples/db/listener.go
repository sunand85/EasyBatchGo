package main

import "gorm.io/gorm"

type PgWriteListener struct {
	Db         *gorm.DB
	TargetType interface{}
}

func NewPgWriteListener(db *gorm.DB, targetType interface{}) *PgWriteListener {
	return &PgWriteListener{Db: db, TargetType: targetType}
}

func (p *PgWriteListener) BeforeRecordWriting() {
	p.Db.AutoMigrate(p.TargetType) //Creates the table based on the model definition
	p.Db.Exec("Truncate table tweets")
}

func (p *PgWriteListener) AfterRecordWriting() {
	//No OP
}
