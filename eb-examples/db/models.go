package main

import "gorm.io/gorm"

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

type Tweet struct {
	Id     string
	Handle string
	Tweet  string
}
