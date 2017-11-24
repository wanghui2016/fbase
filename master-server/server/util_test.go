package server

import (
	"testing"
	"time"
)

func TestSQLParse(t *testing.T) {
	var sql = `CREATE TABLE Persons(Id_P int NOT NULL,
LastName varchar(255) NULL,
FirstName varchar(255) DEFAULT 'Sandnes',
Address varchar(255),
City varchar(255),
UNIQUE (LastName),
PRIMARY KEY (Id_P));`
	table, err := SqlParse(sql)
	if err != nil {
		t.Error("test failed")
		time.Sleep(time.Second)
		return
	}
	t.Logf("table %v", table)
}
