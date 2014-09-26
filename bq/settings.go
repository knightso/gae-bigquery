package bq

import (
	"model"
)

func init() {
	model.SetBigQuerySchema("1", "log_test", "table_a", "kind:string,name:string,count:integer")
	model.SetBigQuerySchema("2", "log_test", "table_b", "kind:string,name:string,count:integer")
}
