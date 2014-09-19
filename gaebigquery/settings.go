package gaebigquery

const (
	projectID string = "metal-bus-589"
	queueName string = "gaebigquery"
)

func init() {
	//logConfig.Add("TestLog_A", "log_test", "table_a", "kind:string,name:string,count:integer")
	SetSchema("TestLog_A", "log_test", "table_a", "kind:string,name:string,count:integer")
}

// dev server config is "dev.go"
