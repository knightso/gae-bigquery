package gaebigquery

import (
	"fmt"
	"strings"
)

type BigQueryDefine struct {
	DatasetID string
	TableID   string
	Schema    map[string]string
}

type LogConfig map[string]BigQueryDefine // keyはLogID

var logConfig = LogConfig{}

// logIDはユーザーが定義したログの種類を表す識別子です。
func (l *LogConfig) Add(logID, datasetID, tableID, schema string) {
	bqDef := BigQueryDefine{
		DatasetID: datasetID,
		TableID:   tableID,
		Schema:    l.parseSchema(schema),
	}
	(*l)[logID] = bqDef
}

func (l *LogConfig) parseSchema(schema string) map[string]string {
	// コンマ(,)でschemaを分割します。
	col_schemata := strings.FieldsFunc(schema, func(r rune) bool {
		return strings.ContainsRune(",", r)
	})

	schemaMap := make(map[string]string)
	for _, col_schema := range col_schemata {
		// コロン(:)で分割します。
		nameAndType := strings.FieldsFunc(col_schema, func(r rune) bool {
			return strings.ContainsRune(":", r)
		})

		// column名、type名の有無を確認します。
		if len(nameAndType) != 2 {
			panic(fmt.Errorf("Invalid schema: %s\n ex. \"column1_name:data_type,column2_name:data_type,...\"", schema))
		}

		// 許可されたtypeか確認します。
		switch strings.ToUpper(nameAndType[1]) {
		case "STRING":
		case "INTEGER":
		case "FLOAT":
		case "BOOLEAN":
		case "TIMESTAMP":
		case "RECORD":
		default:
			panic(fmt.Errorf("Invalid Type: %s\nValid type: STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, RECORD", nameAndType[1]))
		}

		schemaMap[nameAndType[0]] = strings.ToUpper(nameAndType[1])
	}

	return schemaMap
}
