package gaebigquery

import (
	"fmt"
	"strings"
)

var bqDefine = make(map[string]bigQueryDefine)

type bigQueryDefine struct {
	DatasetID string
	TableID   string
	Schema    map[string]string
}

func SetSchema(logID, datasetID, tableID, schema string) {
	bqDefine[logID] = bigQueryDefine{
		DatasetID: datasetID,
		TableID:   tableID,
		Schema:    parseSchema(schema),
	}
}

func parseSchema(schema string) map[string]string {
	// コンマ(,)でschemaを分割します。
	separete := strings.FieldsFunc(schema, func(r rune) bool {
		return strings.ContainsRune(",", r)
	})

	schemaMap := make(map[string]string)
	for _, column := range separete {
		// コロン(:)で分割します。
		labelAndType := strings.FieldsFunc(column, func(r rune) bool {
			return strings.ContainsRune(":", r)
		})

		// label、typeの有無を確認します。
		if len(labelAndType) != 2 {
			panic(fmt.Errorf("Invalid schema: %s\n ex. \"column1_name:data_type,column2_name:data_type,...\"", schema))
		}

		label := labelAndType[0]
		labelType := labelAndType[1]

		// BigQueryで有効な型か確認します。
		switch strings.ToUpper(labelType) {
		case "STRING":
		case "INTEGER":
		case "FLOAT":
		case "BOOLEAN":
		case "TIMESTAMP":
		case "RECORD":
		default:
			panic(fmt.Errorf("Invalid Type: %s\nValid type: STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, RECORD", labelType))
		}

		schemaMap[label] = strings.ToUpper(labelType)
	}

	return schemaMap
}
