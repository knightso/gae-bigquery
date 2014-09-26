package gaebigquery

import (
	"fmt"

	"appengine"

	"code.google.com/p/goauth2/appengine/serviceaccount"
	"code.google.com/p/google-api-go-client/bigquery/v2"

	"constants"
	"model"
)

const (
	BIGQUERY_SCOPE string = bigquery.BigqueryScope
	PROJECT_ID     string = constants.PROJECT_ID
)

func NewService(c appengine.Context) *Service {
	s := &Service{}
	s.TableData = &TabledataService{s: s}
	s.Tables = &TablesService{s: s}

	if appengine.IsDevAppServer() {
		err := setServiceAccountForDev(c, s)
		if err != nil {
			panic(fmt.Sprintf("%s", err.Error()))
		}
		return s
	}

	client, err := serviceaccount.NewClient(c, BIGQUERY_SCOPE)
	if err != nil {
		panic(fmt.Sprintf("%s", err.Error()))
	}
	service, err := bigquery.New(client)
	if err != nil {
		panic(fmt.Sprintf("%s", err.Error()))
	}
	s.bq = service
	return s
}

type Service struct {
	bq        *bigquery.Service
	TableData *TabledataService
	Tables    *TablesService
}

type TabledataService struct {
	s *Service
}

func (t *TabledataService) InsertAll(datasetID, tableID string, tasks []*model.Task) (*bigquery.TableDataInsertAllResponse, error) {
	data := make([]*bigquery.TableDataInsertAllRequestRows, len(tasks))
	for i, task := range tasks {
		data[i] = &bigquery.TableDataInsertAllRequestRows{
			InsertId: task.InsertID,
			Json:     task.Record,
		}
	}

	response, err := t.s.bq.Tabledata.InsertAll(PROJECT_ID, datasetID, tableID, &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: data,
	}).Do()
	if err != nil {
		return nil, err
	}
	return response, nil
}

type TablesService struct {
	s *Service
}

func (t *TablesService) Insert(datasetID, tableID string) (*bigquery.Table, error) {
	table := &bigquery.Table{
		TableReference: &bigquery.TableReference{
			DatasetId: datasetID,
			ProjectId: PROJECT_ID,
			TableId:   tableID,
		},
	}

	newTable, err := t.s.bq.Tables.Insert(PROJECT_ID, datasetID, table).Do()
	if err != nil {
		return nil, err
	}
	return newTable, nil
}

type TableNotFoundException struct {
	s string
}

func (t *TableNotFoundException) Error() string {
	return t.s
}
