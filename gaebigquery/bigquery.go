package gaebigquery

import (
	"appengine"

	"code.google.com/p/goauth2/appengine/serviceaccount"
	"code.google.com/p/google-api-go-client/bigquery/v2"

	"model"
)

func newBigQueryService(c appengine.Context) (BigQueryService, error) {
	service := BigQueryService{}
	service.Tabledata = &TabledataService{s: &service}

	err := setServiceAccount(c, &service)
	if err != nil {
		return BigQueryService{}, err
	}
	return service, nil
}

func setServiceAccount(c appengine.Context, s *BigQueryService) error {
	if appengine.IsDevAppServer() {
		err := setServiceAccountForDev(c, s)
		if err != nil {
			return err
		}
		return nil
	}
	client, err := serviceaccount.NewClient(c, bigquery.BigqueryScope)
	if err != nil {
		return err
	}
	service, err := bigquery.New(client)
	if err != nil {
		return err
	}
	s.service = service

	return nil
}

type BigQueryService struct {
	service   *bigquery.Service
	Tabledata *TabledataService
}

type TabledataService struct {
	s *BigQueryService
}

func (t *TabledataService) InsertAll(datasetID, tableID string, tasks []*model.Task) (*bigquery.TableDataInsertAllResponse, error) {
	data := make([]*bigquery.TableDataInsertAllRequestRows, len(tasks))
	for i, task := range tasks {
		data[i] = &bigquery.TableDataInsertAllRequestRows{
			InsertId: task.InsertID,
			Json:     task.Record,
		}
	}

	response, err := t.s.service.Tabledata.InsertAll(projectID, datasetID, tableID, &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: data,
	}).Do()
	if err != nil {
		return nil, err
	}
	return response, nil
}
