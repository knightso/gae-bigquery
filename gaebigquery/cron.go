package gaebigquery

import (
	"appengine"
	"appengine/taskqueue"
	"encoding/json"
	"net/http"

	"model"
)

const (
	maxTasks  int = 500
	leaseTime int = 60 * 5
)

const (
	// ユーザー編集箇所。
	projectID string = "metal-bus-589"
	queueName string = "gaebigquery"
)

func init() {
	logConfig.Add("TestLog_A", "log_test", "table_a", "kind:string,name:string,count:integer")
	//logConfig.Add("TestLog_B", "dataset_test", "table_b", "kind:string,date:timestamp,count:integer")

	http.HandleFunc("/schedule", ScheduleHandler)
}

func ScheduleHandler(rw http.ResponseWriter, req *http.Request) {
	c := appengine.NewContext(req)

	tasks, err := taskqueue.Lease(c, maxTasks, queueName, leaseTime)
	if err != nil {
		c.Errorf("%s", err.Error())
		return
	}
	if len(tasks) == 0 {
		c.Infof("%s queue is empty.", queueName)
		return
	}

	// TaskをLogIDで分けます。
	eachTable, err := classfyTask(tasks)
	if err != nil {
		c.Errorf("%s", err.Error())
		return
	}

	service, err := newBigQueryService(c)
	if err != nil {
		c.Errorf("%s", err.Error())
		return
	}

	// LogIDごとにBigQueryに書き込みます。
	for logID, tasks := range eachTable {
		def := logConfig[logID]
		_, err = service.Tabledata.InsertAll(def.DatasetID, def.TableID, tasks)
		if err != nil {
			c.Errorf("%s", err.Error())
			return
		}
	}

	// 書き込んだログをtaskqueueから削除します。
	err = taskqueue.DeleteMulti(c, tasks, queueName)
	if err != nil {
		c.Errorf("%s", err.Error())
		return
	}

	c.Infof("Schedule Task is success.")
}

// LogIDごとに分類して返します。
func classfyTask(tasks []*taskqueue.Task) (map[string][]*model.Task, error) {
	logIDs := make(map[string][]*model.Task)
	for _, task := range tasks {
		data := model.Task{}
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return nil, err
		}

		logIDs[data.LogID] = append(logIDs[data.LogID], &data)
	}
	return logIDs, nil
}
