package bq

import (
	"encoding/json"
	"fmt"
	"net/http"
	//"time"

	"appengine"
	"appengine/taskqueue"

	"constants"
	"gaebigquery"
	"model"
)

var isShuttingDown bool

const (
	PROJECT_ID string = constants.PROJECT_ID
	QUEUE_NAME string = constants.QUEUE_NAME
	TASK_COUNT int    = constants.TASK_COUNT
	LEASE_TIME int    = constants.LEASE_TIME

	YYYYMMDD string = constants.YYYYMMDD
)

func init() {
	http.HandleFunc("/_ah/stop", StopHandler)
	http.HandleFunc("/cron/insert", StreamingInsertHandler)
}

// 本ハンドラはbackendインスタンスがシャットダウンする30秒前に呼び出されます。
// 参照：https://developers.google.com/appengine/docs/go/modules/　の　Scaling Types参照。
func StopHandler(rw http.ResponseWriter, req *http.Request) {
	isShuttingDown = true
}

func StreamingInsertHandler(rw http.ResponseWriter, req *http.Request) {
	c := appengine.NewContext(req)

	// ログの種類一覧を取得する
	logInfos := model.GetBigQueries()
	for _, logInfo := range logInfos {
		//key := model.NewBigQueryInfoKey(c, logInfo)

		for {
			if isShuttingDown {
				c.Warningf("The instance is shutting down for some reason.")
				return
			}

			tasks, err := taskqueue.LeaseByTag(c, TASK_COUNT, QUEUE_NAME, LEASE_TIME, logInfo.LogID)
			if err != nil {
				c.Errorf("%s", err.Error())
				return
			}

			taskSize := len(tasks)
			if taskSize > 0 {
				handlesMap := map[string][]*taskqueue.Task{}
				for _, task := range tasks {
					params := unmarshalPayload(task)

					registerDate := params.Date
					// TODO: LocationをAsia/Tokyoにズラした時刻を取得する方法が分からない。
					registerYmd := registerDate.Format(YYYYMMDD)

					handles, ok := handlesMap[registerYmd]
					if !ok {
						handles = []*taskqueue.Task{}
						handlesMap[registerYmd] = handles
					}
					handlesMap[registerYmd] = append(handlesMap[registerYmd], task)

					// debug. ok.
					c.Infof("logID: %s task count: %d", logInfo.LogID, len(handlesMap[registerYmd]))
				}

				// 362行目相当
				for registerYmd, handles := range handlesMap {
					// TODO: 下記テーブルのローテートは、ローテート無しでBigQueryへのInsert確認後に実装。
					/*
						date := time.Parse(constants.YYYYMMDD, registerYmd)
						slashedYmd := date.Format(constants.DATE_FORMAT_SLASHED)

						if この日付のテーブルが有るか {
							BigQueryに新しくテーブルを作成
							// DatastoreUtilityがない・・・後で池田さんに聞こう。
						}
					*/

					// InsertAll用のデータ構築と、insertIDの有無を確認してpanic処理
					rows := make([]*model.Task, len(handles))
					for i, taskHandle := range handles {
						rows[i] = unmarshalPayload(taskHandle)

						if len(rows[i].InsertID) == 0 {
							panic(fmt.Sprintln("log key not found."))
						}
					}

					c.Debugf("inserting! logID:%s, ymd:%s, rows:%d", logInfo.LogID, registerYmd, len(rows))

					service := gaebigquery.NewService(c)
					res, err := service.TableData.InsertAll(logInfo.DatasetID, logInfo.TableID, rows)
					// insertAllでエラーが出たら、次のループへ。(ここはJavaと異なる)
					if err != nil {
						c.Errorf("%s", err.Error())
						for _, ft := range handles {
							c.Debugf("modify task lease...")
							taskqueue.ModifyLease(c, ft, QUEUE_NAME, 5)
						}
						break
					}

					failures := make([]*taskqueue.Task, 0)
					if res.InsertErrors != nil && len(res.InsertErrors) > 0 {
						for _, insertErrors := range res.InsertErrors {
							// TableDataInsertAllResponseInsertErrors.Index は int64です。
							// そのためzero valueの場合と、値が0である場合を区別できません。
							// よってErrorsの有無で判別することにしました。
							if insertErrors.Errors == nil {
								continue
							}
							for _, errProto := range insertErrors.Errors {
								errJson, err := json.Marshal(errProto)
								if err != nil {
									c.Warningf("json error: %s", err.Error())
								} else {
									c.Debugf("%s", string(errJson))
								}
							}
							failures = append(failures, handles[int(insertErrors.Index)])
						}
					}

					succeeds := make([]*taskqueue.Task, 0)
					for _, taskHandle := range handles {
						ifIn := func() bool {
							for _, f := range failures {
								if taskHandle == f {
									return true
								}
							}
							return false
						}()
						if ifIn == false {
							succeeds = append(succeeds, taskHandle)
						}
					}

					c.Debugf("succeeds:%d", len(succeeds))
					c.Debugf("failures:%d", len(failures))

					if len(succeeds) > 0 {
						c.Debugf("delete tasks...")
						err = taskqueue.DeleteMulti(c, succeeds, QUEUE_NAME)
						if err != nil {
							multiError, ok := err.(appengine.MultiError)
							if ok {
								for _, err = range multiError {
									c.Errorf("%s", err.Error())
								}
							} else {
								c.Errorf("%s", err.Error())
							}
						}
					}

					for _, ft := range failures {
						c.Debugf("modify task lease...")
						taskqueue.ModifyLease(c, ft, QUEUE_NAME, 5)
					}
				}
			}

			// if BigQueryのテーブル作成でエラー吐いた {
			//		break
			// }
			if len(tasks) < TASK_COUNT {
				break
			}
		}
	}
	/***
	  for(ログの種類) {
	  	cron自体のログの記録を取るエンティティを取得	// 必要？

	  	datastoreに保存してあるログの種類のBigQuery定義を取得する。

	  	キューからタスクを取得
	  	for {
	  		if isShuttingDown {	// backendインスタンスが落ちる30秒前か否か
					map[string]string{"result":"shutdown"}を返す // どこに返してんの？
	  		}

	  		taskをleaseする。

				if taskが有る {
					handlesMap := map[string][]*taskqueue.Task	// keyは何だ？ → 日付のようだ。
					// illegalTasks関連の処理は今回要らないので除いてます。
					for リースしたtask {
						タスクのパラメータを取得

						タスクのpayloadから、タスクの登録日時を取得
						取得した日時を基に、フォーマットとタイムゾーンを指定してDate型変数に変換

						Date型の日時を文字列に変換
						handles := handlesMap[日時文字列]
						if handlesが無かった(nil) {
							handlesMap[日時文字列] = make([]*taskqueue.Task)
						}
						handlesMap[日時文字列] = append(handlesMap[日時文字列], task)
					}

					for 登録日時, taskのslice := range handlesMap {	// 362行目
						handlesMapから日付ごとのTaskを取得
						年/月/日のフォーマットに文字列変更

						// TODO: 下記テーブルのローテートは、ローテート無しでBigQueryへのInsert確認後に実装。
						if この日付のテーブルが有るか {
							BigQueryに新しくテーブルを作成
							// DatastoreUtilityがない・・・後で池田さんに聞こう。
						}

						インサートIDを入れるスライス変数
						行を入れるスライス変数
						for 個別タスク := range 日付ごとのTask {
							taskのパラメータを取得（payloadのこと）
							パラメータをスキーマに合わせたmapに変換
							行を格納するスライスに追加

							パラメータmapからInsertIDを取得
							if InsertIDがnull {
								例外を投げる(GoではpanicでもOK)
							}
							インサートIDのスライスに追加
						}

						ログ出力

						InsertAllの実行	// TODO: bigquery apiのエラー処理を細かにする
						失敗したtask一覧を入れるスライス
						if エラーがあれば {
							for エラー一覧 {
								if エラーのインデックスがnull {
									continue
								}
								for エラーの中身 {
									エラーの中身をログに出力
								}
								エラーのインデックスを基に、失敗したタスクを取得して、失敗したタスク一覧に追加
							}
						}

						成功したtask一覧用変数
						for 日付ごとのTask {
							if 失敗したタスク一覧に入っていない {
								成功したタスク一覧に追加する
							}
						}

						成功したタスク数をログ出力
						失敗したタスク数をログ出力

						if 成功したタスク数 > 0 {
							ログにタスクを削除する旨を出力
							成功したタスクをキューから削除
						}

						for 失敗したタスク一覧 {
							失敗したタスクはキューに戻す旨をログに出力
							失敗したタスクをキューに戻す
						}
					}
				}
				if タスクの数が500件以下 {
					ログに次へと出力
					break
				}
	  	}
	  }
	  ***/
}

func unmarshalPayload(t *taskqueue.Task) *model.Task {
	v := model.Task{}
	err := json.Unmarshal(t.Payload, &v)
	if err != nil {
		panic(fmt.Sprintf("%s", err.Error()))
	}
	return &v
}
