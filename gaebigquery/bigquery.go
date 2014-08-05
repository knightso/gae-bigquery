package gaebigquery

import (
	"appengine"
	"code.google.com/p/goauth2/appengine/serviceaccount"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"net/http"

	"model"

	// devAppServer用import
	"appengine/memcache"
	"appengine/urlfetch"
	"code.google.com/p/goauth2/oauth"
	"encoding/json"
)

/* ここから devAppServer用コード */
// 開発サーバーでは、一度 /_admin/gapi_auth　にアクセスして認証を行ってください。
// memcacheにトークンが残っている限り継続して使用出来ます。

const (
	tokenCacheKey = "gaebigquerytoken"
)

var config = &oauth.Config{
	ClientId:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com",
	ClientSecret: "xxxxxxxxxxxxxxxxx",
	Scope:        bigquery.BigqueryScope,
	RedirectURL:  "http://localhost:8081/_admin/set_token",
	AuthURL:      "https://accounts.google.com/o/oauth2/auth",
	TokenURL:     "https://accounts.google.com/o/oauth2/token",
}

func init() {
	http.HandleFunc("/_admin/gapi_auth", AuthHandler)
	http.HandleFunc("/_admin/set_token", SetTokenHandler)
}

func AuthHandler(rw http.ResponseWriter, req *http.Request) {
	url := config.AuthCodeURL("")
	http.Redirect(rw, req, url, http.StatusFound)
}

func SetTokenHandler(rw http.ResponseWriter, req *http.Request) {
	c := appengine.NewContext(req)
	authorizationCode := req.FormValue("code")

	transport := &oauth.Transport{
		Config: config,
		Transport: &urlfetch.Transport{
			Context: c,
		},
	}
	token, err := transport.Exchange(authorizationCode)
	if err != nil {
		fmt.Fprintf(rw, "%s", err.Error())
		return
	}

	// トークン情報の一時保存
	value, err := json.Marshal(token)
	if err != nil {
		fmt.Fprintf(rw, "%s", err.Error())
		return
	}
	item := &memcache.Item{
		Key:   tokenCacheKey,
		Value: value,
	}
	err = memcache.Add(c, item)
	if err != nil {
		fmt.Fprintf(rw, "%s", err.Error())
		return
	}

	fmt.Fprintf(rw, "Success.")
}

/* ここまで devAppServer用コード */

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
		transport := &oauth.Transport{
			Config: config,
			Transport: &urlfetch.Transport{
				Context: c,
			},
		}
		// cacheがあるか？
		item, err := memcache.Get(c, tokenCacheKey)
		if err == memcache.ErrCacheMiss {
			return fmt.Errorf("Visit this URL to get a code: http://localhost:8081/_admin/gapi_auth")
		} else if err != nil {
			return err
		}
		// トークンの読み込み
		token := &oauth.Token{}
		err = json.Unmarshal(item.Value, token)
		if err != nil {
			return err
		}
		// トークンの更新
		transport.Token = token
		err = transport.Refresh()
		if err != nil {
			return err
		}

		client := transport.Client()
		service, err := bigquery.New(client)
		if err != nil {
			return err
		}
		s.service = service
	} else {
		client, err := serviceaccount.NewClient(c, bigquery.BigqueryScope)
		if err != nil {
			return err
		}
		service, err := bigquery.New(client)
		if err != nil {
			return err
		}
		s.service = service
	}
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
