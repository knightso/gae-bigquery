package gaebigquery

import (
	"encoding/json"
	"fmt"
	"net/http"

	"appengine"
	"appengine/memcache"
	"appengine/urlfetch"

	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/google-api-go-client/bigquery/v2"
)

const (
	tokenCacheKey = "gaebigquerytoken"
)

var config = &oauth.Config{
	ClientId:     "961460936936-ked3bqh8sa80onmr1t0cs1k95h999na6.apps.googleusercontent.com",
	ClientSecret: "Jx0_0355D681HSv_29FXTwJN",
	Scope:        bigquery.BigqueryScope,
	RedirectURL:  "http://localhost:8081/_admin/set_token",
	AuthURL:      "https://accounts.google.com/o/oauth2/auth",
	TokenURL:     "https://accounts.google.com/o/oauth2/token",
}

// 開発サーバーでは、一度 /_admin/gapi_auth　にアクセスして認証を行ってください。
// memcacheにトークンが残っている限り継続して使用可能です。

func init() {
	if appengine.IsDevAppServer() {
		http.HandleFunc("/_admin/gapi_auth", AuthHandler)
		http.HandleFunc("/_admin/set_token", SetTokenHandler)
	}
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

func setServiceAccountForDev(c appengine.Context, s *BigQueryService) error {
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
	return nil
}
