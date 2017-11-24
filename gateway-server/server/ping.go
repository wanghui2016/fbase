package server

import (
	"net/http"
	"time"
	"net/url"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"errors"
	"crypto/md5"

	"util/log"
	"model/pkg/metapb"
)

var (
	routePingUrl              = "/route/get"
	schemaPingUrl             = "/meta/get"

	Date_Format_Pattern       = "2006-01-02 15:04:05"
)

type routeReply struct {
	Code int `json:"code"`
	Message string `json:"message`
	Data []*metapb.Route `json:"data"`
}

type schemaReply struct {
	Code int `json:"code"`
	Message string `json:"message`
	Data []*metapb.Table `json:"data"`
}

type SchemaPing struct {
    signKey string
    cli     *http.Client
    addrs   []string
    leader  string
}

func NewSchemaPing(cli *http.Client, addrs []string, signKey string) *SchemaPing {
	return &SchemaPing{
		signKey: signKey,
		cli: cli,
		addrs: addrs,
		// default leader
		leader: addrs[0],
	}
}

func (sch *SchemaPing) ping(version string) ([]*metapb.Table, string, error){
	retry:
	_url := "http://" + sch.leader +  schemaPingUrl
	getUrl, _ := url.Parse(_url)
	q := getUrl.Query()
	reqDate := time.Now().Format(Date_Format_Pattern)
	source := sch.signKey + reqDate
	s := fmt.Sprintf("%x", md5.Sum([]byte(source)))
	q.Set("d", reqDate)
	q.Set("s", s)
	q.Set("version", version)
	getUrl.RawQuery = q.Encode()

	resp, err := sch.cli.Get(getUrl.String())
	if err != nil {
		log.Error("[schema ping] Get error, [%s]", err.Error())
		time.Sleep(time.Second * 30)
		return nil, "", err
	}
	if resp == nil {
		log.Error("ping error, retry!!!!!!!")
		goto retry
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("[schema ping] Get reponse status not ok: [%d]", resp.StatusCode)
		time.Sleep(time.Second * 30)
		return nil, "", err
	}
	if resp.Body == nil {
		log.Error("empty ping result, retry!!!!!!!")
		goto retry
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("[schema ping] ReadAll error, [%s]", err.Error())
		return nil, "", err
	}
	log.Debug("===schema result=%s",string(body))
	result := &schemaReply{}
	err = json.Unmarshal(body, result)
	if err != nil {
		log.Error("[route ping] Unmarshal error, [%s]", err.Error())
		return nil, "", err
	}
	if result.Code == 0 {
		// TODO update schema
		return result.Data, result.Message, nil
	}
	if result.Code == -1 {
		// TODO change leader
		log.Debug("ms leader changed, we need retry")
		goto retry
	} else {
		// TODO other error
		return nil, "", errors.New(result.Message)
	}
}

type RoutePing struct {
	db      string
	table   string
	signKey string
	cli     *http.Client
    addrs   []string
	leader  string
}

func NewRoutePing(db, table string, cli *http.Client, addrs []string, signKey string) *RoutePing {
	return &RoutePing{
		db: db,
		table: table,
		signKey: signKey,
		cli: cli,
		addrs: addrs,
		// default leader
		leader: addrs[0],
	}
}

func (t *RoutePing) ping(version string) ([]*metapb.Route, string, error) {
	retry:
	_url := "http://" + t.leader + routePingUrl
	getUrl, _ := url.Parse(_url)
	q := getUrl.Query()
	reqDate := time.Now().Format(Date_Format_Pattern)
	source := t.signKey + reqDate
	s := fmt.Sprintf("%x", md5.Sum([]byte(source)))
	q.Set("d", reqDate)
	q.Set("s", s)
	q.Set("version", version)
    q.Set("dbname", t.db)
	q.Set("tablename", t.table)
	getUrl.RawQuery = q.Encode()
    start := time.Now()
	resp, err := t.cli.Get(getUrl.String())
	if err != nil {
		log.Error("[route ping] Get error, [%s]", err.Error())
		if time.Now().Sub(start) < time.Second * 30 {
			time.Sleep(time.Second * 30)
		}
		return nil, version, nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Error("[route ping] Get reponse status not ok: [%d]", resp.StatusCode)
		time.Sleep(time.Second * 30)
		return nil, "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("[route ping] ReadAll error, [%s]", err.Error())
		return nil, "", err
	}
	log.Info("===pingUrl result=%s",string(body))
	result := &routeReply{}
	err = json.Unmarshal(body, result)
	if err != nil {
		log.Error("[route ping] Unmarshal error, [%s]", err.Error())
		return nil, "", err
	}
	if result.Code == 0 {
		// TODO update route
		return result.Data, result.Message, nil
	}
	if result.Code == -1 {
		// TODO change leader
		goto retry
	} else {
		// TODO other error
		return nil, "", errors.New("unknown error")
	}
}

