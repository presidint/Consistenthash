package main

import (
	"database/sql"
	"fmt"
	"sync"
	. "utils"

	"net/http"

	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/valyala/fasthttp"
)

const (
	DEFAULT_HTTP_IP   = "0.0.0.0"
	DEFAULT_HTTP_PORT = "18090"
)

var http_once sync.Once
var http_live_once sync.Once
var http_vod_once sync.Once

var g_http_mgmt *HttpMgmt

type HttpMgmt struct {
	mgmt_live *HttpLiveMgmt
	mgmt_vod  *HttpVodMgmt
}

type HttpLiveMgmt struct {
	db               *sql.DB
	delay_segs       int
	pull_server_addr string
	client           *http.Client
	active           bool
}

type HttpVodMgmt struct {
	active bool
}

func GetHttpMgmtInstance() *HttpMgmt {
	http_once.Do(func() {
		g_http_mgmt = &HttpMgmt{
			mgmt_live: &HttpLiveMgmt{},
			mgmt_vod:  &HttpVodMgmt{},
		}
	})
	return g_http_mgmt
}

func (this *HttpMgmt) Start() error {
	var err error

	router, err := InitHttpRouter()
	if err != nil {
		return fmt.Errorf("Init http router failed. [%s]\n", err.Error())
	}

	ip := g_cf.DefaultString("httpip", DEFAULT_HTTP_IP)
	port := g_cf.DefaultString("httpport", DEFAULT_HTTP_PORT)
	listen_addr := ip + ":" + port

	err = fasthttp.ListenAndServe(listen_addr, router.Handler)
	if err != nil {
		return fmt.Errorf("Listen %s failed. [%s]", listen_addr, err.Error())
	}
	return nil
}

func (this *HttpLiveMgmt) Init() error {
	http_live_once.Do(func() {
		var err error
		mysql := g_cf.String("mysql_segs")
		this.db, err = sql.Open("mysql", mysql)
		if err != nil {
			VLOG(VLOG_ERROR, "%s", err.Error())
		}
		this.delay_segs = g_cf.DefaultInt("Delay::delay_slices", 3)
		if this.delay_segs < 3 {
			this.delay_segs = 3
		}
		//this.db.SetMaxIdleConns()
		//this.db.SetMaxOpenConns()
		this.pull_server_addr = g_cf.DefaultString("PULL_STREAM_SERVER::addr", "127.0.0.1")
		this.client = &http.Client{
			Transport:     nil,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       time.Second * 3,
		}
		this.active = g_cf.DefaultBool("live_active", false)
	})
	return nil
}

func (this *HttpVodMgmt) Init() error {
	this.active = g_cf.DefaultBool("vod_active", false)
	return nil
}
