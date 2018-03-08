package main

import (
	"github.com/billhathaway/consistentHash"
	"fmt"
	"net/http"
	"strings"
	"sync"
	. "utils"

	"bytes"
	"io"

	"github.com/buaazp/fasthttprouter"
	"github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

var vslb_once sync.Once
var vslb_mgmt *VslbMgmt

const (
	DEFAULT_STATISTIC_IP   = "0.0.0.0"
	DEFAULT_STATISTIC_PORT = "15222"
)

type VslbMgmt struct {
	HashList *consistentHash.ConsistentHash
	NodeMap  map[string]*VslbNode
	client   *http.Client
}

func GetVslbMgmtInstance() *VslbMgmt {
	vslb_once.Do(func() {
		vslb_mgmt = &VslbMgmt{
			HashList: consistentHash.New(),
			NodeMap:  make(map[string]*VslbNode),
			client:   &http.Client{},
		}
		vslb_mgmt.init()
	})
	return vslb_mgmt
}

func (this *VslbMgmt) init() error {

	var i int = 1
	var key string
	for {
		key = fmt.Sprintf("Node%d::private_addr", i)
		private_addr := g_cf.String(key)
		if private_addr == "" {
			break
		}
		key = fmt.Sprintf("Node%d::public_addr", i)
		public_addr := g_cf.DefaultString(key, private_addr)

		key = fmt.Sprintf("Node%d::heartbeat", i)
		hb := g_cf.String(key)

		key = fmt.Sprintf("Node%d::heartbeat_interval", i)
		hbi := g_cf.DefaultInt(key, DEFAULT_HEARTBEAT_INTERVAL)
		if hbi < 3 {
			hbi = 3
		}

		this.NodeMap[private_addr] = &VslbNode{
			IsLive:            false,
			Status:            &NginxStatus{},
			PrivateAddr:       private_addr,
			PublicAddr:        public_addr,
			HeartBeatUrl:      hb,
			HeartBeatInterval: hbi,
		}
		VLOG(VLOG_ALTER, "Find Node [%s]", private_addr)
		i++
	}
	return nil
}

func (this *VslbMgmt) Start() error {
	for _, v := range this.NodeMap {
		go v.Start()
	}
	return nil
}

func (this *VslbMgmt) statisticServer() {
	router := &fasthttprouter.Router{
		RedirectTrailingSlash:  false,
		RedirectFixedPath:      false,
		HandleMethodNotAllowed: false,
		HandleOPTIONS:          false,
		NotFound:               nil,
		MethodNotAllowed:       nil,
		PanicHandler:           nil,
	}
	router.GET("/status", RouteStatus)
	router.GET("/status/*filepath", RouteStatus)

	ip := g_cf.DefaultString("Statistic:listen_ip", DEFAULT_STATISTIC_IP)
	port := g_cf.DefaultString("Statistic:listen_port", DEFAULT_STATISTIC_PORT)
	listen_addr := ip + ":" + port

	err := fasthttp.ListenAndServe(listen_addr, router.Handler)
	if err != nil {
		VLOG(VLOG_ERROR, "Listen %s failed. [%s]", listen_addr, err.Error())
		return
	}
}

func RouteStatus(ctx *fasthttp.RequestCtx) {
	ctx.SetConnectionClose()

	mgmt := GetVslbMgmtInstance()
	sum := &struct {
		Active    int `json:"active"`
		BandWidth int `json:"bandwidth"`
	}{}

	for pri_addr, node := range mgmt.NodeMap {
		if node.Status == nil {
			continue
		}
		tmp := strings.Split(pri_addr, ":")
		pri_ip := tmp[0]

		for _, st := range node.Status.Zone {
			if strings.EqualFold(st.Key, pri_ip) {
				//同一个IP
				sum.Active += st.Active
				sum.BandWidth += st.BandWidth
				break
			}
		}
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	res_body, err := json.Marshal(*sum)
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		goto RES_ERROR
	}

	ctx.SetStatusCode(200)
	ctx.Response.Header.SetContentLength(len(res_body))
	_, err = io.CopyN(ctx.Response.BodyWriter(), bytes.NewReader(res_body), int64(len(res_body)))
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		goto RES_ERROR
	}
	return
RES_ERROR:
	ctx.SetStatusCode(500)
	return
}
