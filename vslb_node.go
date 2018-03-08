package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	. "utils"

	"github.com/json-iterator/go"
)

const DEFAULT_HEARTBEAT_INTERVAL = 5

type StatusSeg struct {
	ZoneName     string `json:"zone_name"`
	Key          string `json:"key"`
	MaxActive    int    `json:"max_active"`
	MaxBandWidth int    `json:"max_bandwidth"`
	Traffic      int    `json:"traffic"`
	Requests     int    `json:"requests"`
	Active       int    `json:"active"`
	BandWidth    int    `json:"bandwidth"`
}

type NginxStatus struct {
	Zone []StatusSeg `json:"zone"`
}

type VslbNode struct {
	IsLive            bool
	Status            *NginxStatus
	PrivateAddr       string
	PublicAddr        string
	HeartBeatUrl      string
	HeartBeatInterval int

	client   *http.Client
	ht_timer *time.Ticker
}

func (this *VslbNode) Start() {
	this.client = &http.Client{}

	go this.handleHeartBeat()
	this.ht_timer = time.NewTicker(time.Duration(this.HeartBeatInterval) * time.Second)

	go this.handleTimeOut()
}

func (this *VslbNode) handleTimeOut() {
	for {
		select {
		case <-this.ht_timer.C:
			go this.handleHeartBeat()
		}
	}
}

func (this *VslbNode) handleHeartBeat() {
	var body []byte
	var err error

	mgmt := GetVslbMgmtInstance()
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	this.client.Timeout = 2 * time.Second

	resp, err := this.client.Get(this.HeartBeatUrl)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		goto HT_ERROR
	}
	if resp.StatusCode != 200 {
		goto HT_ERROR
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		VLOG(VLOG_ERROR, "[HeartBeat] Recv body failed. [%s]", err.Error())
		goto HT_ERROR
	}

	err = json.Unmarshal(body, this.Status)
	if err != nil {
		VLOG(VLOG_ERROR, "[HeartBeat] Json unmarshal failed. [%s]", err.Error())
		goto HT_ERROR
	}

	if this.IsLive == false {
		VLOG(VLOG_WARNNING, "Node[%s] is Enable.", this.PrivateAddr)
	}
	this.IsLive = true
	mgmt.HashList.Add(this.PrivateAddr)
	return
HT_ERROR:
	this.IsLive = false
	mgmt.HashList.Remove(this.PrivateAddr)
	VLOG(VLOG_ERROR, "Node[%s] is Disable.", this.PrivateAddr)
	fmt.Printf("Node[%s] is Disable.\n", this.PrivateAddr)
	return
}
