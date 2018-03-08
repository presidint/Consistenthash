package main

import (
	"flag"
	"fmt"
	. "utils"

	"github.com/astaxie/beego/config"
)

var SVN_INFO string = `
URL: http://172.16.42.152/svn/project/01Dev/Go/SRC/src/vss_live_hg
Relative URL: ^/01Dev/Go/SRC/src/vss_live_hg
Repository Root: http://172.16.42.152/svn/project
Repository UUID: 234dda73-5abe-4da5-8a1e-2d7603de72eb
Revision: 626604
Node Kind: directory
Schedule: normal
Last Changed Author: zhaoguoxin
Last Changed Rev: 626604
Last Changed Date: 2017-11-27 10:23:22 +0800 (Mon, 27 Nov 2017)
`
var CONFIG_FILE_NAME string = "conf.ini"
var g_cf config.Configer
var EXIT_PROCESS = make(chan bool)

func main() {
	var err error
	ver := flag.Bool("version", false, "check version")
	flag.Parse()
	if *ver {
		fmt.Println(SVN_INFO)
		return
	}
	if false == VooleDaemon() {
		return
	}
	Vlog_init("Log.json")
	VLOG(VLOG_ALTER, "========================== start ==========================")

	g_cf, err = config.NewConfig("ini", CONFIG_FILE_NAME)
	if err != nil {
		VLOG(VLOG_ERROR, "Load config file failed. [%s]", err.Error())
		fmt.Printf("Load config file failed. [%s]\n", err.Error())
		return
	}

	mgmt := GetVslbMgmtInstance()
	mgmt.Start()

	http_mgmt := GetHttpMgmtInstance()
	err = http_mgmt.Start()
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		fmt.Println(err)
		return
	}

	<-EXIT_PROCESS
	return
}
