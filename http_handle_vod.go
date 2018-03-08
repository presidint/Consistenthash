package main

import (
	"VSLB/auth"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
	. "utils"

	"strconv"

	"regexp"

	"github.com/valyala/fasthttp"
)

const (
	ISP_TYPE_PRIVATE = 2
	ISP_TYPE_PUBLIC  = 32
)
const VOD_TS_TIMEOUT = 3600 //单位 秒

type VodSession struct {
	rw_uri string
	ctx    *fasthttp.RequestCtx
	r      *regexp.Regexp

	url     *url.URL
	urlVals url.Values
}

func (this *VodSession) Init() error {
	var err error
	this.url, err = url.ParseRequestURI(this.rw_uri)
	if err != nil {
		VLOG(VLOG_ERROR, "Parse url failed.[%s]", this.rw_uri)
		return err
	}
	this.urlVals = this.url.Query()
	return nil
}

func (this *VodSession) Handler() {
	var err error
	var expr string

	ctx := this.ctx

	err = this.Init()
	if err != nil {
		goto RES_ERROR
	}

	//match mp4
	//安徽电信
	//http://cdn.voole.com:3580/play?fid=a32e54067f58e0da8418693ba741e27f&keyid=135372&stamp=1440401367&auth=6caba6f89099539475e09a81e9696066&s=29&ext=oid:300017,eid:600044,code:gjzjmmd20150527160200091&hy=ah&uid=shengyuan&stbModel=E8205&stbCode=h55100070120&hy=ah&uid=shengyuan
	expr = "/(play)\\?(.*)"
	this.r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if this.r.MatchString(string(ctx.RequestURI())) {
		this.handle_request_mp4()
		return
	}

	//match mp4
	expr = "/(file/down)(.*)"
	this.r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if this.r.MatchString(string(ctx.RequestURI())) {
		this.handle_request_mp4()
		return
	}

	//match mp4
	expr = "/file/(\\w{32})(.*)"
	this.r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if this.r.MatchString(string(ctx.RequestURI())) {
		s := this.r.FindStringSubmatch(string(ctx.Path()))
		this.handle_request_common_file(s[1])
		return
	}

	//default match m3u8
	this.handle_request_m3u8()
	return
RES_ERROR:
	ctx.Response.SetStatusCode(404)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *VodSession) handle_request_m3u8() {
	var err error

	var m3u8_name, m3u8_fid string
	var tmp []string
	var source_addr, source_url string
	var resp *http.Response
	var stime, etime int64
	var resp_body, dst_body []byte
	var write_bytes int64

	ctx := this.ctx
	mgmt := GetVslbMgmtInstance()
	client := mgmt.client

	m3u8_name = this.url.Path[1:]
	tmp = strings.Split(m3u8_name, ".")
	m3u8_fid = tmp[0]

	source_addr, err = mgmt.HashList.Get([]byte(m3u8_fid))
	if err != nil {
		VLOG(VLOG_ERROR, "HashList Get Node Failed. [%s] [%s]", err.Error(), string(ctx.RequestURI()))
		goto RES_ERROR
	}

	client.Timeout = time.Second * 5
	source_url = fmt.Sprintf("http://%s/vod/%s", source_addr, m3u8_name) //回源url

	stime = time.Now().Unix()
	resp, err = client.Get(source_url)
	if resp != nil {
		defer resp.Body.Close()
	}
	etime = time.Now().Unix()
	if err != nil {
		VLOG(VLOG_ERROR, "Back Source failed. [%s] [%s], usetime:%v", source_url, err.Error(), etime-stime)
		goto RES_ERROR
	}
	if resp.StatusCode != 200 {
		VLOG(VLOG_ERROR, "Back Source failed. [%s], StatusCode:[%v], usetime:%v", source_url, resp.StatusCode, etime-stime)
		goto RES_ERROR
	}

	resp_body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		VLOG(VLOG_ERROR, "Back Source Read Body failed. [%s] [%s]", source_url, err.Error())
		goto RES_ERROR
	}
	etime = time.Now().Unix()
	VLOG(VLOG_MSG, "Back Source Succeed. [%s] usetime:%v", source_url, etime-stime)

	dst_body, err = this.modifyM3u8(resp_body)
	if err != nil {
		VLOG(VLOG_ERROR, "Modify m3u8 failed. [%s]", err.Error())
		goto RES_500
	}

	//TODO:no range, no 206
	ctx.Response.SetStatusCode(200)
	ctx.Response.Header.Set("Content-Length", fmt.Sprintf("%v", len(dst_body)))

	write_bytes, err = io.CopyN(ctx.Response.BodyWriter(), bytes.NewReader(dst_body), int64(len(dst_body)))
	if err != nil {
		VLOG(VLOG_ERROR, "Copy data failed. [%s] [%s], write_bytes:[%v]",
			string(ctx.RequestURI()), err.Error(), write_bytes)
		return
	}
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	ctx.Response.SetStatusCode(404)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_500:
	ctx.Response.SetStatusCode(500)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *VodSession) modifyM3u8(src []byte) ([]byte, error) {
	mgmt := GetVslbMgmtInstance()
	var dststring string

	sc := bufio.NewScanner(bytes.NewReader(src))
	for sc.Scan() {
		l := sc.Text()

		if strings.TrimSpace(l) == "" {
			continue
		}
		if l[0] == '#' {
			dststring += l + "\n"
			continue
		}

		// ./mp4?fid=3d793ae49f73eb6956f9b88339b20334&length=7486536&offset=4043692&ext=qvstarti=272,qvendi=521,qastarti=509,qaendi=976,qtype=100,qnalcount=0,qsize=7486536,qoffset=4043692
		u, err := url.ParseRequestURI("/" + l)
		if err != nil {
			return nil, err
		}
		tsUrlVals := u.Query()
		fid := tsUrlVals.Get("fid")
		offset := tsUrlVals.Get("offset")
		if fid == "" || offset == "" {
			return nil, fmt.Errorf("[%s] find fid:[%s], offset:[%s] failed.", l, fid, offset)
		}
		priv_addr, err := mgmt.HashList.Get([]byte(fid + offset))
		if err != nil {
			return nil, fmt.Errorf("Hash get node failed. fid:[%s], offset:[%s], error:[%s]", fid, offset, err.Error())
		}
		pub_addr := mgmt.NodeMap[priv_addr].PublicAddr

		cur_stamp := time.Now().Unix() + VOD_TS_TIMEOUT
		test_key := make([]byte, 64)
		ainfo := &auth.AuthInfo{
			Key:     test_key, //TODO:提前获取key
			Version: auth.AUTH_CHECK_VERSION_3_0,
			Stamp:   fmt.Sprintf("%v", cur_stamp),
			Fid:     fid,
		}
		astr, err := ainfo.Generate()
		if err != nil {
			return nil, fmt.Errorf("Generate auth failed. [%s], [%s]", l, err.Error())
		}

		uid := this.urlVals.Get("uid")
		keyid := this.urlVals.Get("keyid")
		k, _ := strconv.Atoi(keyid)
		isp := (k >> 16) | 0X00FF

		var ts_addr string
		if isp == ISP_TYPE_PUBLIC {
			ts_addr = pub_addr
		} else {
			ts_addr = priv_addr
		}
		ts_url := fmt.Sprintf("http://%s/%s&uid=%s&keyid=%s&stamp=%v&aver=%s&auth=%s",
			ts_addr, l, uid, keyid, cur_stamp, auth.AUTH_CHECK_VERSION_3_0, astr)

		dststring += ts_url + "\n"
	}

	return []byte(dststring), nil
}

func (this *VodSession) handle_request_mp4() {
	ctx := this.ctx
	mgmt := GetVslbMgmtInstance()

	var source_addr string
	var err error

	mp4fid := string(ctx.QueryArgs().Peek("fid"))
	if mp4fid == "" {
		goto RES_ERROR
	}
	source_addr, err = mgmt.HashList.Get([]byte(mp4fid))
	if err != nil {
		VLOG(VLOG_ERROR, "HashList Get Node Failed. [%s] [%s]", err.Error(), string(ctx.RequestURI()))
		goto RES_ERROR
	}
	ctx.Redirect(
		fmt.Sprintf("http://%s%s", source_addr, this.rw_uri), 302)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	ctx.Response.SetStatusCode(404)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *VodSession) handle_request_common_file(fid string) {
	ctx := this.ctx
	mgmt := GetVslbMgmtInstance()

	var source_addr string
	var err error

	if fid == "" {
		goto RES_ERROR
	}

	source_addr, err = mgmt.HashList.Get([]byte(fid))
	if err != nil {
		VLOG(VLOG_ERROR, "HashList Get Node Failed. [%s] [%s]", err.Error(), string(ctx.RequestURI()))
		goto RES_ERROR
	}
	ctx.Redirect(
		fmt.Sprintf("http://%s%s", source_addr, string(ctx.RequestURI())), 302)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	ctx.Response.SetStatusCode(404)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}
