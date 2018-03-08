package main

import (
	"VSLB/auth"
	"net/url"
	. "utils"

	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"bytes"
	"io"

	"bufio"
	"strconv"

	"container/list"

	"regexp"

	"github.com/valyala/fasthttp"
)

const (
	QTYPE_LIVE     = 1
	QTYPE_DELAY    = 2
	QTYPE_LOOKBACK = 3
)

const LIVE_TS_TIMEOUT = 3600 //单位 秒

type LiveSession struct {
	rw_uri string
	ctx    *fasthttp.RequestCtx
	qtype  int
	chid   string

	url     *url.URL
	urlVals url.Values
}

type TsElem struct {
	sess         *LiveSession
	segment_name string
	file_order   int64
	duration     float32
	node_host    string
}

func (e *TsElem) ToUrl() (string, error) {
	if e.sess == nil || e.node_host == "" {
		return "", fmt.Errorf("ToUrl error.")
	}

	mgmt := GetVslbMgmtInstance()
	msg := e.sess
	getTsAddr := func(keyid string, pubaddr string, priaddr string) string {
		if keyid == "" {
			return priaddr
		}
		k, _ := strconv.Atoi(keyid)
		isp := (k >> 16) | 0X00FF

		if isp == ISP_TYPE_PUBLIC {
			return pubaddr
		}
		return priaddr
	}

	priv_addr := e.node_host
	pub_addr := mgmt.NodeMap[priv_addr].PublicAddr
	ts_addr := getTsAddr(msg.urlVals.Get("keyid"), pub_addr, priv_addr)
	cur_stamp := time.Now().Unix() + LIVE_TS_TIMEOUT

	test_key := make([]byte, 64)
	ainfo := &auth.AuthInfo{
		Key:     test_key, //TODO:提前获取key
		Version: auth.AUTH_CHECK_VERSION_3_0,
		Stamp:   fmt.Sprintf("%v", cur_stamp),
		Fid:     e.segment_name,
	}
	astr, err := ainfo.Generate()
	if err != nil {
		return "", fmt.Errorf("Generate auth failed. [%s], [%s]", e.segment_name, err.Error())
	}

	hid := msg.urlVals.Get("hid")
	oemid := msg.urlVals.Get("oemid")
	path := msg.urlVals.Get("path")
	url_ver := msg.urlVals.Get("ver")

	//模式：http://[host]:[port]/[auth]/[stamp]/[hid]/[oemid]/[path]/xxx.ts?[ver=]
	//示例：/TEvW9GFiZYjdVKqgte9P9Q/1506787200/b083fe4d9483/817/7/xxx.ts?ver=3.0
	ts_url := fmt.Sprintf("http://%s/%s/%v/%s/%s/%s/%s?ver=%s",
		ts_addr, astr, cur_stamp, hid, oemid, path, e.segment_name, url_ver)

	return ts_url, nil
}

func (this *LiveSession) Init() error {
	var err error
	this.url, err = url.ParseRequestURI(this.rw_uri)
	if err != nil {
		VLOG(VLOG_ERROR, "Parse url failed.[%s]", this.rw_uri)
		return err
	}
	this.url.Scheme = "http"
	this.urlVals = this.url.Query()
	return nil
}

func (this *LiveSession) Handler() {
	var err error
	var r *regexp.Regexp
	var expr string

	ctx := this.ctx

	err = this.Init()
	if err != nil {
		goto RES_ERROR
	}

	//match ts
	expr = "(.*)(.ts)(.*)"
	r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if r.MatchString(string(ctx.RequestURI())) {
		this.handle_request_ts()
		return
	}

	//match package
	//http://ip:port/package?ext=qtype=500,starttime:1511756760,endtime:1511759580,sublevel:5,step:1
	expr = "/package(.*)"
	r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if r.MatchString(string(ctx.RequestURI())) {
		this.handle_request_image()
		return
	}
	//match image
	//http://ip:port/image?ext=qtype:600,starttime:1511761290,sublevel:739
	expr = "/image(.*)"
	r, err = regexp.Compile(expr)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	if r.MatchString(string(ctx.RequestURI())) {
		this.handle_request_image()
		return
	}
	//default match m3u8
	this.handle_request_m3u8()
	return

RES_ERROR:
	ctx.SetStatusCode(404)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *LiveSession) handle_request_m3u8() {
	starttime := this.urlVals.Get("starttime")
	endtime := this.urlVals.Get("endtime")
	lastplayseg := this.urlVals.Get("lastplayseg")

	m3u8_name := this.url.Path[1:]
	tmp := strings.Split(m3u8_name, ".")
	this.chid = tmp[0]

	if starttime != "" && endtime != "" {
		this.qtype = QTYPE_LOOKBACK
		this.handle_request_lookback()
		return
	}
	if starttime == "" && endtime == "" && lastplayseg == "" {
		this.qtype = QTYPE_LIVE
		this.handle_request_live()
		return
	}
	if starttime != "" && endtime == "" {
		this.qtype = QTYPE_DELAY
		this.handle_request_delay()
		return
	}
	return
}

func (this *LiveSession) handle_request_ts() {
	ctx := this.ctx
	mgmt := GetVslbMgmtInstance()

	tmp := strings.Split(this.url.Path, "/")
	ts_name := tmp[len(tmp)-1]
	tmp = strings.Split(ts_name, ".")
	ts_id := tmp[0]

	priv_addr, err := mgmt.HashList.Get([]byte(ts_id))
	if err != nil {
		VLOG(VLOG_ERROR, "HashList Get [%s][%s]", ts_id, err.Error())
		goto RES_ERROR
	}
	this.url.Host = priv_addr
	ctx.Redirect(this.url.String(), 302)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	ctx.SetStatusCode(500)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *LiveSession) handle_request_image() {
	mgmt := GetHttpMgmtInstance().mgmt_live
	ctx := this.ctx

	u := fmt.Sprintf("http://%s%s", mgmt.pull_server_addr, string(ctx.RequestURI()))
	res, err := mgmt.client.Get(u)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		VLOG(VLOG_ERROR, "Request Failed. [%s] [%s]", u, err.Error())
		goto RES_404
	}
	ctx.SetStatusCode(200)
	_, err = io.Copy(ctx.Response.BodyWriter(), res.Body)
	if err != nil {
		VLOG(VLOG_ERROR, "Request Failed. [%s] [%s]", u, err.Error())
		return
	}
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_404:
	ctx.SetStatusCode(500)
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func (this *LiveSession) handle_request_live() {
	var err error
	var source_addr, source_url string
	var resp *http.Response
	var stime, etime int64
	var resp_body, dst_body []byte
	var write_bytes int64
	var u *url.URL

	ctx := this.ctx
	mgmt := GetVslbMgmtInstance()
	client := mgmt.client

	source_addr, err = mgmt.HashList.Get([]byte(this.chid))
	if err != nil {
		VLOG(VLOG_ERROR, "HashList Get Node Failed. [%s] [%s]", err.Error(), string(ctx.RequestURI()))
		goto RES_ERROR
	}

	client.Timeout = time.Second * 3
	//source_url = fmt.Sprintf("http://%s/live/%s", source_addr, m3u8_name) //回源url

	u, _ = url.Parse(ctx.URI().String())
	u.Host = source_addr
	source_url = u.String()

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
	VLOG(VLOG_DEBUG, "%s", string(resp_body))

	dst_body, err = this.modify_live_m3u8(resp_body)
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

func (this *LiveSession) handle_request_delay() {
	var dst_m3u8 string
	var res_body []byte

	ctx := this.ctx

	ts_list, err := this.query_ts_from_db(0, 0, QTYPE_DELAY)
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		goto RES_ERROR
	}
	dst_m3u8, err = this.modify_delay_m3u8(ts_list)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] [%s]", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	res_body = []byte(dst_m3u8)
	this.ctx.Response.SetStatusCode(200)
	_, err = io.CopyN(this.ctx.Response.BodyWriter(), bytes.NewReader(res_body), int64(len(res_body)))
	if err != nil {
		VLOG(VLOG_ERROR, "Copy data failed. [%s] [%s]",
			string(this.ctx.RequestURI()), err.Error())
		return
	}
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	this.ctx.Response.SetStatusCode(404)
	return
}

func (this *LiveSession) handle_request_lookback() {
	var dst_m3u8 string
	var res_body []byte
	var err error

	ctx := this.ctx
	cur_time := time.Now()

	st := this.urlVals.Get("starttime")
	et := this.urlVals.Get("endtime")

	starttime, _ := strconv.ParseInt(st, 10, 64)
	endtime, _ := strconv.ParseInt(et, 10, 64)
	if endtime > cur_time.Unix() {
		endtime = cur_time.Unix()
	}

	ts_list, err := this.query_ts_from_db(starttime, endtime, QTYPE_LOOKBACK)
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		goto RES_ERROR
	}
	dst_m3u8, err = this.modify_lookback_m3u8(ts_list)
	if err != nil {
		VLOG(VLOG_ERROR, "[%s] [%s]", string(ctx.RequestURI()), err.Error())
		goto RES_ERROR
	}
	res_body = []byte(dst_m3u8)

	this.ctx.Response.SetStatusCode(200)
	_, err = io.CopyN(this.ctx.Response.BodyWriter(), bytes.NewReader(res_body), int64(len(res_body)))
	if err != nil {
		VLOG(VLOG_ERROR, "Copy data failed. [%s] [%s]",
			string(this.ctx.RequestURI()), err.Error())
		return
	}
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
RES_ERROR:
	VLOG(VLOG_MSG, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	ctx.Response.SetStatusCode(404)
	return
}

func (this *LiveSession) modify_live_m3u8(src []byte) ([]byte, error) {
	mgmt := GetVslbMgmtInstance()
	var dststring string
	var ts_addr, ts_url string

	getTsAddr := func(keyid string, pubaddr string, priaddr string) string {
		if keyid == "" {
			return priaddr
		}
		k, _ := strconv.Atoi(keyid)
		isp := (k >> 16) | 0X00FF

		if isp == ISP_TYPE_PUBLIC {
			return pubaddr
		}
		return priaddr
	}

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

		if strings.Contains(l, "http://") {
			//已经修改过的m3u8文件，只需要更新host即可
			t, err := url.ParseRequestURI(l)
			if err != nil {
				return nil, fmt.Errorf("[%s] Format error.", l)
			}
			tmp := strings.Split(t.Path, "/")
			ts_name := tmp[len(tmp)-1]
			tmp = strings.Split(ts_name, ".")
			ts_id := tmp[0]
			priv_addr, err := mgmt.HashList.Get([]byte(ts_id))
			if err != nil {
				return nil, fmt.Errorf("Hash get node failed. ts_id:[%s], error:[%s]", ts_id, err.Error())
			}
			pub_addr := mgmt.NodeMap[priv_addr].PublicAddr

			ts_addr = getTsAddr(this.urlVals.Get("keyid"), pub_addr, priv_addr)

			t.Host = ts_addr
			ts_url = t.String()
		} else {
			ts_name := l
			tmp := strings.Split(ts_name, ".")
			ts_id := tmp[0]

			priv_addr, err := mgmt.HashList.Get([]byte(ts_id))
			if err != nil {
				return nil, fmt.Errorf("Hash get node failed. ts_id:[%s], error:[%s]", ts_id, err.Error())
			}
			pub_addr := mgmt.NodeMap[priv_addr].PublicAddr
			ts_addr = getTsAddr(this.urlVals.Get("keyid"), pub_addr, priv_addr)

			///////////////////////////////////////////////////////////////////////////////////////////////////////////////
			cur_stamp := time.Now().Unix() + LIVE_TS_TIMEOUT

			test_key := make([]byte, 64)
			ainfo := &auth.AuthInfo{
				Key:     test_key, //TODO:提前获取key
				Version: auth.AUTH_CHECK_VERSION_3_0,
				Stamp:   fmt.Sprintf("%v", cur_stamp),
				Fid:     ts_id,
			}
			astr, err := ainfo.Generate()
			if err != nil {
				return nil, fmt.Errorf("Generate auth failed. [%s], [%s]", l, err.Error())
			}

			hid := this.urlVals.Get("hid")
			oemid := this.urlVals.Get("oemid")
			path := this.urlVals.Get("path")
			url_ver := this.urlVals.Get("ver")

			//模式：http://[host]:[port]/[auth]/[stamp]/[hid]/[oemid]/[path]/xxx.ts?[ver=]
			//示例：/TEvW9GFiZYjdVKqgte9P9Q/1506787200/b083fe4d9483/817/7/xxx.ts?ver=3.0
			ts_url = fmt.Sprintf("http://%s/%s/%v/%s/%s/%s/%s?ver=%s",
				ts_addr, astr, cur_stamp, hid, oemid, path, l, url_ver)
		}
		dststring += ts_url + "\n"
	}

	return []byte(dststring), nil
}

func (this *LiveSession) query_ts_from_db(stime, etime int64, qtype int) (*list.List, error) {
	results := list.New()
	results.Init()

	mgmt := GetHttpMgmtInstance().mgmt_live
	if mgmt.db == nil {
		return nil, fmt.Errorf("Live db is nil...")
	}
	t := time.Now()

	switch qtype {
	case QTYPE_DELAY:
		y, m, d := t.Date()
		table_name := fmt.Sprintf("%s_%04d_%02d_%02d", this.chid, y, m, d)
		sql_str := fmt.Sprintf("select  segment_name, file_order, duration, node_host from %s order by uid DESC limit 0,%d",
			table_name, mgmt.delay_segs)

		rows, err := mgmt.db.Query(sql_str)
		if rows != nil {
			defer rows.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("[%s][%s]", sql_str, err.Error())
		}

		for rows.Next() {
			e := TsElem{
				sess:         this,
				segment_name: "",
				file_order:   0,
				duration:     0,
				node_host:    "",
			}
			rows.Scan(&e.segment_name, &e.file_order, &e.duration, &e.node_host)
			results.PushFront(e)
		}

		if results.Len() < mgmt.delay_segs {
			//跨天
			remain_segs := mgmt.delay_segs - results.Len()
			y, m, d := t.AddDate(0, 0, -1).Date()
			table_name := fmt.Sprintf("%s_%04d_%02d_%02d", this.chid, y, m, d)
			sql_str := fmt.Sprintf("select  segment_name, file_order, duration, node_host from %s order by uid DESC limit 0,%d",
				table_name, remain_segs)

			rows, err := mgmt.db.Query(sql_str)
			if rows != nil {
				defer rows.Close()
			}
			if err == nil {
				for rows.Next() {
					e := TsElem{
						sess:         this,
						segment_name: "",
						file_order:   0,
						duration:     0,
						node_host:    "",
					}
					rows.Scan(&e.segment_name, &e.file_order, &e.duration, &e.node_host)
					results.PushFront(e)
				}
			}
		}
	case QTYPE_LOOKBACK:
		cury, curm, curd := t.Date()
		table_time := time.Unix(stime, 0)
		for {
			y, m, d := table_time.Date()
			table_name := fmt.Sprintf("%s_%04d_%02d_%02d", this.chid, y, m, d)
			//query
			sql_str := fmt.Sprintf("SELECT segment_name, file_order, duration, node_host FROM %s WHERE file_date BETWEEN %d AND %d ORDER BY file_order ASC",
				table_name, stime, etime)

			rows, err := mgmt.db.Query(sql_str)
			if err != nil {
				if rows != nil {
					rows.Close()
				}
				return nil, fmt.Errorf("[%s][%s]", sql_str, err.Error())
			}
			for rows.Next() {
				e := TsElem{
					sess:         this,
					segment_name: "",
					file_order:   0,
					duration:     0,
					node_host:    "",
				}
				rows.Scan(&e.segment_name, &e.file_order, &e.duration, &e.node_host)
				results.PushBack(e)
			}
			rows.Close()
			if y == cury && m == curm && d == curd {
				//查询过今天的表，不再继续查询
				break
			}
			table_time = table_time.AddDate(0, 0, 1)
		}
	}

	if results.Len() <= 0 {
		return nil, fmt.Errorf("Find seg failed.[%v][%v][%v]", qtype, stime, etime)
	}
	return results, nil
}

func (this *LiveSession) modify_delay_m3u8(elist *list.List) (string, error) {
	var dst_head, dst_body string
	var max_duration int
	var sequence int64 = -1

	cur_count := 0
	max := 3

	v := elist.Front()
	for v != nil {
		e, ok := v.Value.(TsElem)
		if !ok {
			return "", fmt.Errorf("Assert elem type failed.")
		}
		dst_body += fmt.Sprintf("#EXTINF:%v, no desc\n", e.duration)
		ts_url, _ := e.ToUrl()
		dst_body += ts_url + "\n"

		if int(e.duration+1) > max_duration {
			max_duration = int(e.duration + 1)
		}
		if sequence < 0 {
			sequence = e.file_order
		}

		cur_count++
		if cur_count >= max {
			break
		}
		v = v.Next()
	}
	dst_head += "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-ALLOW-CACHE:YES\n"
	dst_head += fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-TARGETDURATION:%d\n", sequence, max_duration)
	return dst_head + dst_body, nil
}

func (this *LiveSession) modify_lookback_m3u8(elist *list.List) (string, error) {
	var dst_head, dst_body, dst_tail string
	var max_duration int
	var sequence int64 = -1

	v := elist.Front()
	for v != nil {
		e, ok := v.Value.(TsElem)
		if !ok {
			return "", fmt.Errorf("Assert elem type failed.")
		}
		dst_body += fmt.Sprintf("#EXTINF:%v, no desc\n", e.duration)
		ts_url, _ := e.ToUrl()
		dst_body += ts_url + "\n"
		if int(e.duration+1) > max_duration {
			max_duration = int(e.duration + 1)
		}
		if sequence < 0 {
			sequence = e.file_order
		}
		v = v.Next()
	}
	dst_head += "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-ALLOW-CACHE:YES\n"
	dst_head += fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-TARGETDURATION:%d\n", sequence, max_duration)
	dst_tail += "#EXT-X-ENDLIST\n"
	return dst_head + dst_body + dst_tail, nil
}
