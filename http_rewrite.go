package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	. "utils"
)

type rewriteTemplate struct {
	expr string
	dest string
}

type httpRewrite struct {
	LiveActive bool
	VodActive  bool
	LiveR      []rewriteTemplate
	VodR       []rewriteTemplate
	replace    map[string]string
}

var rewrite_mgmt *httpRewrite
var rewrite_once sync.Once

func initInstance() *httpRewrite {
	rewrite_once.Do(func() {
		rewrite_mgmt = &httpRewrite{}
		var i int = 1
		var key string
		//直播rewrite规则
		for {
			rewrite_mgmt.LiveActive = g_cf.DefaultBool("LIVE_REWRITE::active", false)
			if !rewrite_mgmt.LiveActive {
				break
			}

			var t rewriteTemplate
			key = fmt.Sprintf("LIVE_REWRITE::rule%d", i)
			t.expr = g_cf.String(key)
			if t.expr == "" {
				break
			}
			key = fmt.Sprintf("LIVE_REWRITE::dest%d", i)
			t.dest = g_cf.DefaultString(key, "")

			rewrite_mgmt.LiveR = append(rewrite_mgmt.LiveR, t)
			i++
		}

		//点播rewrite规则
		i = 1
		for {
			rewrite_mgmt.VodActive = g_cf.DefaultBool("VOD_REWRITE::active", false)
			if !rewrite_mgmt.VodActive {
				break
			}

			var t rewriteTemplate
			key = fmt.Sprintf("VOD_REWRITE::rule%d", i)
			t.expr = g_cf.String(key)
			if t.expr == "" {
				break
			}
			key = fmt.Sprintf("VOD_REWRITE::dest%d", i)
			t.dest = g_cf.DefaultString(key, "")

			rewrite_mgmt.VodR = append(rewrite_mgmt.VodR, t)
			i++
		}

		rewrite_mgmt.replace = make(map[string]string)
		var a byte = 'A'
		for {
			key = fmt.Sprintf("REPLACE::%s", string(a))
			val := g_cf.String(key)
			if val == "" {
				break
			}
			rewrite_mgmt.replace[string(a)] = val
			a++
		}
	})
	return rewrite_mgmt
}

func RewriteUrl(src string) string {
	mgmt := initInstance()
	live_mgmt := GetHttpMgmtInstance().mgmt_live
	vod_mgmt := GetHttpMgmtInstance().mgmt_vod
	if live_mgmt.active {
		for _, v := range mgmt.LiveR {
			r, err := regexp.Compile(v.expr)
			if err == nil {
				if r.MatchString(src) {
					//匹配
					if v.dest != "" {
						dst := r.ExpandString([]byte(nil), v.dest, src, r.FindStringSubmatchIndex(src))
						return string(dst)
					}
				}
			}
		}
	}
	if vod_mgmt.active {
		for _, v := range mgmt.VodR {
			r, err := regexp.Compile(v.expr)
			if err == nil {
				if r.MatchString(src) {
					//匹配
					if v.dest != "" {
						dst := r.ExpandString([]byte(nil), v.dest, src, r.FindStringSubmatchIndex(src))
						return string(dst)
					}
				}
			}
		}
	}
	return src
}

/*
 *必须要有匹配的正则，否则不处理请求，第二个返回值为false
 *找到匹配的正则，无论是否rewrite，第二个返回值都为true
 */
func MatchLiveUrl(src string) (string, bool) {
	mgmt := initInstance()
	if mgmt.LiveActive {
		for _, v := range mgmt.LiveR {
			r, err := regexp.Compile(v.expr)
			if err == nil {
				if r.MatchString(src) {
					//匹配
					if v.dest != "" {
						dst := r.ExpandString([]byte(nil), v.dest, src, r.FindStringSubmatchIndex(src))
						return string(dst), true
					} else {
						return src, true
					}
				}
			}
		}
	}
	return src, false
}

func MatchVodUrl(src string) (string, bool) {
	mgmt := initInstance()
	if mgmt.VodActive {
		for _, v := range mgmt.VodR {
			r, err := regexp.Compile(v.expr)
			if err == nil {
				if r.MatchString(src) {
					//匹配
					if v.dest != "" {
						dst := r.ExpandString([]byte(nil), v.dest, src, r.FindStringSubmatchIndex(src))
						return string(dst), true
					} else {
						return src, true
					}
				}
			}
		}
	}
	return src, false
}

func ReplaceExt(src string) string {
	var dst string = src
	mgmt := initInstance()

	ext_map, err := ParseExt(src)
	if err != nil {
		VLOG(VLOG_ERROR, "%s", err.Error())
		return dst
	}

	var keys []string
	//keys := make([]string, 0, 10)
	for k, v := range ext_map {
		r, ok := mgmt.replace[k]
		if ok {
			keys = append(keys, fmt.Sprintf("%s=%s", r, v))
		} else {
			keys = append(keys, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return strings.Join(keys, ",")
}

func ParseExt(ext string) (map[string]string, error) {
	if ext == "" {
		return nil, fmt.Errorf("Ext format error. [%s]", ext)
	}

	ext_map := make(map[string]string)

	tmp := strings.Split(ext, ",")
	for _, v := range tmp {
		var val []string
		if strings.Contains(v, "=") {
			val = strings.Split(v, "=")
		} else if strings.Contains(v, ":") {
			val = strings.Split(v, ":")
		} else {
			return nil, fmt.Errorf("Ext format error. [%s]", ext)
		}
		ext_map[strings.TrimSpace(val[0])] = strings.TrimSpace(val[1])
	}
	return ext_map, nil
}
