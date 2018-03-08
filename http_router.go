package main

import (
	. "utils"

	"strings"

	"regexp"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

type HttpRouter struct {
}

type Handler func(ctx *fasthttp.RequestCtx, rw_uri string)

var g_live_router map[string]Handler
var g_vod_router map[string]Handler
var g_router map[string]Handler

func InitHttpRouter() (*fasthttprouter.Router, error) {
	router := &fasthttprouter.Router{
		RedirectTrailingSlash:  false,
		RedirectFixedPath:      false,
		HandleMethodNotAllowed: false,
		HandleOPTIONS:          false,
		NotFound:               nil,
		MethodNotAllowed:       nil,
		PanicHandler:           nil,
	}

	live_mgmt := GetHttpMgmtInstance().mgmt_live
	live_mgmt.Init()
	vod_mgmt := GetHttpMgmtInstance().mgmt_vod
	vod_mgmt.Init()

	g_router = make(map[string]Handler)
	if live_mgmt.active {
		g_router["/(\\w+)/([0-9]+)/(\\w+)/([0-9]+)/(\\w+)/([0-9]+).m3u8(.+)"] = LiveM3u8Handler
		g_router["/(\\w+)/([0-9]+)/(\\w+)/([0-9]+)/(\\w+)/([\\w, -]+).ts(.*)"] = LiveTsHandler
		g_router["/(\\w+)/([\\w, -]+).ts(.*)"] = LiveTsHandler
		g_router["/package(.*)"] = LiveImageHandler
		g_router["/image(.*)"] = LiveImageHandler
	}
	if vod_mgmt.active {
		g_router["/([a-z]+)\\$([0-9]+)/([a-z]+)\\$([0-9]+)/([a-z]+)\\$([0-9]+)/([a-z]+)\\$([0-9a-z]+)/([a-z]+)\\$(.*)/([0-9a-z]+)\\.m3u8\\?(.*)"] = VodM3u8Handler
		g_router["/(play)\\?(.*)"] = VodMp4Handler
		g_router["/(file/down)(.*)"] = VodMp4Handler
		g_router["/file/(\\w{32})(.*)"] = VodCommonHandler
	}

	//router.GET("/*filepath", handlerRouter)
	router.GET("/*filepath", handlerRouter2)
	return router, nil
}

func LiveM3u8Handler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	sess := &LiveSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	sess.handle_request_m3u8()
	return
RES_ERROR:
	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}
func LiveTsHandler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	sess := &LiveSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	sess.handle_request_ts()
	return
RES_ERROR:
	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}
func LiveImageHandler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	sess := &LiveSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	sess.handle_request_image()
	return
RES_ERROR:
	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func VodM3u8Handler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	sess := &VodSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	sess.handle_request_m3u8()
	return
RES_ERROR:
	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}
func VodMp4Handler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	sess := &VodSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	sess.handle_request_mp4()
	return
RES_ERROR:
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}
func VodCommonHandler(ctx *fasthttp.RequestCtx, rw_uri string) {
	var err error
	var path string
	var i int

	sess := &VodSession{
		rw_uri: rw_uri,
		ctx:    ctx,
	}
	err = sess.Init()
	if err != nil {
		goto RES_ERROR
	}
	path = string(ctx.Path())
	i = strings.LastIndex(path, "/")
	if i == -1 {
		goto RES_ERROR
	}
	sess.handle_request_common_file(path[i+1:])
	return
RES_ERROR:
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", string(ctx.RequestURI()), ctx.Response.Header.String())
	return
}

func handlerRouter2(ctx *fasthttp.RequestCtx) {
	VLOG(VLOG_MSG, "%s", ctx.Request.Header.String())
	ctx.Response.Header.Set("Server", "VSLB")
	ctx.SetConnectionClose()
	req_uri := string(ctx.RequestURI())

	for expr, handler := range g_router {
		r, err := regexp.Compile(expr)
		if err != nil {
			VLOG(VLOG_ERROR, "[%s] %s", string(ctx.RequestURI()), err.Error())
			goto RES_ERROR
		}
		if r.MatchString(string(ctx.RequestURI())) {
			rw_uri := RewriteUrl(req_uri)
			handler(ctx, rw_uri)
			return
		}
	}
RES_ERROR:
	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", req_uri, ctx.Response.Header.String())
	return
}

func handlerRouter(ctx *fasthttp.RequestCtx) {
	var rw_uri string
	var ok bool
	//var nr *regexp.Regexp

	VLOG(VLOG_MSG, "%s", ctx.Request.Header.String())
	ctx.Response.Header.Set("Server", "VSLB")
	ctx.SetConnectionClose()

	req_uri := string(ctx.RequestURI())

	if strings.Contains(string(ctx.Path()), "/status") {
		RouteStatus(ctx)
		return
	}

	rw_uri, ok = MatchLiveUrl(req_uri)
	if ok {
		VLOG(VLOG_MSG, "Rewrite url. [%s] -> [%s]", req_uri, rw_uri)
		//handle live
		GetHttpMgmtInstance().mgmt_live.Init()

		sess := &LiveSession{
			rw_uri: rw_uri,
			ctx:    ctx,
		}
		sess.Handler()
		return
	}
	rw_uri, ok = MatchVodUrl(req_uri)
	if ok {
		VLOG(VLOG_MSG, "Rewrite url. [%s] -> [%s]", req_uri, rw_uri)
		//handle vod
		GetHttpMgmtInstance().mgmt_vod.Init()
		sess := &VodSession{
			rw_uri: rw_uri,
			ctx:    ctx,
		}
		sess.Handler()
		return
	}

	//return 400
	ctx.Response.SetStatusCode(400)
	VLOG(VLOG_ERROR, "Response [%s]\n%s", req_uri, ctx.Response.Header.String())
	return
}
