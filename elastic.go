package elastic

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/lovego/xiaomei/utils/httputil"
	"github.com/nu7hatch/gouuid"
)

type ES struct {
	BaseAddrs []string
	i         int
	client    *httputil.Client
}

func New(addrs ...string) *ES {
	if len(addrs) == 0 {
		log.Panic(`empty elastic addrs`)
	}
	return &ES{BaseAddrs: addrs, client: httputil.DefaultClient}
}

func New2(client *httputil.Client, addrs ...string) *ES {
	if len(addrs) == 0 {
		log.Panic(`empty elastic addrs`)
	}
	return &ES{BaseAddrs: addrs, client: client}
}

var NotFoundErr = errors.New(`404 not found.`)

func (es *ES) Get(path string, bodyData, data interface{}) error {
	resp, err := es.client.Get(es.Uri(path), nil, bodyData)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return NotFoundErr
	}
	if err := resp.Ok(); err != nil {
		return err
	}
	return resp.Json(data)
}

func (es *ES) Post(path string, bodyData, data interface{}) error {
	return es.client.PostJson(es.Uri(path), nil, bodyData, data)
}

func (es *ES) RootGet(path string, bodyData, data interface{}) error {
	uri, err := es.RootUri(path)
	if err != nil {
		return err
	}
	resp, err := es.client.Get(uri, nil, bodyData)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return NotFoundErr
	}
	if err := resp.Ok(); err != nil {
		return err
	}
	return resp.Json(data)
}

func (es *ES) RootPost(path string, bodyData, data interface{}) error {
	if uri, err := es.RootUri(path); err != nil {
		return err
	} else {
		return es.client.PostJson(uri, nil, bodyData, data)
	}
}

func (es *ES) Uri(path string) string {
	uri := es.BaseAddrs[es.i] + path
	if len(es.BaseAddrs) > 1 { // Round-Robin elastic nodes
		es.i++
		if es.i >= len(es.BaseAddrs) {
			es.i = 0
		}
	}
	return uri
}

func (es *ES) RootUri(path string) (string, error) {
	uri, err := url.Parse(es.BaseAddrs[es.i])
	if err != nil {
		return ``, err
	}
	newUri := url.URL{Scheme: uri.Scheme, User: uri.User, Host: uri.Host}
	if len(es.BaseAddrs) > 1 { // Round-Robin elastic nodes
		es.i++
		if es.i >= len(es.BaseAddrs) {
			es.i = 0
		}
	}
	return newUri.String() + path, nil
}

func GenUUID() (string, error) {
	if uid, err := uuid.NewV4(); err != nil {
		return ``, err
	} else {
		return strings.Replace(uid.String(), `-`, ``, -1), nil
	}
}