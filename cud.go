package elastic

import (
	"net/http"
	"net/url"
)

// 覆盖
func (es *ES) Put(path string, bodyData, data interface{}) error {
	resp, err := es.client.Put(es.Uri(path), contentTypeHeader, bodyData)
	if err != nil {
		return err
	}
	if err := resp.Check(http.StatusOK, http.StatusCreated); err != nil {
		var errData struct{ Error struct{ Type string } }
		if resp.StatusCode == http.StatusBadRequest && resp.Json(&errData) == nil &&
			(errData.Error.Type == `index_already_exists_exception` ||
				errData.Error.Type == `resource_already_exists_exception`) {
			return Error{typ: ErrorIndexAreadyExists, message: err.Error()}
		}
		return err
	}
	return resp.Json(data)
}

// 创建
func (es *ES) Create(path string, bodyData, data interface{}) error {
	uri, err := url.Parse(es.Uri(path))
	if err != nil {
		return err
	}
	uri.Path += `/_create`
	resp, err := es.client.Put(uri.String(), contentTypeHeader, bodyData)
	if err != nil {
		return err
	}
	if err := resp.Check(http.StatusOK, http.StatusCreated); err != nil {
		return err
	}
	return resp.Json(data)
}

// 删除
func (es *ES) Delete(path string, data interface{}) error {
	resp, err := es.client.Delete(es.Uri(path), nil, nil)
	if err != nil {
		return err
	}
	if err := resp.Ok(); err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return Error{typ: ErrorNotFound, message: err.Error()}
		}
		return err
	}
	return resp.Json(data)
}

// 更新
func (es *ES) Update(path string, bodyData, data interface{}) error {
	uri, err := url.Parse(es.Uri(path))
	if err != nil {
		return err
	}
	uri.Path += `/_update`
	return es.client.PostJson(uri.String(), contentTypeHeader, bodyData, data)
}

// Create if not Exist
func (es *ES) Ensure(path string, def interface{}) error {
	if ok, err := es.Exist(path); err != nil {
		return err
	} else if !ok {
		return es.Put(path, def, nil)
	}
	return nil
}

func (es *ES) Exist(path string) (bool, error) {
	resp, err := es.client.Head(es.Uri(path), nil, nil)
	if err != nil {
		return false, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, resp.CodeError()
	}
}
