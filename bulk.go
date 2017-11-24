package elastic

import "net/url"

type BulkResult struct {
	Errors bool                                `json:"errors"`
	Items  []map[string]map[string]interface{} `json:"items"`
}

func (es *ES) BulkCreate(path string, data [][2]interface{}) error {
	if len(data) <= 0 {
		return nil
	}
	body, err := makeBulkCreate(data)
	if err != nil {
		return err
	}
	return es.BulkDo(path, body, `create`, data)
}

func (es *ES) BulkIndex(path string, data [][2]interface{}) error {
	if len(data) <= 0 {
		return nil
	}
	body, err := makeBulkIndex(data)
	if err != nil {
		return err
	}
	return es.BulkDo(path, body, `index`, data)
}

func (es *ES) BulkUpdate(path string, data [][2]interface{}) error {
	if len(data) <= 0 {
		return nil
	}
	body, err := makeBulkUpdate(data)
	if err != nil {
		return err
	}
	return es.BulkDo(path, body, `update`, data)
}

func (es *ES) BulkDo(path string, body, typ string, data [][2]interface{}) error {
	uri, err := url.Parse(es.Uri(path))
	if err != nil {
		return err
	}
	uri.Path += `/_bulk`

	result := BulkResult{}
	if err := es.client.PostJson(uri.String(), nil, body, &result); err != nil {
		return err
	}
	if !result.Errors {
		return nil
	}
	return bulkError{typ: typ, inputs: data, results: result.Items}
}

func (es *ES) BulkDelete(path string, data []string) error {
	uri, err := url.Parse(es.Uri(path))
	if err != nil {
		return err
	}
	uri.Path += `/_bulk`

	if len(data) <= 0 {
		return nil
	}
	body, err := makeBulkDelete(data)
	if err != nil {
		return err
	}

	result := BulkResult{}
	if err := es.client.PostJson(uri.String(), nil, body, &result); err != nil {
		return err
	}
	if !result.Errors {
		return nil
	}
	return bulkDeleteError{inputs: data, results: result.Items}
}
