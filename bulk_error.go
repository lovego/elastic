package elastic

import (
	"encoding/json"
	"fmt"
	"log"
)

type BulkError interface {
	FailedItems(ignoreMapperParsingException, ignoreIllegalArgumentException bool) [][2]interface{}
	Error() string
}

type bulkError struct {
	typ    string
	inputs [][2]interface{}
	/* results example:
		 * [
		 *	   {
		 *	       "index":  {
		 *	           "_id": "9208c6f4941b46dd61d5cd2b34ea2267",
		 *	           "_index": "logc-common-calls-erp-log.2020.02",
		 *	           "_type": "_doc",
		 *	           "error": {
		 *	               "caused_by": {
		 *	                   "reason": "cannot parse empty date",
		 *	                   "type": "illegal_argument_exception"
		 *	               },
		 *	               "reason": "failed to parse field [data_o.BeginDate_s] of type [date] in document with id '9208c6f4941b46dd61d5cd2b34ea2267'",
		 *	               "type": "mapper_parsing_exception"
		 *	           },
		 *	           "status": 400
		 *	       }
		 *	   },
		 *	   {
		 *	       "create":  {
	     *             ...
		 *	       }
		 *	   },
		 *	   {
		 *	       "update":  {
	     *             ...
		 *	       }
		 *	   },
		 *	   {
		 *	       "delete":  {
	     *             ...
		 *	       }
		 *	   },
		 *	   ...
		 * ]
	*/
	results []map[string]map[string]interface{}
}

func (b bulkError) FailedItems(
	ignoreMapperParsingException, ignoreIllegalArgumentException bool,
) [][2]interface{} {
	failedItems := make([][2]interface{}, 0)
	for i, result := range b.results {
		res := result[b.typ]
		if err := res[`error`]; err != nil {
			if ignoreMapperParsingException {
				if errMap, _ := err.(map[string]interface{}); errMap != nil &&
					errMap["type"] == "mapper_parsing_exception" {
					continue
				}
			}
			if ignoreIllegalArgumentException {
				if errMap, _ := err.(map[string]interface{}); errMap != nil &&
					errMap["type"] == "illegal_argument_exception" {
					continue
				}
			}
			failedItems = append(failedItems, b.inputs[i])
		}
	}
	return failedItems
}

func (b bulkError) Error() string {
	var errs []interface{}
	for _, result := range b.results {
		info := result[b.typ]
		if info[`error`] != nil {
			errs = append(errs, info)
		}
	}
	buf, err := json.MarshalIndent(errs, ``, `  `)
	if err != nil {
		log.Println(`marshal elastic bulk errors: `, err)
	}
	return fmt.Sprintf("bulk %s errors(%d of %d)\n%s\n",
		b.typ, len(errs), len(b.inputs), buf,
	)
}

type BulkDeleteError interface {
	FailedItems() []string
	Error() string
}

type bulkDeleteError struct {
	inputs  []string
	results []map[string]map[string]interface{}
}

func (b bulkDeleteError) FailedItems() []string {
	failedItems := make([]string, 0)
	for i, result := range b.results {
		res := result[`delete`]
		if res[`error`] != nil {
			failedItems = append(failedItems, b.inputs[i])
		}
	}
	return failedItems
}

func (b bulkDeleteError) Error() string {
	var errs []interface{}
	for _, result := range b.results {
		info := result[`delete`]
		if info[`error`] != nil {
			errs = append(errs, info)
		}
	}
	buf, err := json.MarshalIndent(errs, ``, `  `)
	if err != nil {
		log.Println(`marshal elastic bulk errors: `, err)
	}
	return fmt.Sprintf("bulk delete errors(%d of %d)\n%s\n", len(errs), len(b.inputs), buf)
}
