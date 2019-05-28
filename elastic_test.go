package elastic

import (
	"fmt"
	"log"
	"os"
)

var testES = New(testEsHost() + `/test-index`)

func testEsHost() string {
	if esHost := os.Getenv(`EsHost`); esHost != `` {
		return esHost
	}
	return `http://localhost:9200`
}

func createEmptyUsers() {
	if err := testES.Delete(`/`, nil); err != nil {
		log.Panic(err)
	}

	if err := testES.Ensure(`/`, nil); err != nil {
		log.Panic(err)
	}
	if err := testES.Put(`/_mapping`, map[string]interface{}{
		"properties": map[string]interface{}{
			"name": map[string]string{"type": "keyword"},
			"age":  map[string]string{"type": "integer"},
		},
	}, nil); err != nil {
		log.Panic(err)
	}
}

func printData() {
	if err := testES.Get(`/_refresh`, nil, nil); err != nil {
		log.Panic(err)
	}

	result, err := testES.Search(`/_doc`, map[string]map[string]string{`sort`: {`age`: `desc`}})
	if err != nil {
		log.Panic(err)
	}
	hits := result.Hits
	fmt.Println(hits.Total)
	for _, row := range hits.Hits {
		fmt.Println(string(row.Source))
	}
}
