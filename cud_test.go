package elastic

import (
	"fmt"
	"log"
)

func ExampleCURD() {
	createEmptyUsers()

	if err := testES.Put(`/`, nil, nil); !IsIndexAreadyExists(err) {
		fmt.Printf("expect ErrorIndexAreadyExists, got: %v", err)
		return
	}

	if err := testES.Create(
		`/_doc/1`, map[string]interface{}{`name`: `lilei`, `age`: 21}, nil,
	); err != nil {
		log.Panic(err)
	}
	if err := testES.Create(
		`/_doc/2`, map[string]interface{}{`name`: `hanmeimei`, `age`: 19}, nil,
	); err != nil {
		log.Panic(err)
	}
	if err := testES.Create(
		`/_doc/3`, map[string]interface{}{`name`: `tom`, `age`: 22}, nil,
	); err != nil {
		log.Panic(err)
	}

	if err := testES.Delete(`/_doc/3`, nil); err != nil {
		log.Panic(err)
	}

	if err := testES.Update(
		`/_doc/1`, map[string]map[string]int{`doc`: {`age`: 31}}, nil,
	); err != nil {
		log.Panic(err)
	}
	if err := testES.Update(
		`/_doc/2`, map[string]map[string]int{`doc`: {`age`: 29}}, nil,
	); err != nil {
		log.Panic(err)
	}
	printData()

	// Output:
	// {2 eq}
	// {"age":31,"name":"lilei"}
	// {"age":29,"name":"hanmeimei"}
}
