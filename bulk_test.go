package elastic

import "log"

func ExampleBulk() {
	createEmptyUsers()

	if err := testES.BulkCreate(`/_doc`, [][2]interface{}{
		{1, map[string]interface{}{`name`: `lilei`, `age`: 21}},
		{2, map[string]interface{}{`name`: `hanmeimei`, `age`: 20}},
		{3, map[string]interface{}{`name`: `tom`, `age`: 22}},
	}); err != nil {
		log.Panic(err)
	}

	if err := testES.Delete(`/_doc/3`, nil); err != nil {
		log.Panic(err)
	}

	if err := testES.BulkUpdate(`/_doc`, [][2]interface{}{
		{1, map[string]map[string]interface{}{`doc`: {`age`: 31}}},
		{2, map[string]map[string]interface{}{`doc`: {`age`: 29}}},
	}); err != nil {
		log.Panic(err)
	}

	printData()
	// Output:
	// {2 eq}
	// {"age":31,"name":"lilei"}
	// {"age":29,"name":"hanmeimei"}
}
